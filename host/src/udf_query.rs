//! Embedded SQL approach for executing Python UDFs within SQL queries.

use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::ScalarUDF;
use datafusion_sql::sqlparser::ast::Statement;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use sqlparser::ast::{CreateFunctionBody, Expr, Value};

use crate::{WasmComponentPrecompiled, WasmScalarUdf};

/// A SQL query containing a Python UDF and SQL string that uses the UDF
#[derive(Debug, Clone)]
pub struct UdfQuery(String);

impl UdfQuery {
    /// Create a new UDF query
    pub fn new(query: String) -> Self {
        Self(query)
    }

    /// Get the query string
    pub fn query(&self) -> &str {
        &self.0
    }
}

/// Accepts a [UdfQuery] and invokes the query using DataFusion
pub struct UdfQueryInvocator<'a> {
    /// DataFusion session context
    session_ctx: SessionContext,
    /// Pre-compiled Python WASM component
    python_component: &'a WasmComponentPrecompiled,
}

impl std::fmt::Debug for UdfQueryInvocator<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdfQueryInvocator")
            .field("session_ctx", &"SessionContext { ... }")
            .field("python_component", &self.python_component)
            .finish()
    }
}

impl<'a> UdfQueryInvocator<'a> {
    /// Registers the UDF query in DataFusion
    pub async fn new(
        session_ctx: SessionContext,
        python_component: &'a WasmComponentPrecompiled,
    ) -> DataFusionResult<Self> {
        Ok(Self {
            session_ctx,
            python_component,
        })
    }

    /// Invoke the query, returning a result
    pub async fn invoke(&mut self, udf_query: UdfQuery) -> DataFusionResult<Vec<Vec<String>>> {
        let query_str = udf_query.query();

        // Parse the combined query to extract CREATE FUNCTION and SELECT statements
        let (python_code, sql_query) = self.parse_combined_query(query_str)?;

        // Create Python UDFs from the code
        let udfs = WasmScalarUdf::new(self.python_component, python_code).await?;

        // Register UDFs with DataFusion
        for udf in udfs {
            let scalar_udf = ScalarUDF::new_from_impl(udf);
            self.session_ctx.register_udf(scalar_udf);
        }

        // Execute the SQL query
        let df = self.session_ctx.sql(&sql_query).await?;
        let batches = df.collect().await?;

        // Convert results to strings for easy testing/display
        let mut results = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let column = batch.column(col_idx);
                    let value = arrow::util::display::array_value_to_string(column, row_idx)?;
                    row.push(value);
                }
                results.push(row);
            }
        }

        Ok(results)
    }

    /// Parse the combined query to extract Python code and SQL
    fn parse_combined_query(&self, query: &str) -> DataFusionResult<(String, String)> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, query)
            .map_err(|e| DataFusionError::Plan(format!("Failed to parse SQL: {}", e)))?;

        let mut python_code = String::new();
        let mut sql_statements = Vec::new();

        for statement in statements {
            match statement {
                Statement::CreateFunction(create_function) => {
                    let function_body = create_function.function_body;
                    let language = create_function.language;

                    // Verify it's a Python function
                    if let Some(lang) = language
                        && lang.to_string().to_lowercase() != "python"
                    {
                        return Err(DataFusionError::Plan(format!(
                            "Only Python language is supported, got: {}",
                            lang
                        )));
                    }

                    // Extract Python code from function body
                    if let Some(body) = function_body {
                        match body {
                            CreateFunctionBody::AsAfterOptions(e)
                            | CreateFunctionBody::AsBeforeOptions(e) => match e {
                                Expr::Value(v) => match v.value {
                                    Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                                        python_code.push_str(&s);
                                        python_code.push('\n');
                                    }
                                    _ => {
                                        return Err(DataFusionError::Plan(
                                            "Function body must be a string".to_string(),
                                        ));
                                    }
                                },
                                _ => {
                                    return Err(DataFusionError::Plan(
                                        "Function body must be a string".to_string(),
                                    ));
                                }
                            },
                            _ => {
                                return Err(DataFusionError::Plan(
                                    "Unsupported function body type".to_string(),
                                ));
                            }
                        }
                    }
                }
                _ => {
                    // All other statements (like SELECT) are treated as SQL
                    sql_statements.push(statement.to_string());
                }
            }
        }

        if python_code.is_empty() {
            return Err(DataFusionError::Plan(
                "No Python UDF found in query".to_string(),
            ));
        }

        if sql_statements.is_empty() {
            return Err(DataFusionError::Plan("No SQL query found".to_string()));
        }

        let sql_query = sql_statements.join(";\n");

        Ok((python_code, sql_query))
    }
}
