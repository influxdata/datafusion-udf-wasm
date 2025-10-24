//! Embedded SQL approach for executing Python UDFs within SQL queries.

use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::ScalarUDF;
use datafusion_sql::parser::{DFParserBuilder, Statement};
use sqlparser::ast::{CreateFunctionBody, Expr, Statement as SqlStatement, Value};
use sqlparser::dialect::dialect_from_str;

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
    component: &'a WasmComponentPrecompiled,
}

impl std::fmt::Debug for UdfQueryInvocator<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdfQueryInvocator")
            .field("session_ctx", &"SessionContext { ... }")
            .field("python_component", &self.component)
            .finish()
    }
}

impl<'a> UdfQueryInvocator<'a> {
    /// Registers the UDF query in DataFusion
    pub async fn new(
        session_ctx: SessionContext,
        component: &'a WasmComponentPrecompiled,
    ) -> DataFusionResult<Self> {
        Ok(Self {
            session_ctx,
            component,
        })
    }

    /// Invoke the query, returning a result
    pub async fn invoke(&mut self, udf_query: UdfQuery) -> DataFusionResult<DataFrame> {
        let query_str = udf_query.query();

        let (code, sql_query) = self.parse_combined_query(query_str)?;

        let udfs = WasmScalarUdf::new(self.component, code).await?;

        for udf in udfs {
            let scalar_udf = ScalarUDF::new_from_impl(udf);
            self.session_ctx.register_udf(scalar_udf);
        }

        self.session_ctx.sql(&sql_query).await
    }

    /// Parse the combined query to extract Python code and SQL
    fn parse_combined_query(&self, query: &str) -> DataFusionResult<(String, String)> {
        let task_ctx = self.session_ctx.task_ctx();
        let options = task_ctx.session_config().options();

        let dialect = dialect_from_str(options.sql_parser.dialect.clone()).expect("valid dialect");
        let recursion_limit = options.sql_parser.recursion_limit;

        let statements = DFParserBuilder::new(query)
            .with_dialect(dialect.as_ref())
            .with_recursion_limit(recursion_limit)
            .build()?
            .parse_statements()?;

        let mut code = String::new();
        let mut sql_statements = Vec::new();

        for s in statements {
            if let Statement::Statement(stmt) = s {
                parse_udf(*stmt, &mut code, &mut sql_statements)?;
            }
        }

        if code.is_empty() {
            return Err(DataFusionError::Plan(
                "no Python UDF found in query".to_string(),
            ));
        }

        if sql_statements.is_empty() {
            return Err(DataFusionError::Plan("no SQL query found".to_string()));
        }

        let sql_query = sql_statements
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>()
            .join(";\n");

        Ok((code, sql_query))
    }
}

/// Parse a single SQL statement to extract a UDF
fn parse_udf(
    stmt: SqlStatement,
    code: &mut String,
    sql: &mut Vec<SqlStatement>,
) -> DataFusionResult<()> {
    match stmt {
        SqlStatement::CreateFunction(cf) => {
            let function_body = cf.function_body.as_ref();
            let language = cf.language.as_ref();

            if let Some(lang) = language
                && lang.to_string().to_lowercase() != "python"
            {
                return Err(DataFusionError::Plan(format!(
                    "only Python is supported, got: {}",
                    lang
                )));
            }

            match function_body {
                Some(body) => extract_function_body(body, code),
                None => Err(DataFusionError::Plan(
                    "function body is required for Python UDFs".to_string(),
                )),
            }
        }
        _ => {
            sql.push(stmt);
            Ok(())
        }
    }
}

/// Extracts the code from the function body, adding it to `code`.
fn extract_function_body(body: &CreateFunctionBody, code: &mut String) -> DataFusionResult<()> {
    match body {
        CreateFunctionBody::AsAfterOptions(e) | CreateFunctionBody::AsBeforeOptions(e) => {
            let s = expression_into_str(e)?;
            code.push_str(s);
            code.push('\n');
            Ok(())
        }
        CreateFunctionBody::Return(_) => Err(DataFusionError::Plan(
            "`RETURN` function body not supported for Python UDFs".to_string(),
        )),
    }
}

/// Attempt to convert an `Expr` into a `str`
fn expression_into_str(expr: &Expr) -> DataFusionResult<&str> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(s),
            _ => Err(DataFusionError::Plan("expected string value".to_string())),
        },
        _ => Err(DataFusionError::Plan(
            "expected value expression".to_string(),
        )),
    }
}
