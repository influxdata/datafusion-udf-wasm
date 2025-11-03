//! Embedded SQL approach for executing Python UDFs within SQL queries.

use std::collections::HashMap;

use datafusion::execution::TaskContext;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_sql::parser::{DFParserBuilder, Statement};
use sqlparser::ast::{CreateFunctionBody, Expr, Statement as SqlStatement, Value};
use sqlparser::dialect::dialect_from_str;

use crate::{WasmComponentPrecompiled, WasmScalarUdf};

/// A [ParsedQuery] contains the extracted UDFs and SQL query string
#[derive(Debug)]
pub struct ParsedQuery {
    /// Extracted UDFs from the query
    pub udfs: Vec<WasmScalarUdf>,
    /// SQL query string with UDF definitions removed
    pub sql: String,
}

/// Handles the registration and invocation of UDF queries in DataFusion with a
/// pre-compiled WASM component.
pub struct UdfQueryParser<'a> {
    /// Pre-compiled WASM component.
    /// Necessary to create UDFs.
    components: HashMap<String, &'a WasmComponentPrecompiled>,
}

impl std::fmt::Debug for UdfQueryParser<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdfQueryParser")
            .field("session_ctx", &"SessionContext { ... }")
            .field("components", &self.components)
            .finish()
    }
}

impl<'a> UdfQueryParser<'a> {
    /// Registers the UDF query in DataFusion.
    pub async fn new(
        components: HashMap<String, &'a WasmComponentPrecompiled>,
    ) -> DataFusionResult<Self> {
        Ok(Self { components })
    }

    /// Parses a SQL query that defines & uses Python UDFs into a [ParsedQuery].
    pub async fn parse(
        &self,
        udf_query: &str,
        task_ctx: &TaskContext,
    ) -> DataFusionResult<ParsedQuery> {
        let (code, sql, lang) = self.parse_inner(udf_query, task_ctx)?;

        let component = self.components.get(&lang).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "no WASM component registered for language: {:?}",
                lang
            ))
        })?;

        let udfs = WasmScalarUdf::new(component, code).await?;
        Ok(ParsedQuery { udfs, sql })
    }

    /// Parse the combined query to extract the chosen UDF language, UDF
    /// definitions, and SQL statements.
    fn parse_inner(
        &self,
        query: &str,
        task_ctx: &TaskContext,
    ) -> DataFusionResult<(String, String, String)> {
        let options = task_ctx.session_config().options();

        let dialect = dialect_from_str(options.sql_parser.dialect.clone()).expect("valid dialect");
        let recursion_limit = options.sql_parser.recursion_limit;

        let statements = DFParserBuilder::new(query)
            .with_dialect(dialect.as_ref())
            .with_recursion_limit(recursion_limit)
            .build()?
            .parse_statements()?;

        let mut udf_code = String::new();
        let mut sql = String::new();

        let mut udf_language = String::new();
        for s in statements {
            let Statement::Statement(stmt) = s else {
                continue;
            };

            match parse_udf(*stmt)? {
                Parsed::Udf { code, language } => {
                    udf_language = language;
                    udf_code.push_str(&code);
                    udf_code.push('\n');
                }
                Parsed::Other(statement) => {
                    sql.push_str(&statement);
                    sql.push_str(";\n");
                }
            }
        }

        if udf_code.is_empty() {
            return Err(DataFusionError::Plan(
                "UDF not defined in query".to_string(),
            ));
        }

        if sql.is_empty() {
            return Err(DataFusionError::Plan("no SQL query found".to_string()));
        }

        Ok((udf_code, sql, udf_language))
    }
}

/// Represents a parsed SQL statement
enum Parsed {
    /// A UDF definition
    Udf {
        /// UDF code
        code: String,
        /// UDF language
        language: String,
    },
    /// Any other SQL statement
    Other(String),
}

/// Parse a single SQL statement to extract a UDF
fn parse_udf(stmt: SqlStatement) -> DataFusionResult<Parsed> {
    match stmt {
        SqlStatement::CreateFunction(cf) => {
            let function_body = cf.function_body.as_ref();

            let language = if let Some(lang) = cf.language.as_ref() {
                lang.to_string()
            } else {
                return Err(DataFusionError::Plan(
                    "function language is required for UDFs".to_string(),
                ));
            };

            let code = match function_body {
                Some(body) => extract_function_body(body),
                None => Err(DataFusionError::Plan(
                    "function body is required for UDFs".to_string(),
                )),
            }?;

            Ok(Parsed::Udf {
                code: code.to_string(),
                language,
            })
        }
        _ => Ok(Parsed::Other(stmt.to_string())),
    }
}

/// Extracts the code from the function body, adding it to `code`.
fn extract_function_body(body: &CreateFunctionBody) -> DataFusionResult<&str> {
    match body {
        CreateFunctionBody::AsAfterOptions(e) | CreateFunctionBody::AsBeforeOptions(e) => {
            expression_into_str(e)
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
