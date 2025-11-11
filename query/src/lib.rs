//! Embedded SQL approach for executing UDFs within SQL queries.
#![allow(unused_crate_dependencies)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::async_udf::AsyncScalarUDFImpl;
use datafusion_sql::parser::{DFParserBuilder, Statement};
use sqlparser::ast::{CreateFunctionBody, Expr, Statement as SqlStatement, Value};
use sqlparser::dialect::dialect_from_str;

use datafusion_udf_wasm_host::{WasmComponentPrecompiled, WasmPermissions, WasmScalarUdf};
use tokio::runtime::Handle;

use crate::format::UdfCodeFormatter;

/// Module for UDF code formatting implementations
pub mod format;

/// Represents a supported UDF language with its associated WASM component
/// and code formatter.
#[derive(Debug)]
pub struct Lang<'a> {
    /// Pre-compiled WASM component for the language
    pub component: &'a WasmComponentPrecompiled,
    /// Code formatter for the language
    pub formatter: Box<dyn UdfCodeFormatter>,
}

/// A [ParsedQuery] contains the extracted UDFs and SQL query string
#[derive(Debug)]
pub struct ParsedQuery {
    /// Extracted UDFs from the query
    pub udfs: Vec<Arc<dyn AsyncScalarUDFImpl>>,
    /// SQL query string with UDF definitions removed
    pub sql: String,
}

/// Handles the registration and invocation of UDF queries in DataFusion with a
/// pre-compiled WASM component.
pub struct UdfQueryParser<'a> {
    /// Map of strings (eg "python") to supported UDF languages and their WASM
    /// components
    components: HashMap<String, Lang<'a>>,
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
    pub fn new(components: HashMap<String, Lang<'a>>) -> Self {
        Self { components }
    }

    /// Parses a SQL query that defines & uses UDFs into a [ParsedQuery].
    pub async fn parse(
        &self,
        udf_query: &str,
        permissions: &WasmPermissions,
        io_rt: Handle,
        task_ctx: &TaskContext,
    ) -> DataFusionResult<ParsedQuery> {
        let (code, sql) = self.parse_inner(udf_query, task_ctx)?;

        let mut udfs = vec![];
        for (lang, blocks) in code {
            let lang = self.components.get(&lang).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "no WASM component registered for language: {:?}",
                    lang
                ))
            })?;

            for code in blocks {
                let code = lang.formatter.format(code);
                udfs.extend(
                    WasmScalarUdf::new(lang.component, permissions, io_rt.clone(), code).await?,
                );
            }
        }

        Ok(ParsedQuery { udfs, sql })
    }

    /// Parse the combined query to extract the chosen UDF language, UDF
    /// definitions, and SQL statements.
    fn parse_inner(
        &self,
        query: &str,
        task_ctx: &TaskContext,
    ) -> DataFusionResult<(HashMap<String, Vec<String>>, String)> {
        let options = task_ctx.session_config().options();

        let dialect = dialect_from_str(options.sql_parser.dialect.clone()).expect("valid dialect");
        let recursion_limit = options.sql_parser.recursion_limit;

        let statements = DFParserBuilder::new(query)
            .with_dialect(dialect.as_ref())
            .with_recursion_limit(recursion_limit)
            .build()?
            .parse_statements()?;

        let mut sql = String::new();
        let mut udf_blocks: HashMap<String, Vec<String>> = HashMap::new();
        for s in statements {
            match parse_udf(s)? {
                Parsed::Udf { code, language } => {
                    if let Some(existing) = udf_blocks.get_mut(&language) {
                        existing.push(code);
                    } else {
                        udf_blocks.insert(language.clone(), vec![code]);
                    }
                }
                Parsed::Other(statement) => {
                    sql.push_str(&statement);
                    sql.push_str(";\n");
                }
            }
        }

        if sql.is_empty() {
            return Err(DataFusionError::Plan("no SQL query found".to_string()));
        }

        Ok((udf_blocks, sql))
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
fn parse_udf(stmt: Statement) -> DataFusionResult<Parsed> {
    match stmt {
        Statement::Statement(stmt) => match *stmt {
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
        },
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
            "`RETURN` function body not supported for UDFs".to_string(),
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
