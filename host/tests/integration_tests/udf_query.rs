use std::collections::HashMap;

use datafusion::{
    assert_batches_eq,
    prelude::{DataFrame, SessionContext},
};
use datafusion_common::Result as DataFusionResult;
use datafusion_udf_wasm_host::udf_query::{ParsedQuery, UdfQueryParser};

use crate::integration_tests::python::test_utils::python_component;

/// A helper struct for invoking UDF queries and validating their results.
struct UdfQueryInvocator;

impl UdfQueryInvocator {
    async fn invoke(
        ctx: &SessionContext,
        parsed_query: ParsedQuery,
    ) -> DataFusionResult<DataFrame> {
        for udf in parsed_query.udfs {
            let scalar_udf = datafusion_expr::ScalarUDF::new_from_impl(udf);
            ctx.register_udf(scalar_udf);
        }

        ctx.sql(&parsed_query.sql).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_basic() {
    let query = r#"
CREATE FUNCTION add_one()
LANGUAGE python
AS '
def add_one(x: int) -> int:
    return x + 1
';

SELECT add_one(1);
"#;

    let ctx = SessionContext::new();
    let component = python_component().await;

    let parser = UdfQueryParser::new(HashMap::from_iter([("python", component)]))
        .await
        .unwrap();
    let parsed_query = parser.parse(query, ctx.task_ctx().as_ref()).await.unwrap();

    let df = UdfQueryInvocator::invoke(&ctx, parsed_query).await.unwrap();
    let batch = df.collect().await.unwrap();

    assert_batches_eq!(
        [
            "+-------------------+",
            "| add_one(Int64(1)) |",
            "+-------------------+",
            "| 2                 |",
            "+-------------------+",
        ],
        &batch
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_functions() {
    let query = r#"
CREATE FUNCTION add_one()
LANGUAGE python
AS '
def add_one(x: int) -> int:
    return x + 1
';

CREATE FUNCTION multiply_two()
LANGUAGE python  
AS '
def multiply_two(x: int) -> int:
    return x * 2
';

SELECT add_one(1), multiply_two(3);
"#;

    let ctx = SessionContext::new();
    let component = python_component().await;

    let parser = UdfQueryParser::new(HashMap::from_iter([("python", component)]))
        .await
        .unwrap();
    let parsed_query = parser.parse(query, ctx.task_ctx().as_ref()).await.unwrap();

    let df = UdfQueryInvocator::invoke(&ctx, parsed_query).await.unwrap();
    let batch = df.collect().await.unwrap();

    assert_batches_eq!(
        [
            "+-------------------+------------------------+",
            "| add_one(Int64(1)) | multiply_two(Int64(3)) |",
            "+-------------------+------------------------+",
            "| 2                 | 6                      |",
            "+-------------------+------------------------+",
        ],
        &batch
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_functions_single_statement() {
    let query = r#"
CREATE FUNCTION add_one()
LANGUAGE python
AS '
def add_one(x: int) -> int:
    return x + 1

def multiply_two(x: int) -> int:
    return x * 2
';

SELECT add_one(1), multiply_two(3);
"#;

    let ctx = SessionContext::new();
    let component = python_component().await;

    let parser = UdfQueryParser::new(HashMap::from_iter([("python", component)]))
        .await
        .unwrap();
    let parsed_query = parser.parse(query, ctx.task_ctx().as_ref()).await.unwrap();

    let df = UdfQueryInvocator::invoke(&ctx, parsed_query).await.unwrap();
    let batch = df.collect().await.unwrap();

    assert_batches_eq!(
        [
            "+-------------------+------------------------+",
            "| add_one(Int64(1)) | multiply_two(Int64(3)) |",
            "+-------------------+------------------------+",
            "| 2                 | 6                      |",
            "+-------------------+------------------------+",
        ],
        &batch
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_string() {
    let query = r#"
CREATE FUNCTION add_one()
LANGUAGE python
AS '';

SELECT add_one(1)
"#;

    let ctx = SessionContext::new();
    let component = python_component().await;

    let parser = UdfQueryParser::new(HashMap::from_iter([("python", component)]))
        .await
        .unwrap();
    let parsed_query = parser.parse(query, ctx.task_ctx().as_ref()).await.unwrap();

    let r = UdfQueryInvocator::invoke(&ctx, parsed_query).await;
    assert!(r.is_err());

    let err = r.err().unwrap();
    assert!(err.message().contains("Invalid function 'add_one'"));
}
