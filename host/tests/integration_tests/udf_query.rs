use datafusion::common::Result as DataFusionResult;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_udf_wasm_host::udf_query::{UdfQuery, UdfQueryInvocator};

use crate::integration_tests::python::test_utils::python_component;

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_udf_query() {
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
    let python_component = python_component().await;

    let udf_query = UdfQuery::new(query.to_string());
    let mut invocator = UdfQueryInvocator::new(ctx, python_component).await.unwrap();

    let df = invocator.invoke(udf_query).await.unwrap();
    let result = dataframe_to_string_matrix(df).await.unwrap();

    // Verify the result
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].len(), 1);
    assert_eq!(result[0][0], "2");
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
    let python_component = python_component().await;

    let udf_query = UdfQuery::new(query.to_string());
    let mut invocator = UdfQueryInvocator::new(ctx, python_component).await.unwrap();

    let df = invocator.invoke(udf_query).await.unwrap();
    let result = dataframe_to_string_matrix(df).await.unwrap();

    // Verify the result
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].len(), 2);
    assert_eq!(result[0][0], "2"); // add_one(1) = 2
    assert_eq!(result[0][1], "6"); // multiply_two(3) = 6
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
    let python_component = python_component().await;

    let udf_query = UdfQuery::new(query.to_string());
    let mut invocator = UdfQueryInvocator::new(ctx, python_component).await.unwrap();

    let df = invocator.invoke(udf_query).await.unwrap();
    let result = dataframe_to_string_matrix(df).await.unwrap();

    // Verify the result
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].len(), 2);
    assert_eq!(result[0][0], "2"); // add_one(1) = 2
    assert_eq!(result[0][1], "6"); // multiply_two(3) = 6
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
    let python_component = python_component().await;

    let udf_query = UdfQuery::new(query.to_string());
    let mut invocator = UdfQueryInvocator::new(ctx, python_component).await.unwrap();

    let r = invocator.invoke(udf_query).await;
    assert!(r.is_err());

    let err = r.err().unwrap();
    assert!(err.message().contains("Invalid function 'add_one'"));
}

/// Converts a DataFrame into a matrix of strings for easier verification
async fn dataframe_to_string_matrix(df: DataFrame) -> DataFusionResult<Vec<Vec<String>>> {
    let batches = df.collect().await?;

    let mut result = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                let value = arrow::util::display::array_value_to_string(column, row_idx)?;
                row.push(value);
            }
            result.push(row);
        }
    }

    Ok(result)
}
