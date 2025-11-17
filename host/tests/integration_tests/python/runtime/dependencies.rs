use std::sync::Arc;

use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, async_udf::AsyncScalarUDFImpl};

use crate::integration_tests::{
    python::test_utils::python_scalar_udf, test_utils::ColumnarValueExt,
};

#[tokio::test]
async fn call_other_function() {
    const CODE: &str = "
def _sub1(x: int) -> int:
    return x + 1

def _sub2(x: int) -> int:
    return x * 10

def foo(x: int) -> int:
    return _sub1(x) + _sub2(x)
";

    let udf = python_scalar_udf(CODE).await.unwrap();
    let array = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                Some(1),
                Some(2),
                Some(3),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Int64, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Int64Array::from_iter([Some(12), Some(23), Some(34)]) as &dyn Array,
    );
}

#[tokio::test]
async fn functools_cache() {
    const CODE: &str = "
from functools import cache

_counter = 0

@cache
def foo(x: int) -> int:
    global _counter
    _counter += 1
    return x + _counter
";

    let udf = python_scalar_udf(CODE).await.unwrap();
    let array = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                Some(10),
                Some(20),
                Some(10),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Int64, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Int64Array::from_iter([Some(11), Some(22), Some(11)]) as &dyn Array,
    );
}
