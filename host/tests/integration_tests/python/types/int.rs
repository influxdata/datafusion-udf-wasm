use std::sync::Arc;

use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::integration_tests::{
    python::test_utils::python_scalar_udf, test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_ok() {
    const CODE: &str = "
def foo(x: int) -> int:
    return x + 1
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(vec![DataType::Int64], Volatility::Volatile),
    );

    assert_eq!(
        udf.return_type(&[DataType::Int64]).unwrap(),
        DataType::Int64,
    );

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                Some(3),
                None,
                Some(-10),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Int64, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Int64Array::from_iter([Some(4), None, Some(-9)]) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_bool() {
    const CODE: &str = "
def foo(x: int) -> int:
    return True
";

    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                Some(1),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Int64, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected `int` but got `True` of type `bool`",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_out_of_range() {
    const CODE: &str = "
def foo(x: int) -> int:
    i64_max = 9223372036854775807
    return i64_max + 1
";

    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                Some(1),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Int64, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected i64 but got `9223372036854775808` of type `int`, which is out-of-range",
    );
}
