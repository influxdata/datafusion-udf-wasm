use std::sync::Arc;

use arrow::{
    array::{Array, BooleanArray},
    datatypes::{DataType, Field},
};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    async_udf::AsyncScalarUDFImpl,
};

use crate::integration_tests::python::test_utils::python_scalar_udf;

#[tokio::test]
async fn test_ok() {
    const CODE: &str = "
def foo(x: bool) -> bool:
    return not x
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(vec![DataType::Boolean], Volatility::Volatile),
    );

    assert_eq!(
        udf.return_type(&[DataType::Boolean]).unwrap(),
        DataType::Boolean,
    );

    let array = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(BooleanArray::from_iter([
                    Some(true),
                    None,
                    Some(false),
                ])))],
                arg_fields: vec![Arc::new(Field::new("a1", DataType::Boolean, true))],
                number_rows: 3,
                return_field: Arc::new(Field::new("r", DataType::Boolean, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        array.as_ref(),
        &BooleanArray::from_iter([Some(false), None, Some(true)]) as &dyn Array,
    );
}

#[tokio::test]
async fn test_return_int() {
    const CODE: &str = "
def foo(x: bool) -> bool:
    return 1
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(BooleanArray::from_iter([
                    Some(true),
                ])))],
                arg_fields: vec![Arc::new(Field::new("a1", DataType::Boolean, true))],
                number_rows: 1,
                return_field: Arc::new(Field::new("r", DataType::Boolean, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected bool but got `1` of type `int`",
    );
}
