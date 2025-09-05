use std::sync::Arc;

use arrow::{
    array::{Array, BooleanArray},
    datatypes::{DataType, Field},
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::integration_tests::{
    python::test_utils::python_scalar_udf, test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
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
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(BooleanArray::from_iter([
                Some(true),
                None,
                Some(false),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Boolean, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Boolean, true)),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &BooleanArray::from_iter([Some(false), None, Some(true)]) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_int() {
    const CODE: &str = "
def foo(x: bool) -> bool:
    return 1
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(BooleanArray::from_iter([
                Some(true),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Boolean, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Boolean, true)),
        })
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected bool but got `1` of type `int`",
    );
}
