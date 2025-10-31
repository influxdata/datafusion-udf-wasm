use std::sync::Arc;

use arrow::{
    array::BinaryArray,
    datatypes::{DataType, Field},
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::integration_tests::{
    python::test_utils::python_scalar_udf, test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_ok() {
    const CODE: &str = "
def foo(x: bytes) -> bytes:
    return x + b'_suffix'
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(vec![DataType::Binary], Volatility::Volatile),
    );

    assert_eq!(
        udf.return_type(&[DataType::Binary]).unwrap(),
        DataType::Binary,
    );

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(BinaryArray::from_iter([
                Some(b"hello".as_slice()),
                None,
                Some(b"world".as_slice()),
                Some(b"".as_slice()),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Binary, true))],
            number_rows: 4,
            return_field: Arc::new(Field::new("r", DataType::Binary, true)),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &BinaryArray::from_iter([
            Some(b"hello_suffix".as_slice()),
            None,
            Some(b"world_suffix".as_slice()),
            Some(b"_suffix".as_slice()),
        ]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_str() {
    const CODE: &str = "
def foo(x: bytes) -> bytes:
    return 'not bytes'
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(BinaryArray::from_iter([
                Some(b"hello".as_slice()),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Binary, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Binary, true)),
        })
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected `bytes` but got `not bytes` of type `str`",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_int() {
    const CODE: &str = "
def foo(x: bytes) -> bytes:
    return 42
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(BinaryArray::from_iter([
                Some(b"hello".as_slice()),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Binary, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Binary, true)),
        })
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected `bytes` but got `42` of type `int`",
    );
}
