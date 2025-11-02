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

#[tokio::test(flavor = "multi_thread")]
async fn test_binary_with_null_bytes() {
    const CODE: &str = "
def foo(x: bytes) -> bytes:
    null_count = x.count(b'\\x00')
    return x + f'_nulls_{null_count}'.encode('utf-8')
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(BinaryArray::from_iter([
                Some(b"hello\x00world\x00".as_slice()),
                Some(b"no_nulls".as_slice()),
                Some(b"\x00\x00\x00".as_slice()),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Binary, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Binary, true)),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &BinaryArray::from_iter([
            Some(b"hello\x00world\x00_nulls_2".as_slice()),
            Some(b"no_nulls_nulls_0".as_slice()),
            Some(b"\x00\x00\x00_nulls_3".as_slice()),
        ]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bytes_slice_operations() {
    const CODE: &str = "
def foo(x: bytes) -> bytes:
    if len(x) < 3:
        return x
    return x[::-1]
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(BinaryArray::from_iter([
                Some(b"abc".as_slice()),
                Some(b"ab".as_slice()),
                Some(b"hello world".as_slice()),
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
            Some(b"cba".as_slice()),
            Some(b"ab".as_slice()),
            Some(b"dlrow olleh".as_slice()),
            Some(b"".as_slice()),
        ]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bytes_encoding_operations() {
    const CODE: &str = "
def foo(x: bytes) -> bytes:
    try:
        decoded = x.decode('utf-8')
        return decoded.upper().encode('utf-8')
    except UnicodeDecodeError:
        return x + b'_invalid_utf8'
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(BinaryArray::from_iter([
                Some(b"hello".as_slice()),
                Some(b"\xff\xfe\xfd".as_slice()), // Invalid UTF-8
                Some("café".as_bytes()),          // Valid UTF-8 with accents
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Binary, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Binary, true)),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &BinaryArray::from_iter([
            Some(b"HELLO".as_slice()),
            Some(b"\xff\xfe\xfd_invalid_utf8".as_slice()),
            Some("CAFÉ".as_bytes()),
        ]),
    );
}
