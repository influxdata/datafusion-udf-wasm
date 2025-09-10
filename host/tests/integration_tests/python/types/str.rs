use std::sync::Arc;

use arrow::{
    array::{Array, StringArray},
    datatypes::{DataType, Field},
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::integration_tests::{
    python::test_utils::python_scalar_udf, test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_ok() {
    const CODE: &str = "
def foo(x: str) -> str:
    return f'prefix_{x}_suffix'
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
    );

    assert_eq!(udf.return_type(&[DataType::Utf8]).unwrap(), DataType::Utf8,);

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from_iter([
                Some("hello".to_owned()),
                None,
                Some("".to_owned()),
                Some("w\x00rld".to_owned()),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Utf8, true))],
            number_rows: 4,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &StringArray::from_iter([
            Some("prefix_hello_suffix".to_owned()),
            None,
            Some("prefix__suffix".to_owned()),
            Some("prefix_w\x00rld_suffix".to_owned())
        ]) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_returning_bytes_fails() {
    const CODE: &str = "
def foo(x: str) -> str:
    return x.encode('utf-8')
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from_iter([
                Some("hello".to_owned()),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Utf8, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
        })
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected `str` but got `b'hello'` of type `bytes`",
    );
}
