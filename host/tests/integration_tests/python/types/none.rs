use std::sync::Arc;

use arrow::{
    array::{Array, NullArray},
    datatypes::{DataType, Field},
};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    async_udf::AsyncScalarUDFImpl,
};

use crate::integration_tests::python::test_utils::python_scalar_udf;

#[tokio::test(flavor = "multi_thread")]
async fn test_ok() {
    const CODE: &str = "
def foo(x: None) -> None:
    return None
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(vec![DataType::Null], Volatility::Volatile),
    );

    assert_eq!(udf.return_type(&[DataType::Null]).unwrap(), DataType::Null,);

    let array = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(NullArray::new(3)))],
                arg_fields: vec![Arc::new(Field::new("a1", DataType::Null, true))],
                number_rows: 3,
                return_field: Arc::new(Field::new("r", DataType::Null, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(array.as_ref(), &NullArray::new(3) as &dyn Array,);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_implicit_return_pass() {
    const CODE: &str = "
def foo() -> None:
    pass
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: 3,
                return_field: Arc::new(Field::new("r", DataType::Null, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(array.as_ref(), &NullArray::new(3) as &dyn Array,);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_implicit_return_ellipsis() {
    const CODE: &str = "
def foo() -> None:
    ...
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: 3,
                return_field: Arc::new(Field::new("r", DataType::Null, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(array.as_ref(), &NullArray::new(3) as &dyn Array,);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_implicit_return_docstring() {
    const CODE: &str = r#"
def foo() -> None:
    """
    Some docs.
    """
"#;
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: 3,
                return_field: Arc::new(Field::new("r", DataType::Null, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(array.as_ref(), &NullArray::new(3) as &dyn Array,);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_err_zero_returned() {
    const CODE: &str = "
def foo() -> None:
    return 0
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: 3,
                return_field: Arc::new(Field::new("r", DataType::Null, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"Execution error: expected `None` but got `0` of type `int`",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_err_false_returned() {
    const CODE: &str = "
def foo() -> None:
    return False
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: 3,
                return_field: Arc::new(Field::new("r", DataType::Null, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"Execution error: expected `None` but got `False` of type `bool`",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_err_empty_tuple_returned() {
    const CODE: &str = "
def foo() -> None:
    return ()
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: 3,
                return_field: Arc::new(Field::new("r", DataType::Null, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"Execution error: expected `None` but got `()` of type `tuple`",
    );
}
