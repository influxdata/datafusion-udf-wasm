//! Test special forms of arguments in Python functions.
//!
//! See <https://docs.python.org/3/library/inspect.html#inspect.Parameter.kind>.
use std::sync::Arc;

use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    async_udf::AsyncScalarUDFImpl,
};

use crate::integration_tests::python::test_utils::python_scalar_udf;

#[tokio::test(flavor = "multi_thread")]
async fn test_positional_or_keyword() {
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
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                    Some(3),
                    None,
                    Some(-10),
                ])))],
                arg_fields: vec![Arc::new(Field::new("a1", DataType::Int64, true))],
                number_rows: 3,
                return_field: Arc::new(Field::new("r", DataType::Int64, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        array.as_ref(),
        &Int64Array::from_iter([Some(4), None, Some(-9)]) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_positional_or_keyword_default() {
    const CODE: &str = "
def foo(x: int = 1) -> int:
    return x + 1
";
    let err = python_scalar_udf(CODE).await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: default parameter values are not supported, got `1` of type `int`

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `foo`
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_positional_only() {
    const CODE: &str = "
def foo(x: int, /) -> int:
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
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                    Some(3),
                    None,
                    Some(-10),
                ])))],
                arg_fields: vec![Arc::new(Field::new("a1", DataType::Int64, true))],
                number_rows: 3,
                return_field: Arc::new(Field::new("r", DataType::Int64, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        array.as_ref(),
        &Int64Array::from_iter([Some(4), None, Some(-9)]) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_positional_only_default() {
    const CODE: &str = "
def foo(x: int = 1, /) -> int:
    return x + 1
";
    let err = python_scalar_udf(CODE).await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: default parameter values are not supported, got `1` of type `int`

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `foo`
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_positional_or_keyword_and_positional_only() {
    const CODE: &str = "
def foo(x: int, /, y: int) -> int:
    return x + y
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(vec![DataType::Int64, DataType::Int64], Volatility::Volatile),
    );

    assert_eq!(
        udf.return_type(&[DataType::Int64, DataType::Int64])
            .unwrap(),
        DataType::Int64,
    );

    let array = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(Int64Array::from_iter([Some(3)]))),
                    ColumnarValue::Array(Arc::new(Int64Array::from_iter([Some(4)]))),
                ],
                arg_fields: vec![
                    Arc::new(Field::new("a1", DataType::Int64, true)),
                    Arc::new(Field::new("a2", DataType::Int64, true)),
                ],
                number_rows: 1,
                return_field: Arc::new(Field::new("r", DataType::Int64, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        array.as_ref(),
        &Int64Array::from_iter([Some(7)]) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_var_positional() {
    const CODE: &str = "
def foo(*x: int) -> int:
    return x + 1
";
    let err = python_scalar_udf(CODE).await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: only paramters of kind `POSITIONAL_OR_KEYWORD` and `POSITIONAL_ONLY` are supported, got VAR_POSITIONAL

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `foo`
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_keyword_only() {
    const CODE: &str = "
def foo(*, x: int) -> int:
    return x + 1
";
    let err = python_scalar_udf(CODE).await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: only paramters of kind `POSITIONAL_OR_KEYWORD` and `POSITIONAL_ONLY` are supported, got KEYWORD_ONLY

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `foo`
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_var_keyword() {
    const CODE: &str = "
def foo(**x: int) -> int:
    return x + 1
";
    let err = python_scalar_udf(CODE).await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: only paramters of kind `POSITIONAL_OR_KEYWORD` and `POSITIONAL_ONLY` are supported, got VAR_KEYWORD

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `foo`
    ",
    );
}
