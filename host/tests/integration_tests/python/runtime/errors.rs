use std::sync::Arc;

use arrow::{
    array::{Float64Array, Int64Array},
    datatypes::{DataType, Field},
};
use datafusion_common::DataFusionError;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_udf_wasm_host::test_utils::python::python_scalar_udf;

#[tokio::test(flavor = "multi_thread")]
async fn test_return_type_param_mismatch() {
    const CODE: &str = "
def foo(x: int) -> int:
    return x
";

    let udf = python_scalar_udf(CODE).await.unwrap();

    insta::assert_snapshot!(
        udf.return_type(&[]).unwrap_err(),
        @"Error during planning: `foo` expects 1 parameters but got 0",
    );

    insta::assert_snapshot!(
        udf.return_type(&[DataType::Int64, DataType::Int64]).unwrap_err(),
        @"Error during planning: `foo` expects 1 parameters but got 2",
    );

    insta::assert_snapshot!(
        udf.return_type(&[DataType::Float64]).unwrap_err(),
        @"Error during planning: argument 1 of `foo` should be Int64, got Float64",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_invoke_args_mismatch() {
    const CODE: &str = "
def foo(x: int) -> int:
    return x
";

    let udf = python_scalar_udf(CODE).await.unwrap();

    insta::assert_snapshot!(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![Arc::new(Field::new("x", DataType::Int64, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap_err(),
        @"Execution error: `foo` expects 1 parameters (passed as args) but got 0",
    );

    insta::assert_snapshot!(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                    Some(1),
                ]))),
                ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                    Some(1),
                ]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("x", DataType::Int64, true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap_err(),
        @"Execution error: `foo` expects 1 parameters (passed as args) but got 2",
    );

    insta::assert_snapshot!(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Float64Array::from_iter([
                Some(1.0),
            ])))],
            arg_fields: vec![Arc::new(Field::new("x", DataType::Int64, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap_err(),
        @r"
    Internal error: could not cast array of type Float64 to arrow_array::array::primitive_array::PrimitiveArray<arrow_array::types::Int64Type>.
    This issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues
    ",
    );

    insta::assert_snapshot!(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                    Some(1),
                ]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("x", DataType::Int64, true)),
            ],
            number_rows: 2,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap_err(),
        @"Execution error: array passed for argument 1 should have 2 rows but has 1",
    );

    insta::assert_snapshot!(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                    Some(1),
                    Some(1),
                ]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("x", DataType::Int64, true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap_err(),
        @"Execution error: array passed for argument 1 should have 1 rows but has 2",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_invoke_arg_fields_mismatch() {
    const CODE: &str = "
def foo(x: int) -> int:
    return x
";

    let udf = python_scalar_udf(CODE).await.unwrap();

    insta::assert_snapshot!(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                Some(1),
            ])))],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap_err(),
        @"Execution error: checking argument fields: `foo` expects 1 parameters but got 0",
    );

    insta::assert_snapshot!(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                Some(1),
            ])))],
            arg_fields: vec![
                Arc::new(Field::new("x", DataType::Int64, true)),
                Arc::new(Field::new("y", DataType::Int64, true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap_err(),
        @"Execution error: checking argument fields: `foo` expects 1 parameters but got 2",
    );

    insta::assert_snapshot!(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                Some(1),
            ])))],
            arg_fields: vec![Arc::new(Field::new("x", DataType::Float64, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap_err(),
        @"Execution error: checking argument fields: argument 1 of `foo` should be Int64, got Float64",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_invoke_return_field_mismatch() {
    const CODE: &str = "
def foo(x: int) -> int:
    return x
";

    let udf = python_scalar_udf(CODE).await.unwrap();

    insta::assert_snapshot!(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                Some(1),
            ])))],
            arg_fields: vec![Arc::new(Field::new("x", DataType::Int64, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Float64, true)),
        })
        .unwrap_err(),
        @"Execution error: `foo` returns Int64 but was asked to produce Float64",
    );

    insta::assert_snapshot!(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                Some(1),
            ])))],
            arg_fields: vec![Arc::new(Field::new("x", DataType::Int64, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Int64, false)),
        })
        .unwrap_err(),
        @"Execution error: `foo` returns nullable data but was asked not to do so",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_should_not_return_none() {
    const CODE: &str = "
def foo(x: int) -> int:
    return None
";

    insta::assert_snapshot!(
        err(CODE).await,
        @"Execution error: method was not supposed to return None but did",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_exception_direct() {
    const CODE: &str = "
def foo(x: int) -> int:
    raise Exception('bar')
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r#"
    cannot call function
    caused by
    Execution error: Traceback (most recent call last):
      File "<string>", line 3, in foo
    Exception: bar
    "#,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_exception_indirect() {
    const CODE: &str = "
def _inner1() -> None:
    try:
        _inner2()
    except ValueError as e:
        raise RuntimeError('foo') from e

def _inner2() -> None:
    _inner3()

def _inner3() -> None:
    raise ValueError('bar')


def foo(x: int) -> int:
    try:
        _inner1()
    except RuntimeError as e:
        raise TypeError('baz') from e
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r#"
    cannot call function
    caused by
    Execution error: Traceback (most recent call last):
      File "<string>", line 4, in _inner1
      File "<string>", line 9, in _inner2
      File "<string>", line 12, in _inner3
    ValueError: bar

    The above exception was the direct cause of the following exception:

    Traceback (most recent call last):
      File "<string>", line 17, in foo
      File "<string>", line 6, in _inner1
    RuntimeError: foo

    The above exception was the direct cause of the following exception:

    Traceback (most recent call last):
      File "<string>", line 19, in foo
    TypeError: baz
    "#,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stack_overflow() {
    // borrowed from https://wiki.python.org/moin/CrashingPython
    const CODE: &str = "
def foo(x: int) -> int:
    f = lambda: None
    for i in range(1_000_000):
        f = f.__call__
    del f

    return x
";
    insta::assert_snapshot!(
        // the error is long because it contains the backtrace, so only assert the first line
        err(CODE).await.to_string().split("\n").next().unwrap(),
        @"call ScalarUdf::invoke_with_args",
    );
}

async fn err(code: &str) -> DataFusionError {
    let udf = python_scalar_udf(code).await.unwrap();
    udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
            Some(1),
        ])))],
        arg_fields: vec![Arc::new(Field::new("x", DataType::Int64, true))],
        number_rows: 1,
        return_field: Arc::new(Field::new("r", DataType::Int64, true)),
    })
    .unwrap_err()
}
