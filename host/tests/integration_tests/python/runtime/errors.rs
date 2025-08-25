use std::sync::Arc;

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field},
};
use datafusion_common::DataFusionError;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

use crate::integration_tests::python::test_utils::python_scalar_udf;

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
async fn test_exception() {
    const CODE: &str = "
def foo(x: int) -> int:
    raise Exception('bar')
";

    insta::assert_snapshot!(
        err(CODE).await,
        @"Execution error: cannot call function: Exception: bar",
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
