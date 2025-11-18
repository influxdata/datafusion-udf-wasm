use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, Field};
use datafusion_common::{DataFusionError, config::ConfigOptions};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, async_udf::AsyncScalarUDFImpl};
use datafusion_udf_wasm_host::WasmScalarUdf;
use regex::Regex;

use crate::integration_tests::evil::test_utils::try_scalar_udfs;

#[tokio::test]
async fn test_fillstderr() {
    let udf = udf("fillstderr").await;

    insta::assert_snapshot!(
        err_call_no_params(&udf).await,
        @r"
    call ScalarUdf::invoke_with_args

    stderr:
    xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
    ",
    );
}

#[tokio::test]
async fn test_fillstdout() {
    let udf = udf("fillstdout").await;

    // We do NOT store or print the stdout data (in contrast to stderr). Writes to stdout are discarded, so there is
    // no limit. Hence, this payload never fails.
    try_call_no_params(&udf).await.unwrap();
}

#[tokio::test]
async fn test_divzero() {
    let udf = udf("divzero").await;

    insta::assert_snapshot!(
        normalize_panic_location(err_call_no_params(&udf).await),
        @r"
    call ScalarUdf::invoke_with_args

    stderr:

    thread '<unnamed>' (1) panicked at <FILE>:<LINE>:<ROW>:
    attempt to divide by zero
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
    ",
    );
}

#[tokio::test]
async fn test_maxptr() {
    let udf = udf("maxptr").await;
    let err = err_call_no_params(&udf).await.to_string();

    // linear memory size is nondeterministic
    let err = Regex::new(r#"size 0x[0-9a-f]+"#)
        .unwrap()
        .replace_all(&err, "size <SIZE>");

    insta::assert_snapshot!(
        err,
        @r"
    call ScalarUdf::invoke_with_args
    caused by
    External error: memory fault at wasm address 0xffffffff in linear memory of size <SIZE>
    ",
    );
}

#[tokio::test]
async fn test_nullptr() {
    let udf = udf("nullptr").await;

    insta::assert_snapshot!(
        normalize_panic_location(err_call_no_params(&udf).await),
        @r"
    call ScalarUdf::invoke_with_args

    stderr:

    thread '<unnamed>' (1) panicked at <FILE>:<LINE>:<ROW>:
    null pointer dereference occurred
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
    thread caused non-unwinding panic. aborting.

    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
    ",
    );
}

#[tokio::test]
async fn test_panic() {
    let udf = udf("panic").await;

    insta::assert_snapshot!(
        normalize_panic_location(err_call_no_params(&udf).await),
        @r"
    call ScalarUdf::invoke_with_args

    stderr:

    thread '<unnamed>' (1) panicked at <FILE>:<LINE>:<ROW>:
    foo
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
    ",
    );
}

#[tokio::test]
async fn test_stackoverflow() {
    let udf = udf("stackoverflow").await;

    insta::assert_snapshot!(
        err_call_no_params(&udf).await,
        @r"
    call ScalarUdf::invoke_with_args
    caused by
    External error: wasm trap: call stack exhausted
    ",
    );
}

/// Get evil UDF.
async fn udf(name: &'static str) -> WasmScalarUdf {
    try_scalar_udfs("runtime")
        .await
        .unwrap()
        .into_iter()
        .find(|udf| udf.name() == name)
        .unwrap()
}

async fn try_call_no_params(udf: &WasmScalarUdf) -> Result<(), DataFusionError> {
    udf.invoke_async_with_args(ScalarFunctionArgs {
        args: vec![],
        arg_fields: vec![],
        number_rows: 1,
        return_field: Arc::new(Field::new("r", DataType::Null, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
    .await
    .map(|_| ())
}

async fn err_call_no_params(udf: &WasmScalarUdf) -> DataFusionError {
    try_call_no_params(udf).await.unwrap_err()
}

/// Normalize line & column numbers in panic message, so that changing the code in the respective file does not change
/// the expected outcome. This makes it easier to add new test cases or update the code without needing to update all
/// results.
fn normalize_panic_location(e: impl ToString) -> String {
    let e = e.to_string();

    static REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"(?<m>panicked at) [^:]+:[0-9]+:[0-9]+:"#).unwrap());

    REGEX
        .replace_all(&e, r#"$m <FILE>:<LINE>:<ROW>:"#)
        .to_string()
}
