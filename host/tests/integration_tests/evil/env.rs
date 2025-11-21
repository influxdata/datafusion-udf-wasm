use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_common::{DataFusionError, ScalarValue, config::ConfigOptions};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, async_udf::AsyncScalarUDFImpl};
use datafusion_udf_wasm_host::WasmScalarUdf;

use crate::integration_tests::{evil::test_utils::try_scalar_udfs, test_utils::ColumnarValueExt};

#[tokio::test]
async fn test_args() {
    let udf = udf("args").await;
    assert_eq!(call(&udf).await, None);
}

#[tokio::test]
async fn test_current_dir() {
    let udf = udf("current_dir").await;
    assert_eq!(call(&udf).await, Some("/".to_owned()));
}

#[tokio::test]
async fn test_current_exe() {
    let udf = udf("current_exe").await;
    insta::assert_snapshot!(
        call(&udf).await.unwrap(),
        @"operation not supported on this platform",
    );
}

#[tokio::test]
async fn test_env() {
    let udf = udf("env").await;
    assert_eq!(call(&udf).await, Some("EVIL:env".to_owned()));
}

#[tokio::test]
async fn test_process_id() {
    let udf = udf("process_id").await;
    let err = try_call(&udf).await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    call ScalarUdf::invoke_with_args

    stderr:

    thread '<unnamed>' (1) panicked at library/std/src/sys/pal/wasip2/../wasip1/os.rs:131:5:
    unsupported
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
    ",
    );
}

#[tokio::test]
async fn test_stdin() {
    let udf = udf("stdin").await;
    assert_eq!(call(&udf).await, Some("".to_owned()));
}

#[tokio::test]
async fn test_thread_id() {
    let udf = udf("thread_id").await;
    assert_eq!(call(&udf).await, Some("ThreadId(1)".to_owned()));
}

/// Get evil UDF.
async fn udf(name: &'static str) -> WasmScalarUdf {
    try_scalar_udfs("env")
        .await
        .unwrap()
        .into_iter()
        .find(|udf| udf.name() == name)
        .unwrap()
}

async fn try_call(udf: &WasmScalarUdf) -> Result<Option<String>, DataFusionError> {
    let scalar = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await?
        .unwrap_scalar();
    if let ScalarValue::Utf8(s) = scalar {
        Ok(s)
    } else {
        unreachable!("invalid scalar type: {scalar:?}")
    }
}

async fn call(udf: &WasmScalarUdf) -> Option<String> {
    try_call(udf).await.unwrap()
}
