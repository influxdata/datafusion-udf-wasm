use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_common::{ScalarValue, config::ConfigOptions};
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

/// Get evil UDF.
async fn udf(name: &'static str) -> WasmScalarUdf {
    try_scalar_udfs("env")
        .await
        .unwrap()
        .into_iter()
        .find(|udf| udf.name() == name)
        .unwrap()
}

async fn call(udf: &WasmScalarUdf) -> Option<String> {
    let scalar = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_scalar();
    if let ScalarValue::Utf8(s) = scalar {
        s
    } else {
        unreachable!("invalid scalar type: {scalar:?}")
    }
}
