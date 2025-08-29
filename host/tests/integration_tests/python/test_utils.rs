use datafusion_common::DataFusionError;
use datafusion_udf_wasm_host::{WasmComponentPrecompiled, WasmScalarUdf};
use tokio::sync::OnceCell;

static COMPONENT: OnceCell<WasmComponentPrecompiled> = OnceCell::const_new();

async fn python_component() -> &'static WasmComponentPrecompiled {
    COMPONENT
        .get_or_init(async || {
            let wasm_binary = tokio::fs::read(format!(
                "{}/../target/wasm32-wasip2/debug/datafusion_udf_wasm_python.wasm",
                env!("CARGO_MANIFEST_DIR")
            ))
            .await
            .unwrap();

            WasmComponentPrecompiled::new(wasm_binary.into())
                .await
                .unwrap()
        })
        .await
}

pub(crate) async fn python_scalar_udfs(code: &str) -> Result<Vec<WasmScalarUdf>, DataFusionError> {
    let component = python_component().await;

    WasmScalarUdf::new(component, code.to_owned()).await
}

pub(crate) async fn python_scalar_udf(code: &str) -> Result<WasmScalarUdf, DataFusionError> {
    let udfs = python_scalar_udfs(code).await?;
    assert_eq!(udfs.len(), 1);
    Ok(udfs.into_iter().next().expect("just checked len"))
}
