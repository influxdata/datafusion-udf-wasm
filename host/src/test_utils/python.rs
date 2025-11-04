use crate::{WasmComponentPrecompiled, WasmScalarUdf};
use datafusion_common::DataFusionError;
use tokio::sync::OnceCell;

/// Static precompiled Python WASM component for tests
static COMPONENT: OnceCell<WasmComponentPrecompiled> = OnceCell::const_new();

/// Returns a static reference to the precompiled Python WASM component.
pub async fn python_component() -> &'static WasmComponentPrecompiled {
    COMPONENT
        .get_or_init(async || {
            WasmComponentPrecompiled::new(datafusion_udf_wasm_bundle::BIN_PYTHON.into())
                .await
                .unwrap()
        })
        .await
}

/// Compiles the provided Python UDF code into a list of WasmScalarUdf instances.
pub async fn python_scalar_udfs(code: &str) -> Result<Vec<WasmScalarUdf>, DataFusionError> {
    let component = python_component().await;

    WasmScalarUdf::new(component, &Default::default(), code.to_owned()).await
}

/// Compiles the provided Python UDF code into a single WasmScalarUdf instance.
pub async fn python_scalar_udf(code: &str) -> Result<WasmScalarUdf, DataFusionError> {
    let udfs = python_scalar_udfs(code).await?;
    assert_eq!(udfs.len(), 1);
    Ok(udfs.into_iter().next().expect("just checked len"))
}
