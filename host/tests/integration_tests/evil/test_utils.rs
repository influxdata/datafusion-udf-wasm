use datafusion_common::DataFusionError;
use datafusion_udf_wasm_host::{WasmComponentPrecompiled, WasmPermissions, WasmScalarUdf};
use tokio::{runtime::Handle, sync::OnceCell};

/// Static precompiled WASM component for tests
static COMPONENT: OnceCell<WasmComponentPrecompiled> = OnceCell::const_new();

/// Returns a static reference to the precompiled WASM component.
async fn component() -> &'static WasmComponentPrecompiled {
    COMPONENT
        .get_or_init(async || {
            WasmComponentPrecompiled::new(datafusion_udf_wasm_bundle::BIN_EVIL.into())
                .await
                .unwrap()
        })
        .await
}

/// Try to get scalar UDFs.
pub(crate) async fn try_scalar_udfs(
    evil: &'static str,
) -> Result<Vec<WasmScalarUdf>, DataFusionError> {
    try_scalar_udfs_with_env(evil, &[]).await
}

/// Try to get scalar UDFs with environment variables.
pub(crate) async fn try_scalar_udfs_with_env(
    evil: &'static str,
    vars: &[(&str, &str)],
) -> Result<Vec<WasmScalarUdf>, DataFusionError> {
    let component = component().await;

    let mut permissions = WasmPermissions::new().with_env("EVIL".to_owned(), evil.to_owned());
    for (k, v) in vars {
        permissions = permissions.with_env((*k).to_owned(), (*v).to_owned());
    }

    WasmScalarUdf::new(component, &permissions, Handle::current(), "".to_owned()).await
}
