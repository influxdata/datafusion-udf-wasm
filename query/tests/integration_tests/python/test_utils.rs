use datafusion_udf_wasm_host::{CompilationFlags, WasmComponentPrecompiled};
use tokio::sync::OnceCell;

/// Static precompiled Python WASM component for tests
static COMPONENT: OnceCell<WasmComponentPrecompiled> = OnceCell::const_new();

/// Returns a static reference to the precompiled Python WASM component.
pub(crate) async fn python_component() -> &'static WasmComponentPrecompiled {
    COMPONENT
        .get_or_init(async || {
            WasmComponentPrecompiled::compile(
                datafusion_udf_wasm_bundle::BIN_PYTHON.into(),
                &CompilationFlags::default(),
            )
            .await
            .unwrap()
        })
        .await
}
