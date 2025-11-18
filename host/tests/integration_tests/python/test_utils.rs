use std::sync::Arc;

use datafusion_common::DataFusionError;
use datafusion_execution::memory_pool::GreedyMemoryPool;
use datafusion_udf_wasm_host::{WasmComponentPrecompiled, WasmScalarUdf};
use tokio::{runtime::Handle, sync::OnceCell};

/// Memory limit in bytes.
///
/// 500MB.
const MEMORY_LIMIT: usize = 100_000_000;

/// Static precompiled Python WASM component for tests
static COMPONENT: OnceCell<WasmComponentPrecompiled> = OnceCell::const_new();

/// Returns a static reference to the precompiled Python WASM component.
pub(crate) async fn python_component() -> &'static WasmComponentPrecompiled {
    COMPONENT
        .get_or_init(async || {
            WasmComponentPrecompiled::new(datafusion_udf_wasm_bundle::BIN_PYTHON.into())
                .await
                .unwrap()
        })
        .await
}

/// Compiles the provided Python UDF code into a list of WasmScalarUdf instances.
pub(crate) async fn python_scalar_udfs(code: &str) -> Result<Vec<WasmScalarUdf>, DataFusionError> {
    let component = python_component().await;

    WasmScalarUdf::new(
        component,
        &Default::default(),
        Handle::current(),
        &(Arc::new(GreedyMemoryPool::new(MEMORY_LIMIT)) as _),
        code.to_owned(),
    )
    .await
}

/// Compiles the provided Python UDF code into a single WasmScalarUdf instance.
pub(crate) async fn python_scalar_udf(code: &str) -> Result<WasmScalarUdf, DataFusionError> {
    let udfs = python_scalar_udfs(code).await?;
    assert_eq!(udfs.len(), 1);
    Ok(udfs.into_iter().next().expect("just checked len"))
}
