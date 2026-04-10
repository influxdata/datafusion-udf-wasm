use std::sync::{Arc, LazyLock};

use datafusion_execution::memory_pool::GreedyMemoryPool;
use datafusion_udf_wasm_host::{
    CompilationFlags, WasmComponentPrecompiled, WasmPermissions, WasmScalarUdf,
};
use regex::Regex;
use tokio::{runtime::Runtime, sync::OnceCell};

use crate::integration_tests::test_utils::FullError;

/// Static memory limit.
///
/// 10MB.
pub(crate) const MEMORY_LIMIT: usize = 10 * 1024 * 1024;

/// Static precompiled WASM component for tests
static COMPONENT: OnceCell<WasmComponentPrecompiled> = OnceCell::const_new();

/// Returns a static reference to the precompiled WASM component.
pub(crate) async fn component() -> &'static WasmComponentPrecompiled {
    COMPONENT
        .get_or_init(async || {
            WasmComponentPrecompiled::compile(
                datafusion_udf_wasm_bundle::BIN_EVIL.into(),
                &CompilationFlags::default(),
            )
            .await
            .unwrap()
        })
        .await
}

/// I/O runtime used for all tests.
pub(crate) static IO_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
});

/// Try to get scalar UDFs.
pub(crate) async fn try_scalar_udfs(evil: &'static str) -> Result<Vec<WasmScalarUdf>, FullError> {
    try_scalar_udfs_with_env(evil, &[]).await
}

/// Try to get scalar UDFs with environment variables.
pub(crate) async fn try_scalar_udfs_with_env(
    evil: &'static str,
    vars: &[(&str, &str)],
) -> Result<Vec<WasmScalarUdf>, FullError> {
    let mut permissions = WasmPermissions::new();
    for (k, v) in vars {
        permissions = permissions.with_env((*k).to_owned(), (*v).to_owned());
    }
    try_scalar_udfs_with_permissions(evil, permissions).await
}

/// Try to get scalar UDFs with permissions.
pub(crate) async fn try_scalar_udfs_with_permissions(
    evil: &'static str,
    permissions: WasmPermissions,
) -> Result<Vec<WasmScalarUdf>, FullError> {
    let component = component().await;

    let permissions = permissions.with_env("EVIL".to_owned(), evil.to_owned());

    WasmScalarUdf::new(
        component,
        &permissions,
        IO_RUNTIME.handle().clone(),
        &(Arc::new(GreedyMemoryPool::new(MEMORY_LIMIT)) as _),
        "".to_owned(),
    )
    .await
    .map_err(FullError::new)
}

/// Normalize line & column numbers in panic message, so that changing the code in the respective file does not change
/// the expected outcome. This makes it easier to add new test cases or update the code without needing to update all
/// results.
pub(crate) fn normalize_panic_location(e: impl ToString) -> String {
    let e = e.to_string();

    static REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"(?<m>panicked at) [^:]+:[0-9]+:[0-9]+:"#).unwrap());
    static THREAD_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"thread '<unnamed>' \([0-9]+\)"#).unwrap());

    let e = REGEX
        .replace_all(&e, r#"$m <FILE>:<LINE>:<ROW>:"#)
        .to_string();
    THREAD_REGEX
        .replace_all(&e, "thread '<unnamed>' (<THREAD>)")
        .to_string()
}
