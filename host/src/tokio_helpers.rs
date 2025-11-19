//! Helpers to simplify work with [`tokio`].

use std::time::Duration;

use datafusion_common::DataFusionError;
use tokio::runtime::RuntimeFlavor;

/// Run an async method in a sync context.
///
/// **This is a hack that is required because the respective DataFusion interfaces aren't fully async.**
///
/// TODO: remove this! See <https://github.com/influxdata/datafusion-udf-wasm/issues/169>.
pub(crate) fn async_in_sync_context<Fut, T>(fut: Fut, timeout: Duration) -> Fut::Output
where
    Fut: Future<Output = Result<T, DataFusionError>>,
{
    let handle = tokio::runtime::Handle::try_current().map_err(|e| {
        DataFusionError::External(Box::new(e)).context("get tokio runtime for in-place blocking")
    })?;

    let flavor = handle.runtime_flavor();
    if !matches!(flavor, RuntimeFlavor::MultiThread) {
        return Err(DataFusionError::NotImplemented(format!(
            "in-place blocking only works for tokio multi-thread runtimes, not for {flavor:?}"
        )));
    }

    let fut = async move {
        tokio::time::timeout(timeout, fut)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    };

    tokio::task::block_in_place(move || handle.block_on(fut)).flatten()
}
