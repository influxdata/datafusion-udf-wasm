//! Helpers to simplify work with [`tokio`].

/// Run an async method in a sync context.
///
/// **This is a hack that is required because the respective DataFusion interfaces aren't fully async.**
///
/// TODO: remove this! See <https://github.com/influxdata/datafusion-udf-wasm/issues/31>.
pub(crate) fn async_in_sync_context<Fut>(fut: Fut) -> Fut::Output
where
    Fut: Future,
{
    tokio::task::block_in_place(move || tokio::runtime::Handle::current().block_on(fut))
}

/// Spawn blocking I/O.
pub(crate) async fn blocking_io<F, T>(f: F) -> std::io::Result<T>
where
    F: FnOnce() -> std::io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await?
}
