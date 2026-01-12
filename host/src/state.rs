//! State handling of guests.

use std::sync::Arc;

use tokio::runtime::Handle;
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxView, WasiView, p2::pipe::MemoryOutputPipe};
use wasmtime_wasi_http::WasiHttpCtx;

use crate::{HttpRequestValidator, ignore_debug::IgnoreDebug, limiter::Limiter, vfs::VfsState};

/// State of the WASM payload.
#[derive(Debug)]
pub(crate) struct WasmStateImpl {
    /// Virtual filesystem for the WASM payload.
    ///
    /// This filesystem is provided to the payload in memory with read-write support.
    pub(crate) vfs_state: VfsState,

    /// Resource limiter.
    pub(crate) limiter: Limiter,

    /// A limited buffer for stderr.
    ///
    /// This is especially useful for when the payload crashes.
    pub(crate) stderr: MemoryOutputPipe,

    /// WASI context.
    pub(crate) wasi_ctx: IgnoreDebug<WasiCtx>,

    /// WASI HTTP context.
    pub(crate) wasi_http_ctx: WasiHttpCtx,

    /// Resource tables.
    pub(crate) resource_table: ResourceTable,

    /// HTTP request validator.
    pub(crate) http_validator: Arc<dyn HttpRequestValidator>,

    /// Handle to tokio I/O runtime.
    pub(crate) io_rt: Handle,
}

impl WasiView for WasmStateImpl {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.resource_table,
        }
    }
}
