//! Interfaces for HTTP interactions of the guest.

use std::sync::Arc;

use http::HeaderName;
use tokio::runtime::Handle;
use wasmtime_wasi_http::{
    DEFAULT_FORBIDDEN_HEADERS,
    p2::{
        HttpResult, WasiHttpCtxView, WasiHttpHooks, WasiHttpView,
        bindings::http::types::ErrorCode as HttpErrorCode,
        body::HyperOutgoingBody,
        default_send_request_handler,
        types::{HostFutureIncomingResponse, OutgoingRequestConfig},
    },
};

pub use types::{HttpConnectionMode, HttpMethod, HttpPort};
pub use validator::{
    AllowCertainHttpRequests, AllowHttpEndpoint, AllowHttpHost, HttpRequestRejected,
    HttpRequestValidator, RejectAllHttpRequests,
};

use crate::state::WasmStateImpl;

mod types;
mod validator;

impl WasiHttpView for WasmStateImpl {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.wasi_http_ctx,
            table: &mut self.resource_table,
            hooks: &mut self.wasi_http_hooks,
        }
    }
}

/// Implements [`WasiHttpHooks`].
#[derive(Debug)]
pub(crate) struct WasiHttpHooksImpl {
    /// HTTP request validator.
    pub(crate) http_validator: Arc<dyn HttpRequestValidator>,

    /// Handle to tokio I/O runtime.
    pub(crate) io_rt: Handle,
}

impl WasiHttpHooks for WasiHttpHooksImpl {
    fn send_request(
        &mut self,
        mut request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        let _guard = self.io_rt.enter();

        // Python `requests` sends this so we allow it but later drop it from the actual request.
        request.headers_mut().remove(hyper::header::CONNECTION);

        // technically we could return an error straight away, but `urllib3` doesn't handle that super well, so we
        // create a future and validate the error in there (before actually starting the request of course)

        let validator = Arc::clone(&self.http_validator);
        let handle = wasmtime_wasi::runtime::spawn(async move {
            // yes, that's another layer of futures. The WASI interface is somewhat nested.
            let fut = async {
                let mode = HttpConnectionMode::from_use_tls(config.use_tls);
                validator
                    .validate(&request, mode)
                    .map_err(|_| HttpErrorCode::HttpRequestDenied)?;

                log::debug!(
                    "UDF HTTP request: {} {} ({mode:?})",
                    request.method().as_str(),
                    request.uri(),
                );
                default_send_request_handler(request, config).await
            };

            Ok(fut.await)
        });

        Ok(HostFutureIncomingResponse::pending(handle))
    }

    fn is_forbidden_header(&mut self, name: &HeaderName) -> bool {
        // Python `requests` sends this so we allow it but later drop it from the actual request.
        if name == hyper::header::CONNECTION {
            return false;
        }

        DEFAULT_FORBIDDEN_HEADERS.contains(name)
    }
}
