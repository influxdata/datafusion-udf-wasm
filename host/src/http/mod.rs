//! Interfaces for HTTP interactions of the guest.

use std::{io::ErrorKind, sync::Arc};

use datafusion_common::{DataFusionError, error::Result as DataFusionResult};
use http::HeaderName;
use http_body_util::BodyExt;
use hyper::body::Frame;
use tokio::runtime::Handle;
use wasmtime_wasi_http::{
    DEFAULT_FORBIDDEN_HEADERS,
    p2::{
        HttpResult, WasiHttpCtxView, WasiHttpHooks, WasiHttpView,
        bindings::http::types::ErrorCode as HttpErrorCode,
        body::{HyperIncomingBody, HyperOutgoingBody},
        types::{HostFutureIncomingResponse, IncomingResponse, OutgoingRequestConfig},
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
    http_validator: Arc<dyn HttpRequestValidator>,

    /// Handle to tokio I/O runtime.
    io_rt: Handle,

    /// HTTP client.
    ///
    /// This may cache connections and TLS state.
    client: reqwest::Client,
}

impl WasiHttpHooksImpl {
    /// Set up data structures.
    pub(crate) fn new(
        http_validator: Arc<dyn HttpRequestValidator>,
        io_rt: Handle,
    ) -> DataFusionResult<Self> {
        // https://github.com/seanmonstar/reqwest/issues/2924
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            let _ = rustls::crypto::ring::default_provider().install_default();
        }

        let client = reqwest::Client::builder()
            // disable response body compression (the guest shall do that)
            .no_brotli()
            .no_deflate()
            .no_gzip()
            .no_zstd()
            // disable redirect handling (the guest shall do that)
            .redirect(reqwest::redirect::Policy::none())
            // disable proxy
            // TODO: allow overrides
            .no_proxy()
            // set up DNS
            // TODO: allow overrides
            .no_hickory_dns()
            // TLS setup
            // TODO: allow admin to set CA etc.
            .tls_backend_rustls()
            // done
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(Self {
            http_validator,
            io_rt,
            client,
        })
    }
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
        let client = self.client.clone();
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

                send_request(&client, request, config).await
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

/// Send HTTP request.
async fn send_request(
    client: &reqwest::Client,
    request: hyper::Request<HyperOutgoingBody>,
    config: OutgoingRequestConfig,
) -> Result<IncomingResponse, HttpErrorCode> {
    let OutgoingRequestConfig {
        use_tls,
        connect_timeout,
        first_byte_timeout,
        between_bytes_timeout,
    } = config;

    // "connections" are a rather low-level concept and technically opaque to the guest. We are free to cache
    // connections and TLS state. Hence we just use it to cap the "first byte timeout" and don't really apply it to
    // connections.
    let first_byte_timeout = first_byte_timeout.min(connect_timeout);

    let resp = tokio::time::timeout(
        first_byte_timeout,
        assemble_request(client, request, use_tls)?.send(),
    )
    .await
    .map_err(|_| HttpErrorCode::ConnectionReadTimeout)?
    .map_err(map_reqwest_err)?;

    Ok(IncomingResponse {
        resp: assemble_response(resp)?,
        worker: None,
        between_bytes_timeout,
    })
}

/// Build outgoing request object.
fn assemble_request(
    client: &reqwest::Client,
    request: hyper::Request<HyperOutgoingBody>,
    use_tls: bool,
) -> Result<reqwest::RequestBuilder, HttpErrorCode> {
    let (parts, body) = request.into_parts();
    let http::request::Parts {
        method,
        uri,
        version,
        headers,
        extensions: _,
        ..
    } = parts;

    let mut uri_parts = uri.into_parts();
    uri_parts.scheme = Some(if use_tls {
        http::uri::Scheme::HTTPS
    } else {
        http::uri::Scheme::HTTP
    });
    let uri = http::Uri::from_parts(uri_parts)
        .map_err(|e| HttpErrorCode::InternalError(Some(e.to_string())))?;

    Ok(client
        .request(method, uri.to_string())
        .version(version)
        .headers(headers)
        .body(reqwest::Body::wrap_stream(body.into_data_stream())))
}

/// Build incoming response object.
fn assemble_response(
    resp: reqwest::Response,
) -> Result<hyper::Response<HyperIncomingBody>, HttpErrorCode> {
    let mut builder = hyper::Response::builder()
        .status(resp.status())
        .version(resp.version());

    *builder.headers_mut().ok_or_else(|| {
        HttpErrorCode::InternalError(Some("cannot assemble response".to_owned()))
    })? = resp.headers().clone();

    builder
        .body(
            http_body_util::StreamBody::new(futures_util::stream::try_unfold(
                resp,
                async |mut resp| {
                    let maybe_chunk = resp.chunk().await.map_err(map_reqwest_err)?;
                    Ok(maybe_chunk.map(|chunk| (Frame::data(chunk), resp)))
                },
            ))
            .boxed_unsync(),
        )
        .map_err(|e| HttpErrorCode::InternalError(Some(e.to_string())))
}

/// Map [`reqwest::Error`] to [`HttpErrorCode`].
fn map_reqwest_err(e: reqwest::Error) -> HttpErrorCode {
    // try to find an IO error first, since this is potentially the most low-level information
    if let Some(e) = extract_error_type::<std::io::Error>(&e) {
        match e.kind() {
            ErrorKind::ConnectionRefused => {
                return HttpErrorCode::ConnectionRefused;
            }
            ErrorKind::ConnectionReset => {
                return HttpErrorCode::ConnectionTerminated;
            }
            ErrorKind::TimedOut => {
                return HttpErrorCode::ConnectionTimeout;
            }
            _ => {}
        }
    }

    // hyper might have some hints for us
    if let Some(e) = extract_error_type::<hyper::Error>(&e) {
        if e.is_incomplete_message() {
            return HttpErrorCode::HttpResponseIncomplete;
        } else if e.is_parse() {
            return HttpErrorCode::HttpProtocolError;
        } else if e.is_timeout() {
            return HttpErrorCode::ConnectionTimeout;
        }
    }

    // cannot really extract anything meaningful, fall back to "internal error" ("internal" as in "in our stack", not
    // as in "internal server error")
    HttpErrorCode::InternalError(Some(e.to_string()))
}

/// Extract concrete error type from error chain.
fn extract_error_type<'a, E>(e: &'a (dyn std::error::Error + 'static)) -> Option<&'a E>
where
    E: std::error::Error + 'static,
{
    let mut current = e;

    loop {
        if let Some(concrete) = current.downcast_ref::<E>() {
            return Some(concrete);
        }

        match current.source() {
            Some(next) => {
                current = next;
            }
            None => {
                return None;
            }
        }
    }
}
