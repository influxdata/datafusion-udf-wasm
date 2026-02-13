//! Interfaces for HTTP interactions of the guest.

use std::{borrow::Cow, collections::HashSet, fmt, sync::Arc};

use http::HeaderName;
pub use http::Method as HttpMethod;
use wasmtime_wasi::ResourceTable;
use wasmtime_wasi_http::{
    HttpResult, WasiHttpCtx, WasiHttpView,
    bindings::http::types::ErrorCode as HttpErrorCode,
    body::HyperOutgoingBody,
    types::{
        DEFAULT_FORBIDDEN_HEADERS, HostFutureIncomingResponse, OutgoingRequestConfig,
        default_send_request_handler,
    },
};

use crate::state::WasmStateImpl;

/// Validates if an outgoing HTTP interaction is allowed.
///
/// You can implement your own business logic here or use one of the pre-built implementations, e.g.
/// [`RejectAllHttpRequests`] or [`AllowCertainHttpRequests`].
pub trait HttpRequestValidator: fmt::Debug + Send + Sync + 'static {
    /// Validate incoming request.
    ///
    /// Return [`Ok`] if the request should be allowed, return [`Err`] otherwise.
    fn validate(
        &self,
        request: &hyper::Request<HyperOutgoingBody>,
        use_tls: bool,
    ) -> Result<(), HttpRequestRejected>;
}

/// Reject ALL requests.
#[derive(Debug, Clone, Copy, Default)]
pub struct RejectAllHttpRequests;

impl HttpRequestValidator for RejectAllHttpRequests {
    fn validate(
        &self,
        _request: &hyper::Request<HyperOutgoingBody>,
        _use_tls: bool,
    ) -> Result<(), HttpRequestRejected> {
        Err(HttpRequestRejected)
    }
}

/// A request matcher.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct HttpRequestMatcher {
    /// Method.
    pub method: HttpMethod,

    /// Host.
    ///
    /// Requests without a host will be rejected.
    pub host: Cow<'static, str>,

    /// Port.
    ///
    /// For requests without an explicit port, this defaults to `80` for non-TLS requests and to `443` for TLS requests.
    pub port: u16,
}

/// Allow-list requests.
#[derive(Debug, Clone)]
pub struct AllowCertainHttpRequests {
    /// Set of all matchers.
    ///
    /// If ANY of them matches, the request will be allowed.
    matchers: HashSet<HttpRequestMatcher>,

    /// Set of all allowed paths.
    ///
    /// The request path must start with one of these paths to be allowed.
    allowed_paths: AllowHttpRequestPath,
}

impl Default for AllowCertainHttpRequests {
    fn default() -> Self {
        Self::new(vec!["/".to_string()])
    }
}

impl AllowCertainHttpRequests {
    /// Create new, empty request matcher.
    pub fn new(allowed_paths: Vec<String>) -> Self {
        Self {
            matchers: HashSet::new(),
            allowed_paths: AllowHttpRequestPath::new(allowed_paths),
        }
    }

    /// Allow given request.
    pub fn allow(&mut self, matcher: HttpRequestMatcher) {
        self.matchers.insert(matcher);
    }
}

impl HttpRequestValidator for AllowCertainHttpRequests {
    fn validate(
        &self,
        request: &hyper::Request<HyperOutgoingBody>,
        use_tls: bool,
    ) -> Result<(), HttpRequestRejected> {
        if !self.allowed_paths.is_allowed(request.uri().path()) {
            return Err(HttpRequestRejected);
        }

        let matcher = HttpRequestMatcher {
            method: request.method().clone(),
            host: request
                .uri()
                .host()
                .ok_or(HttpRequestRejected)?
                .to_owned()
                .into(),
            port: request
                .uri()
                .port_u16()
                .unwrap_or(if use_tls { 443 } else { 80 }),
        };

        if self.matchers.contains(&matcher) {
            Ok(())
        } else {
            Err(HttpRequestRejected)
        }
    }
}

/// Restrict HTTP request paths.
#[derive(Debug, Clone, Default)]
pub(crate) struct AllowHttpRequestPath {
    /// Allowed paths.
    pub allowed_paths: Vec<String>,
}

impl AllowHttpRequestPath {
    /// Create new path allow-list.
    pub(crate) fn new(allowed_paths: Vec<String>) -> Self {
        Self { allowed_paths }
    }

    /// Check if given path is allowed.
    pub(crate) fn is_allowed(&self, path: &str) -> bool {
        self.allowed_paths
            .iter()
            .any(|allowed| path.starts_with(allowed))
    }
}

impl HttpRequestValidator for AllowHttpRequestPath {
    fn validate(
        &self,
        request: &hyper::Request<HyperOutgoingBody>,
        _use_tls: bool,
    ) -> Result<(), HttpRequestRejected> {
        let path = request.uri().path_and_query().map_or("", |pq| pq.as_str());

        if self.is_allowed(path) {
            Ok(())
        } else {
            Err(HttpRequestRejected)
        }
    }
}

/// Reject HTTP request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HttpRequestRejected;

impl fmt::Display for HttpRequestRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("rejected")
    }
}

impl std::error::Error for HttpRequestRejected {}

impl WasiHttpView for WasmStateImpl {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.wasi_http_ctx
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.resource_table
    }

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
                validator
                    .validate(&request, config.use_tls)
                    .map_err(|_| HttpErrorCode::HttpRequestDenied)?;

                log::debug!(
                    "UDF HTTP request: {} {}",
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn reject_all() {
        let policy = RejectAllHttpRequests;

        let request = hyper::Request::builder().body(Default::default()).unwrap();
        policy.validate(&request, false).unwrap_err();
    }

    #[test]
    fn allow_certain() {
        let request_no_port = hyper::Request::builder()
            .method(HttpMethod::GET)
            .uri("http://foo.bar")
            .body(Default::default())
            .unwrap();

        let request_with_port = hyper::Request::builder()
            .method(HttpMethod::GET)
            .uri("http://my.universe:1337")
            .body(Default::default())
            .unwrap();

        struct Case {
            matchers: Vec<HttpRequestMatcher>,
            result_no_port_no_tls: Result<(), HttpRequestRejected>,
            result_no_port_with_tls: Result<(), HttpRequestRejected>,
            result_with_port_no_tls: Result<(), HttpRequestRejected>,
            result_with_port_with_tls: Result<(), HttpRequestRejected>,
        }

        let cases = [
            Case {
                matchers: vec![],
                result_no_port_no_tls: Err(HttpRequestRejected),
                result_no_port_with_tls: Err(HttpRequestRejected),
                result_with_port_no_tls: Err(HttpRequestRejected),
                result_with_port_with_tls: Err(HttpRequestRejected),
            },
            Case {
                matchers: vec![HttpRequestMatcher {
                    method: HttpMethod::GET,
                    host: "foo.bar".into(),
                    port: 80,
                }],
                result_no_port_no_tls: Ok(()),
                result_no_port_with_tls: Err(HttpRequestRejected),
                result_with_port_no_tls: Err(HttpRequestRejected),
                result_with_port_with_tls: Err(HttpRequestRejected),
            },
            Case {
                matchers: vec![HttpRequestMatcher {
                    method: HttpMethod::GET,
                    host: "foo.bar".into(),
                    port: 443,
                }],
                result_no_port_no_tls: Err(HttpRequestRejected),
                result_no_port_with_tls: Ok(()),
                result_with_port_no_tls: Err(HttpRequestRejected),
                result_with_port_with_tls: Err(HttpRequestRejected),
            },
            Case {
                matchers: vec![HttpRequestMatcher {
                    method: HttpMethod::POST,
                    host: "foo.bar".into(),
                    port: 80,
                }],
                result_no_port_no_tls: Err(HttpRequestRejected),
                result_no_port_with_tls: Err(HttpRequestRejected),
                result_with_port_no_tls: Err(HttpRequestRejected),
                result_with_port_with_tls: Err(HttpRequestRejected),
            },
            Case {
                matchers: vec![HttpRequestMatcher {
                    method: HttpMethod::GET,
                    host: "my.universe".into(),
                    port: 80,
                }],
                result_no_port_no_tls: Err(HttpRequestRejected),
                result_no_port_with_tls: Err(HttpRequestRejected),
                result_with_port_no_tls: Err(HttpRequestRejected),
                result_with_port_with_tls: Err(HttpRequestRejected),
            },
            Case {
                matchers: vec![HttpRequestMatcher {
                    method: HttpMethod::GET,
                    host: "my.universe".into(),
                    port: 1337,
                }],
                result_no_port_no_tls: Err(HttpRequestRejected),
                result_no_port_with_tls: Err(HttpRequestRejected),
                result_with_port_no_tls: Ok(()),
                result_with_port_with_tls: Ok(()),
            },
            Case {
                matchers: vec![
                    HttpRequestMatcher {
                        method: HttpMethod::GET,
                        host: "foo.bar".into(),
                        port: 80,
                    },
                    HttpRequestMatcher {
                        method: HttpMethod::POST,
                        host: "foo.bar".into(),
                        port: 80,
                    },
                    HttpRequestMatcher {
                        method: HttpMethod::GET,
                        host: "my.universe".into(),
                        port: 1337,
                    },
                ],
                result_no_port_no_tls: Ok(()),
                result_no_port_with_tls: Err(HttpRequestRejected),
                result_with_port_no_tls: Ok(()),
                result_with_port_with_tls: Ok(()),
            },
        ];

        for (i, case) in cases.into_iter().enumerate() {
            println!("case: {}", i + 1);

            let Case {
                matchers,
                result_no_port_no_tls,
                result_no_port_with_tls,
                result_with_port_no_tls,
                result_with_port_with_tls,
            } = case;

            let mut policy = AllowCertainHttpRequests::default();

            for matcher in matchers {
                policy.allow(matcher);
            }

            assert_eq!(
                policy.validate(&request_no_port, false),
                result_no_port_no_tls,
            );
            assert_eq!(
                policy.validate(&request_no_port, true),
                result_no_port_with_tls,
            );
            assert_eq!(
                policy.validate(&request_with_port, false),
                result_with_port_no_tls,
            );
            assert_eq!(
                policy.validate(&request_with_port, true),
                result_with_port_with_tls,
            );
        }
    }

    #[test]
    fn restrict_paths() {
        let policy =
            AllowHttpRequestPath::new(vec!["/allowed".to_string(), "/also/allowed".to_string()]);

        struct Case {
            path: &'static str,
            result: Result<(), HttpRequestRejected>,
        }

        let cases = [
            Case {
                path: "/allowed",
                result: Ok(()),
            },
            Case {
                path: "/also/allowed",
                result: Ok(()),
            },
            Case {
                path: "/not/allowed",
                result: Err(HttpRequestRejected),
            },
        ];

        for case in cases {
            let request = hyper::Request::builder()
                .uri(case.path)
                .body(Default::default())
                .unwrap();
            let result = policy.validate(&request, false);
            assert_eq!(result, case.result);
        }
    }
}
