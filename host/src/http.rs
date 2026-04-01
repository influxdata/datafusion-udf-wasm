//! Interfaces for HTTP interactions of the guest.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt,
    num::NonZeroU16,
    sync::Arc,
};

use http::HeaderName;
pub use http::Method as HttpMethod;
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

use crate::state::WasmStateImpl;

/// An HTTP port.
///
/// Can be any [`u16`] value except for zero.
#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct HttpPort(NonZeroU16);

impl HttpPort {
    /// Create new port from [`u16`].
    ///
    /// Returns [`None`] if port is zero.
    pub const fn new(p: u16) -> Option<Self> {
        // NOTE: `Option::map` isn't const-stable
        match NonZeroU16::new(p) {
            Some(p) => Some(Self(p)),
            None => None,
        }
    }

    /// Get [`u16`] representation of that port.
    pub const fn get_u16(&self) -> u16 {
        self.0.get()
    }
}

impl std::fmt::Debug for HttpPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.get().fmt(f)
    }
}

impl std::fmt::Display for HttpPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.get().fmt(f)
    }
}

impl std::str::FromStr for HttpPort {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let p: NonZeroU16 = s.parse()?;
        Ok(Self(p))
    }
}

/// Default port for unencrypted HTTP traffic.
const DEFAULT_PORT_PLAINTEXT_HTTP: HttpPort = HttpPort::new(80).expect("valid port");

/// Default port for encrypted HTTPs traffic.
const DEFAULT_PORT_ENCRYPTED_HTTP: HttpPort = HttpPort::new(443).expect("valid port");

/// Get default port if no port was provided by the request.
fn default_port(use_tls: bool) -> HttpPort {
    if use_tls {
        DEFAULT_PORT_ENCRYPTED_HTTP
    } else {
        DEFAULT_PORT_PLAINTEXT_HTTP
    }
}

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

/// Allow settings for a given endpoint.
///
/// An endpoint is defined by a host + port.
#[derive(Debug, Clone, Default)]
pub struct AllowHttpEndpoint {
    /// Allowed methods.
    methods: HashSet<HttpMethod>,
}

impl AllowHttpEndpoint {
    /// Allow given HTTP method.
    pub fn allow_method(&mut self, method: HttpMethod) {
        self.methods.insert(method);
    }
}

/// Allow settings for a host.
#[derive(Debug, Clone, Default)]
pub struct AllowHttpHost {
    /// Mapping from port to endpoint.
    endpoints: HashMap<HttpPort, AllowHttpEndpoint>,
}

impl AllowHttpHost {
    /// Allow given port at this host.
    pub fn allow_port(&mut self, port: HttpPort) -> &mut AllowHttpEndpoint {
        self.endpoints.entry(port).or_default()
    }
}

/// Allow-list requests.
#[derive(Debug, Clone, Default)]
pub struct AllowCertainHttpRequests {
    /// Set of allowed hosts.
    hosts: HashMap<Cow<'static, str>, AllowHttpHost>,
}

impl AllowCertainHttpRequests {
    /// Create new, empty request matcher.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow given host.
    pub fn allow_host(&mut self, host: impl Into<Cow<'static, str>>) -> &mut AllowHttpHost {
        self.hosts.entry(host.into()).or_default()
    }
}

impl HttpRequestValidator for AllowCertainHttpRequests {
    fn validate(
        &self,
        request: &hyper::Request<HyperOutgoingBody>,
        use_tls: bool,
    ) -> Result<(), HttpRequestRejected> {
        let host = self
            .hosts
            .get(request.uri().host().ok_or(HttpRequestRejected)?)
            .ok_or(HttpRequestRejected)?;

        let endpoint = host
            .endpoints
            .get(
                &request
                    .uri()
                    .port_u16()
                    .map(|p| HttpPort::new(p).ok_or(HttpRequestRejected))
                    .transpose()?
                    .unwrap_or_else(|| default_port(use_tls)),
            )
            .ok_or(HttpRequestRejected)?;

        if endpoint.methods.contains(request.method()) {
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
        const HOST_1: &str = "foo.bar";
        const HOST_2: &str = "my.universe";
        const SPECIFIC_PORT: HttpPort = HttpPort::new(1337).expect("valid port");

        let request_no_port = hyper::Request::builder()
            .method(HttpMethod::GET)
            .uri(format!("http://{HOST_1}"))
            .body(Default::default())
            .unwrap();

        let request_with_port = hyper::Request::builder()
            .method(HttpMethod::GET)
            .uri(format!("http://{HOST_2}:{SPECIFIC_PORT}"))
            .body(Default::default())
            .unwrap();

        #[derive(Debug, PartialEq, Eq)]
        struct Results {
            no_port_no_tls: Result<(), HttpRequestRejected>,
            no_port_with_tls: Result<(), HttpRequestRejected>,
            with_port_no_tls: Result<(), HttpRequestRejected>,
            with_port_with_tls: Result<(), HttpRequestRejected>,
        }

        #[derive(Debug)]
        struct Case {
            policy: AllowCertainHttpRequests,
            results: Results,
        }

        let cases = [
            Case {
                policy: AllowCertainHttpRequests::default(),
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy.allow_host(HOST_1);
                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy
                        .allow_host(HOST_1)
                        .allow_port(DEFAULT_PORT_PLAINTEXT_HTTP);
                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy
                        .allow_host(HOST_1)
                        .allow_port(DEFAULT_PORT_PLAINTEXT_HTTP)
                        .allow_method(HttpMethod::GET);
                    policy
                },
                results: Results {
                    no_port_no_tls: Ok(()),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy
                        .allow_host(HOST_1)
                        .allow_port(DEFAULT_PORT_ENCRYPTED_HTTP)
                        .allow_method(HttpMethod::GET);
                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Ok(()),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy
                        .allow_host(HOST_1)
                        .allow_port(DEFAULT_PORT_PLAINTEXT_HTTP)
                        .allow_method(HttpMethod::POST);
                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy
                        .allow_host(HOST_2)
                        .allow_port(DEFAULT_PORT_PLAINTEXT_HTTP)
                        .allow_method(HttpMethod::GET);
                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy
                        .allow_host(HOST_2)
                        .allow_port(SPECIFIC_PORT)
                        .allow_method(HttpMethod::GET);
                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Ok(()),
                    with_port_with_tls: Ok(()),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();

                    let endpoint_1 = policy
                        .allow_host(HOST_1)
                        .allow_port(DEFAULT_PORT_PLAINTEXT_HTTP);
                    endpoint_1.allow_method(HttpMethod::GET);
                    endpoint_1.allow_method(HttpMethod::POST);

                    let endpoint_2 = policy.allow_host(HOST_2).allow_port(SPECIFIC_PORT);
                    endpoint_2.allow_method(HttpMethod::GET);

                    policy
                },
                results: Results {
                    no_port_no_tls: Ok(()),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Ok(()),
                    with_port_with_tls: Ok(()),
                },
            },
        ];

        for (i, case) in cases.into_iter().enumerate() {
            println!("========================================");
            println!("case #{}:", i + 1);
            println!("{case:#?}");

            let Case {
                policy,
                results: results_expected,
            } = case;

            let results_actual = Results {
                no_port_no_tls: policy.validate(&request_no_port, false),
                no_port_with_tls: policy.validate(&request_no_port, true),
                with_port_no_tls: policy.validate(&request_with_port, false),
                with_port_with_tls: policy.validate(&request_with_port, true),
            };

            assert!(
                results_actual == results_expected,
                "\nActual:\n{results_actual:#?}",
            );
        }
    }
}
