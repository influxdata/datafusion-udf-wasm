//! [`AllowHttpEndpoint`].
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use wasmtime_wasi_http::p2::body::HyperOutgoingBody;

use crate::http::{
    types::{HttpConnectionMode, HttpMethod, HttpPort},
    validator::{HttpRequestRejected, HttpRequestValidator},
};

/// Allow settings for a given endpoint.
///
/// An endpoint is defined by a host + port.
#[derive(Debug, Clone, Default)]
pub struct AllowHttpEndpoint {
    /// Connection mode.
    mode: HttpConnectionMode,

    /// Allowed methods.
    methods: HashSet<HttpMethod>,
}

impl AllowHttpEndpoint {
    /// Allow given connection mode.
    ///
    /// Note that only one mode can be allowed. Calling this method multiple times will keep the last value.
    pub fn allow_mode(&mut self, mode: HttpConnectionMode) {
        self.mode = mode;
    }

    /// Allow given HTTP method.
    ///
    /// Multiple methods can be allowed.
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
        mode: HttpConnectionMode,
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
                    .unwrap_or_else(|| mode.default_port()),
            )
            .ok_or(HttpRequestRejected)?;

        if endpoint.mode != mode {
            return Err(HttpRequestRejected);
        }

        if !endpoint.methods.contains(request.method()) {
            return Err(HttpRequestRejected);
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_allow_deny() {
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

        let request_zero_port = hyper::Request::builder()
            .method(HttpMethod::GET)
            .uri(format!("http://{HOST_1}:0"))
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
                        .allow_port(HttpConnectionMode::PlainText.default_port());
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
                        .allow_port(HttpConnectionMode::PlainText.default_port())
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

                    let endpoint = policy
                        .allow_host(HOST_1)
                        .allow_port(HttpConnectionMode::PlainText.default_port());
                    endpoint.allow_mode(HttpConnectionMode::PlainText);
                    endpoint.allow_method(HttpMethod::GET);

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
                        .allow_port(HttpConnectionMode::Encrypted.default_port())
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

                    let endpoint = policy
                        .allow_host(HOST_1)
                        .allow_port(HttpConnectionMode::Encrypted.default_port());
                    endpoint.allow_mode(HttpConnectionMode::PlainText);
                    endpoint.allow_method(HttpMethod::GET);

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
                        .allow_port(HttpConnectionMode::PlainText.default_port())
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
                        .allow_port(HttpConnectionMode::PlainText.default_port())
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
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Ok(()),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();

                    let endpoint = policy.allow_host(HOST_2).allow_port(SPECIFIC_PORT);
                    endpoint.allow_mode(HttpConnectionMode::PlainText);
                    endpoint.allow_method(HttpMethod::GET);

                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Ok(()),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();

                    let endpoint_1 = policy
                        .allow_host(HOST_1)
                        .allow_port(HttpConnectionMode::PlainText.default_port());
                    endpoint_1.allow_mode(HttpConnectionMode::PlainText);
                    endpoint_1.allow_method(HttpMethod::GET);
                    endpoint_1.allow_method(HttpMethod::POST);

                    let endpoint_2 = policy.allow_host(HOST_2).allow_port(SPECIFIC_PORT);
                    endpoint_2.allow_method(HttpMethod::GET);

                    policy
                },
                results: Results {
                    no_port_no_tls: Ok(()),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
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
                no_port_no_tls: policy.validate(&request_no_port, HttpConnectionMode::PlainText),
                no_port_with_tls: policy.validate(&request_no_port, HttpConnectionMode::Encrypted),
                with_port_no_tls: policy
                    .validate(&request_with_port, HttpConnectionMode::PlainText),
                with_port_with_tls: policy
                    .validate(&request_with_port, HttpConnectionMode::Encrypted),
            };

            assert!(
                results_actual == results_expected,
                "\nActual:\n{results_actual:#?}",
            );

            // zero port is never allowed
            policy
                .validate(&request_zero_port, HttpConnectionMode::PlainText)
                .unwrap_err();
            policy
                .validate(&request_zero_port, HttpConnectionMode::Encrypted)
                .unwrap_err();
        }
    }
}
