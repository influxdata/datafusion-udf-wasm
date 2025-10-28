//! Interfaces for HTTP interactions of the guest.

use std::{borrow::Cow, collections::HashSet, fmt};

use http::Method;
use wasmtime_wasi_http::body::HyperOutgoingBody;

/// Validates if an outgoing HTTP interaction is allowed.
///
/// You can implement your own business logic here or use one of the pre-built implementations in [this module](self).
pub trait HttpRequestValidator: fmt::Debug + Send + Sync + 'static {
    /// Validate incoming request.
    ///
    /// Return [`Ok`] if the request should be allowed, return [`Err`] otherwise.
    fn validate(
        &self,
        request: &hyper::Request<HyperOutgoingBody>,
        use_tls: bool,
    ) -> Result<(), Rejected>;
}

/// Reject ALL requests.
#[derive(Debug, Clone, Copy, Default)]
pub struct RejectAllHttpRequests;

impl HttpRequestValidator for RejectAllHttpRequests {
    fn validate(
        &self,
        _request: &hyper::Request<HyperOutgoingBody>,
        _use_tls: bool,
    ) -> Result<(), Rejected> {
        Err(Rejected)
    }
}

/// A request matcher.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Matcher {
    /// Method.
    pub method: Method,

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
#[derive(Debug, Clone, Default)]
pub struct AllowCertainHttpRequests {
    /// Set of all matchers.
    ///
    /// If ANY of them matches, the request will be allowed.
    matchers: HashSet<Matcher>,
}

impl AllowCertainHttpRequests {
    /// Create new, empty request matcher.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow given request.
    pub fn allow(&mut self, matcher: Matcher) {
        self.matchers.insert(matcher);
    }
}

impl HttpRequestValidator for AllowCertainHttpRequests {
    fn validate(
        &self,
        request: &hyper::Request<HyperOutgoingBody>,
        use_tls: bool,
    ) -> Result<(), Rejected> {
        let matcher = Matcher {
            method: request.method().clone(),
            host: request.uri().host().ok_or(Rejected)?.to_owned().into(),
            port: request
                .uri()
                .port_u16()
                .unwrap_or(if use_tls { 443 } else { 80 }),
        };

        if self.matchers.contains(&matcher) {
            Ok(())
        } else {
            Err(Rejected)
        }
    }
}

/// Reject HTTP request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Rejected;

impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("rejected")
    }
}

impl std::error::Error for Rejected {}

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
            .method(Method::GET)
            .uri("http://foo.bar")
            .body(Default::default())
            .unwrap();

        let request_with_port = hyper::Request::builder()
            .method(Method::GET)
            .uri("http://my.universe:1337")
            .body(Default::default())
            .unwrap();

        struct Case {
            matchers: Vec<Matcher>,
            result_no_port_no_tls: Result<(), Rejected>,
            result_no_port_with_tls: Result<(), Rejected>,
            result_with_port_no_tls: Result<(), Rejected>,
            result_with_port_with_tls: Result<(), Rejected>,
        }

        let cases = [
            Case {
                matchers: vec![],
                result_no_port_no_tls: Err(Rejected),
                result_no_port_with_tls: Err(Rejected),
                result_with_port_no_tls: Err(Rejected),
                result_with_port_with_tls: Err(Rejected),
            },
            Case {
                matchers: vec![Matcher {
                    method: Method::GET,
                    host: "foo.bar".into(),
                    port: 80,
                }],
                result_no_port_no_tls: Ok(()),
                result_no_port_with_tls: Err(Rejected),
                result_with_port_no_tls: Err(Rejected),
                result_with_port_with_tls: Err(Rejected),
            },
            Case {
                matchers: vec![Matcher {
                    method: Method::GET,
                    host: "foo.bar".into(),
                    port: 443,
                }],
                result_no_port_no_tls: Err(Rejected),
                result_no_port_with_tls: Ok(()),
                result_with_port_no_tls: Err(Rejected),
                result_with_port_with_tls: Err(Rejected),
            },
            Case {
                matchers: vec![Matcher {
                    method: Method::POST,
                    host: "foo.bar".into(),
                    port: 80,
                }],
                result_no_port_no_tls: Err(Rejected),
                result_no_port_with_tls: Err(Rejected),
                result_with_port_no_tls: Err(Rejected),
                result_with_port_with_tls: Err(Rejected),
            },
            Case {
                matchers: vec![Matcher {
                    method: Method::GET,
                    host: "my.universe".into(),
                    port: 80,
                }],
                result_no_port_no_tls: Err(Rejected),
                result_no_port_with_tls: Err(Rejected),
                result_with_port_no_tls: Err(Rejected),
                result_with_port_with_tls: Err(Rejected),
            },
            Case {
                matchers: vec![Matcher {
                    method: Method::GET,
                    host: "my.universe".into(),
                    port: 1337,
                }],
                result_no_port_no_tls: Err(Rejected),
                result_no_port_with_tls: Err(Rejected),
                result_with_port_no_tls: Ok(()),
                result_with_port_with_tls: Ok(()),
            },
            Case {
                matchers: vec![
                    Matcher {
                        method: Method::GET,
                        host: "foo.bar".into(),
                        port: 80,
                    },
                    Matcher {
                        method: Method::POST,
                        host: "foo.bar".into(),
                        port: 80,
                    },
                    Matcher {
                        method: Method::GET,
                        host: "my.universe".into(),
                        port: 1337,
                    },
                ],
                result_no_port_no_tls: Ok(()),
                result_no_port_with_tls: Err(Rejected),
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
}
