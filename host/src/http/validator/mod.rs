//! Validators that can allow/deny HTTP requests.
use std::{fmt, net::IpAddr};

pub use allow_certain::{AllowCertainHttpRequests, AllowHttpEndpoint, AllowHttpHost};
pub use reject_all::RejectAllHttpRequests;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;

use crate::http::types::HttpConnectionMode;

mod allow_certain;
mod reject_all;

/// Reject HTTP request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HttpRequestRejected;

impl fmt::Display for HttpRequestRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("rejected")
    }
}

impl std::error::Error for HttpRequestRejected {}

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
        mode: HttpConnectionMode,
    ) -> Result<(), HttpRequestRejected>;

    /// Validate an IP address resolved for a host.
    ///
    /// The default allows every address, preserving the behavior of validators that only inspect HTTP requests.
    /// Implementations that restrict a host's possible DNS results should return [`Err`] for rejected addresses.
    fn validate_ip(&self, _host: &str, _ip: IpAddr) -> Result<(), HttpRequestRejected> {
        Ok(())
    }
}
