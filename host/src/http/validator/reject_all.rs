//! [`RejectAllHttpRequests`].
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;

use crate::http::validator::{HttpConnectionMode, HttpRequestRejected, HttpRequestValidator};

/// Reject ALL requests.
#[derive(Debug, Clone, Copy, Default)]
pub struct RejectAllHttpRequests;

impl HttpRequestValidator for RejectAllHttpRequests {
    fn validate(
        &self,
        _request: &hyper::Request<HyperOutgoingBody>,
        _mode: HttpConnectionMode,
    ) -> Result<(), HttpRequestRejected> {
        Err(HttpRequestRejected)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_allow_deny() {
        let policy = RejectAllHttpRequests;

        let request = hyper::Request::builder().body(Default::default()).unwrap();
        policy
            .validate(&request, HttpConnectionMode::Encrypted)
            .unwrap_err();
        policy
            .validate(&request, HttpConnectionMode::PlainText)
            .unwrap_err();
    }
}
