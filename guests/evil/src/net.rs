//! Network-related evils.

use std::sync::Arc;

use datafusion_common::error::Result as DataFusionResult;
use datafusion_expr::ScalarUDFImpl;
use wasip2::http::types::{Method, Scheme};

use crate::common::String1Udf;

/// Perform a WASIp2 HTTP request.
fn wasip2_http(addr: &str, method: &Method, scheme: &Scheme) -> Result<String, String> {
    use wasip2::http::{
        outgoing_handler::{OutgoingRequest, handle},
        types::Fields,
    };

    let request = OutgoingRequest::new(Fields::new());
    request.set_method(method).unwrap();
    request.set_path_with_query(Some("/")).unwrap();
    request.set_scheme(Some(scheme)).unwrap();
    request.set_authority(Some(addr)).unwrap();

    let resp = handle(request, None).unwrap();
    resp.subscribe().block();
    resp.get()
        .expect("subscribed, hence not None")
        .expect("we did NOT retrieve the response before")
        .map(|_| "connected".to_owned())
        .map_err(|e| e.to_string())
}

/// Returns our evil UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![
        Arc::new(String1Udf::new("http_get", |addr| {
            wasip2_http(&addr, &Method::Get, &Scheme::Http)
        })),
        Arc::new(String1Udf::new("http_post", |addr| {
            wasip2_http(&addr, &Method::Post, &Scheme::Http)
        })),
        Arc::new(String1Udf::new("https_get", |addr| {
            wasip2_http(&addr, &Method::Get, &Scheme::Https)
        })),
        Arc::new(String1Udf::new("tcp_connect", |addr| {
            std::net::TcpStream::connect(addr)
                .map(|_| "connected".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("udp_bind", |addr| {
            std::net::UdpSocket::bind(addr)
                .map(|_| "bound".to_owned())
                .map_err(|e| e.to_string())
        })),
    ])
}
