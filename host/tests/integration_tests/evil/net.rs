use std::sync::Arc;

use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::{DataType, Field},
};
use datafusion_common::{config::ConfigOptions, test_util::batches_to_string};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, async_udf::AsyncScalarUDFImpl,
};
use datafusion_udf_wasm_host::WasmScalarUdf;

use crate::integration_tests::{evil::test_utils::try_scalar_udfs, test_utils::ColumnarValueExt};

const ADDRESSES: &[&str] = &[
    "0.0.0.0:0",
    "127.0.0.1:0",
    "[::1]:1",
    "1.1.1.1:80",
    "localhost:1337",
    "localhost.local.:1337",
    "google.com:80",
    "google.com:443",
];

#[tokio::test]
async fn test_http_get() {
    let udf = udf("http_get").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-----------------------+-----------------------------------+
    | input                 | output                            |
    +-----------------------+-----------------------------------+
    | 0.0.0.0:0             | ERR: ErrorCode::HttpRequestDenied |
    | 127.0.0.1:0           | ERR: ErrorCode::HttpRequestDenied |
    | [::1]:1               | ERR: ErrorCode::HttpRequestDenied |
    | 1.1.1.1:80            | ERR: ErrorCode::HttpRequestDenied |
    | localhost:1337        | ERR: ErrorCode::HttpRequestDenied |
    | localhost.local.:1337 | ERR: ErrorCode::HttpRequestDenied |
    | google.com:80         | ERR: ErrorCode::HttpRequestDenied |
    | google.com:443        | ERR: ErrorCode::HttpRequestDenied |
    +-----------------------+-----------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_http_post() {
    let udf = udf("http_post").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-----------------------+-----------------------------------+
    | input                 | output                            |
    +-----------------------+-----------------------------------+
    | 0.0.0.0:0             | ERR: ErrorCode::HttpRequestDenied |
    | 127.0.0.1:0           | ERR: ErrorCode::HttpRequestDenied |
    | [::1]:1               | ERR: ErrorCode::HttpRequestDenied |
    | 1.1.1.1:80            | ERR: ErrorCode::HttpRequestDenied |
    | localhost:1337        | ERR: ErrorCode::HttpRequestDenied |
    | localhost.local.:1337 | ERR: ErrorCode::HttpRequestDenied |
    | google.com:80         | ERR: ErrorCode::HttpRequestDenied |
    | google.com:443        | ERR: ErrorCode::HttpRequestDenied |
    +-----------------------+-----------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_https_get() {
    let udf = udf("https_get").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-----------------------+-----------------------------------+
    | input                 | output                            |
    +-----------------------+-----------------------------------+
    | 0.0.0.0:0             | ERR: ErrorCode::HttpRequestDenied |
    | 127.0.0.1:0           | ERR: ErrorCode::HttpRequestDenied |
    | [::1]:1               | ERR: ErrorCode::HttpRequestDenied |
    | 1.1.1.1:80            | ERR: ErrorCode::HttpRequestDenied |
    | localhost:1337        | ERR: ErrorCode::HttpRequestDenied |
    | localhost.local.:1337 | ERR: ErrorCode::HttpRequestDenied |
    | google.com:80         | ERR: ErrorCode::HttpRequestDenied |
    | google.com:443        | ERR: ErrorCode::HttpRequestDenied |
    +-----------------------+-----------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_tcp_connect() {
    let udf = udf("tcp_connect").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-----------------------+------------------------------------------------------------------+
    | input                 | output                                                           |
    +-----------------------+------------------------------------------------------------------+
    | 0.0.0.0:0             | ERR: Permission denied (os error 2)                              |
    | 127.0.0.1:0           | ERR: Permission denied (os error 2)                              |
    | [::1]:1               | ERR: Permission denied (os error 2)                              |
    | 1.1.1.1:80            | ERR: Permission denied (os error 2)                              |
    | localhost:1337        | ERR: failed to lookup address information: Non-recoverable error |
    | localhost.local.:1337 | ERR: failed to lookup address information: Non-recoverable error |
    | google.com:80         | ERR: failed to lookup address information: Non-recoverable error |
    | google.com:443        | ERR: failed to lookup address information: Non-recoverable error |
    +-----------------------+------------------------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_udp_bind() {
    let udf = udf("udp_bind").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-----------------------+------------------------------------------------------------------+
    | input                 | output                                                           |
    +-----------------------+------------------------------------------------------------------+
    | 0.0.0.0:0             | ERR: Permission denied (os error 2)                              |
    | 127.0.0.1:0           | ERR: Permission denied (os error 2)                              |
    | [::1]:1               | ERR: Permission denied (os error 2)                              |
    | 1.1.1.1:80            | ERR: Permission denied (os error 2)                              |
    | localhost:1337        | ERR: failed to lookup address information: Non-recoverable error |
    | localhost.local.:1337 | ERR: failed to lookup address information: Non-recoverable error |
    | google.com:80         | ERR: failed to lookup address information: Non-recoverable error |
    | google.com:443        | ERR: failed to lookup address information: Non-recoverable error |
    +-----------------------+------------------------------------------------------------------+
    ",
    );
}

/// Get evil UDF.
async fn udf(name: &'static str) -> WasmScalarUdf {
    try_scalar_udfs("net")
        .await
        .unwrap()
        .into_iter()
        .find(|udf| udf.name() == name)
        .unwrap()
}

/// Run UDF that expects one string input.
async fn run_1(udf: &WasmScalarUdf) -> String {
    let input = Arc::new(
        ADDRESSES
            .iter()
            .map(|p| Some(p.to_owned()))
            .collect::<StringArray>(),
    ) as _;

    let output = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::clone(&input))],
            arg_fields: vec![Arc::new(Field::new("a", DataType::Utf8, true))],
            number_rows: ADDRESSES.len(),
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();

    batches_to_string(&[
        RecordBatch::try_from_iter([("input", input), ("output", output)]).unwrap(),
    ])
}
