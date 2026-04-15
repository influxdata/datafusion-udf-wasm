use std::{
    collections::HashSet,
    io::Write,
    sync::{Arc, LazyLock},
    time::Duration,
};

use arrow::{
    array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, StringBuilder},
    datatypes::{DataType, Field},
};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{ScalarValue, cast::as_string_array};
use datafusion_execution::memory_pool::UnboundedMemoryPool;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, async_udf::AsyncScalarUDFImpl,
};
use datafusion_udf_wasm_host::{
    AllowCertainHttpRequests, HttpConnectionMode, HttpPort, HttpRequestValidator, WasmPermissions,
    WasmScalarUdf,
};
use http::{
    HeaderName, HeaderValue, Method,
    header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE},
};
use http_body_util::{BodyExt, Full};
use regex::Regex;
use tokio::runtime::Handle;
use wasmtime_wasi_http::DEFAULT_FORBIDDEN_HEADERS;

use crate::integration_tests::{
    python::{
        runtime::http::mock_server::{
            Matcher, MockServer, Request, ResponseGenFn, ServerMock, SimpleResponseGen,
        },
        test_utils::{python_component, python_scalar_udf},
    },
    test_utils::ColumnarValueExt,
};

mod mock_server;

#[tokio::test]
async fn test_requests_simple() {
    const CODE: &str = r#"
import requests

def perform_request(url: str) -> str:
    return requests.get(url).text
"#;

    let server = MockServer::start().await;
    server.mock(ServerMock {
        response: Box::new(SimpleResponseGen {
            body: "hello world!".to_owned(),
            ..Default::default()
        }),
        ..Default::default()
    });

    let mut permissions = AllowCertainHttpRequests::new();
    let endpoint = permissions
        .allow_host(server.address().ip().to_string())
        .allow_port(HttpPort::new(server.address().port()).unwrap());
    endpoint.allow_mode(HttpConnectionMode::PlainText);
    endpoint.allow_method(http::Method::GET);
    let udf = python_udf_with_permissions(CODE, permissions).await;

    let array = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(server.uri())))],
            arg_fields: vec![Arc::new(Field::new("uri", DataType::Utf8, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();

    assert_eq!(
        array.as_ref(),
        &StringArray::from_iter([Some("hello world!".to_owned()),]) as &dyn Array,
    );
}

#[tokio::test]
async fn test_urllib3_unguarded_fail() {
    const CODE: &str = r#"
import urllib3

def perform_request(url: str) -> str:
    resp = urllib3.request("GET", url)
    return resp.data.decode("utf-8")
"#;
    let udf = python_scalar_udf(CODE).await.unwrap();

    let server = MockServer::start().await;
    server.mock(ServerMock {
        hits: Some(0),
        ..Default::default()
    });

    let err = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(server.uri())))],
            arg_fields: vec![Arc::new(Field::new("uri", DataType::Utf8, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap_err();

    // the port number is part of the error message and not deterministic, so we replace it with a placeholder
    let err = err
        .to_string()
        .replace(&format!("port={}", server.address().port()), "port=???");

    insta::assert_snapshot!(
        normalize_exception_location(err),
        @r#"
    cannot call function
    caused by
    Execution error: urllib3.exceptions.ProtocolError: ('Connection aborted.', WasiErrorCode('Request failed with wasi http error ErrorCode_HttpRequestDenied'))

    The above exception was the direct cause of the following exception:

    Traceback (most recent call last):
      File "<FILE>", line <LINE>, in perform_request
      File "<FILE>", line <LINE>, in request
        return _DEFAULT_POOL.request(
               ~~~~~~~~~~~~~~~~~~~~~^
            method,
            ^^^^^^^
        ...<9 lines>...
            json=json,
            ^^^^^^^^^^
        )
        ^
      File "<FILE>", line <LINE>, in request
        return self.request_encode_url(
               ~~~~~~~~~~~~~~~~~~~~~~~^
            method,
            ^^^^^^^
        ...<3 lines>...
            **urlopen_kw,
            ^^^^^^^^^^^^^
        )
        ^
      File "<FILE>", line <LINE>, in request_encode_url
        return self.urlopen(method, url, **extra_kw)
               ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^
      File "<FILE>", line <LINE>, in urlopen
        response = conn.urlopen(method, u.request_uri, **kw)
      File "<FILE>", line <LINE>, in urlopen
        return self.urlopen(
               ~~~~~~~~~~~~^
            method,
            ^^^^^^^
        ...<13 lines>...
            **response_kw,
            ^^^^^^^^^^^^^^
        )
        ^
      File "<FILE>", line <LINE>, in urlopen
        return self.urlopen(
               ~~~~~~~~~~~~^
            method,
            ^^^^^^^
        ...<13 lines>...
            **response_kw,
            ^^^^^^^^^^^^^^
        )
        ^
      File "<FILE>", line <LINE>, in urlopen
        return self.urlopen(
               ~~~~~~~~~~~~^
            method,
            ^^^^^^^
        ...<13 lines>...
            **response_kw,
            ^^^^^^^^^^^^^^
        )
        ^
      File "<FILE>", line <LINE>, in urlopen
        retries = retries.increment(
            method, url, error=new_e, _pool=self, _stacktrace=sys.exc_info()[2]
        )
      File "<FILE>", line <LINE>, in increment
        raise MaxRetryError(_pool, url, reason) from reason  # type: ignore[arg-type]
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    urllib3.exceptions.MaxRetryError: HTTPConnectionPool(host='127.0.0.1', port=???): Max retries exceeded with url: / (Caused by ProtocolError('Connection aborted.', WasiErrorCode('Request failed with wasi http error ErrorCode_HttpRequestDenied')))
    "#,
    );
}

#[tokio::test]
async fn test_integration() {
    const CODE: &str = r#"
import requests
import urllib3

def _headers_str_to_dict(headers: str) -> dict[str, str]:
    headers_dct = {}
    if headers is not None:
        for k_v in headers.split(";"):
            [k, v] = k_v.split(":")
            headers_dct[k] = v
    return headers_dct

def _headers_dict_to_str(headers: dict[str, str]) -> str:
    headers = ";".join((
        f"{k}:{v}"
        for k, v in headers.items()
        if k not in ["content-length", "content-type", "date"]
    ))
    if not headers:
        return "n/a"
    else:
        return headers

def test_requests(method: str, url: str, headers: str | None, body: str | None) -> str:
    try:
        resp = requests.request(
            method=method,
            url=url,
            headers=_headers_str_to_dict(headers),
            data=body,
        )
    except requests.ConnectionError as e:
        (e,) = e.args
        assert isinstance(e, Exception)
        return f"ERR: {e}"
    except Exception as e:
        return f"ERR: {e}"

    resp_status = resp.status_code
    resp_body = f"'{resp.text}'" if resp.text else "n/a"
    resp_headers = _headers_dict_to_str(resp.headers)

    return f"OK: status={resp_status} headers={resp_headers} body={resp_body}"

def test_urllib3(method: str, url: str, headers: str | None, body: str | None) -> str:
    try:
        resp = urllib3.request(
            method=method,
            url=url,
            headers=_headers_str_to_dict(headers),
            body=body,
        )
    except urllib3.exceptions.MaxRetryError as e:
        e = e.reason
        return f"ERR: {e}"
    except Exception as e:
        return f"ERR: {e}"

    resp_status = resp.status
    resp_body = f"'{resp.data.decode("utf-8")}'" if resp.data else "n/a"
    resp_headers = _headers_dict_to_str(resp.headers)

    return f"OK: status={resp_status} headers={resp_headers} body={resp_body}"
"#;
    const NUMBER_OF_IMPLEMENTATIONS: usize = 2;

    let mut cases = vec![
        TestCase {
            resp: Ok(TestResponse {
                body: Some("case_1"),
                ..Default::default()
            }),
            ..Default::default()
        },
        TestCase {
            method: "POST",
            resp: Ok(TestResponse {
                body: Some("case_2"),
                ..Default::default()
            }),
            ..Default::default()
        },
        TestCase {
            path: "/foo".to_owned(),
            resp: Ok(TestResponse {
                body: Some("case_3"),
                ..Default::default()
            }),
            ..Default::default()
        },
        TestCase {
            path: "/500".to_owned(),
            resp: Ok(TestResponse {
                status: 500,
                body: Some("case_4"),
                ..Default::default()
            }),
            ..Default::default()
        },
        TestCase {
            path: "/headers_in".to_owned(),
            requ_headers: vec![
                ("foo".to_owned(), &["bar"]),
                ("multi".to_owned(), &["some", "thing"]),
            ],
            resp: Ok(TestResponse {
                body: Some("case_5"),
                ..Default::default()
            }),
            ..Default::default()
        },
        TestCase {
            path: "/headers_out".to_owned(),
            resp: Ok(TestResponse {
                headers: vec![
                    ("foo".to_owned(), &["bar"]),
                    ("multi".to_owned(), &["some", "thing"]),
                ],
                body: Some("case_6"),
                ..Default::default()
            }),
            ..Default::default()
        },
        TestCase {
            path: "/body_in".to_owned(),
            requ_body: Some("foo"),
            resp: Ok(TestResponse {
                body: Some("case_7"),
                ..Default::default()
            }),
            ..Default::default()
        },
        TestCase {
            path: "/no_body_out".to_owned(),
            resp: Ok(TestResponse {
                body: None,
                ..Default::default()
            }),
            ..Default::default()
        },
        TestCase {
            base: Some("http://test.com"),
            resp: Err("('Connection aborted.', WasiErrorCode('Request failed with wasi http error ErrorCode_HttpRequestDenied'))".to_owned()),
            ..Default::default()
        },
        // Python `requests` sends this so we allow it but later drop it from the actual request.
        TestCase {
            path: "/forbidden_header/connection".to_owned(),
            requ_headers: vec![(http::header::CONNECTION.to_string(), &["foo"])],
            resp: Ok(TestResponse {
                body: Some("header is filtered"),
                ..Default::default()
            }),
            ..Default::default()
        },
    ];
    cases.extend(
        DEFAULT_FORBIDDEN_HEADERS
            .iter()
            // Python `requests` sends this so we allow it but later drop it from the actual request.
            .filter(|h| *h != http::header::CONNECTION)
            .map(|h| TestCase {
                path: format!("/forbidden_header/{h}"),
                requ_headers: vec![(h.to_string(), &["foo"])],
                resp: Err("Err { value: HeaderError_Forbidden }".to_owned()),
                ..Default::default()
            }),
    );

    let server = MockServer::start().await;
    let mut permissions = AllowCertainHttpRequests::default();

    let mut builder_method = StringBuilder::new();
    let mut builder_url = StringBuilder::new();
    let mut builder_headers = StringBuilder::new();
    let mut builder_body = StringBuilder::new();
    let mut builder_result = StringBuilder::new();

    for case in &cases {
        case.allow(&server, &mut permissions);

        let TestCase {
            base,
            method,
            path,
            requ_headers,
            requ_body,
            resp,
        } = case;

        builder_method.append_value(method);
        builder_url.append_value(format!(
            "{}{}",
            base.map(|b| b.to_owned()).unwrap_or_else(|| server.uri()),
            path
        ));
        builder_headers.append_option(headers_to_string(requ_headers));
        builder_body.append_option(requ_body.map(|s| s.to_owned()));

        match resp {
            Ok(TestResponse {
                status,
                headers,
                body,
            }) => {
                let headers = headers_to_string(headers).unwrap_or_else(|| "n/a".to_owned());
                let body = body
                    .map(|s| format!("'{s}'"))
                    .unwrap_or_else(|| "n/a".to_owned());
                builder_result
                    .append_value(format!("OK: status={status} headers={headers} body={body}"));
            }
            Err(e) => {
                builder_result.append_value(format!("ERR: {e}"));
            }
        }
    }

    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::new(builder_method.finish())),
            ColumnarValue::Array(Arc::new(builder_url.finish())),
            ColumnarValue::Array(Arc::new(builder_headers.finish())),
            ColumnarValue::Array(Arc::new(builder_body.finish())),
        ],
        arg_fields: vec![
            Arc::new(Field::new("method", DataType::Utf8, true)),
            Arc::new(Field::new("url", DataType::Utf8, true)),
            Arc::new(Field::new("headers", DataType::Utf8, true)),
            Arc::new(Field::new("body", DataType::Utf8, true)),
        ],
        number_rows: cases.len(),
        return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let array_result = builder_result.finish();

    let udfs = python_udfs_with_permissions(CODE, permissions).await;
    assert_eq!(udfs.len(), NUMBER_OF_IMPLEMENTATIONS);

    for udf in udfs {
        println!("{}", udf.name());

        for case in &cases {
            case.mock(&server);
        }

        let actual = udf
            .invoke_async_with_args(args.clone())
            .await
            .unwrap()
            .unwrap_array();

        // check output and pretty-print failures
        if actual.as_ref() != &array_result as &dyn Array {
            let mask = arrow::compute::kernels::cmp::neq(&actual, &array_result).unwrap();
            let batch =
                RecordBatch::try_from_iter(
                    std::iter::once((
                        "case",
                        Arc::new(Int64Array::from_iter_values(
                            (1..=args.number_rows).map(|i| i as i64),
                        )) as ArrayRef,
                    ))
                    .chain(args.arg_fields.iter().map(|f| f.name().as_str()).zip(
                        args.args.iter().map(|c| match c {
                            ColumnarValue::Array(array) => Arc::clone(array),
                            ColumnarValue::Scalar(_) => unreachable!(),
                        }),
                    ))
                    .chain([("actual", actual), ("expected", Arc::new(array_result))]),
                )
                .unwrap();
            let batch = arrow::compute::filter_record_batch(&batch, &mask).unwrap();
            let s = arrow::util::pretty::pretty_format_batches(&[batch]).unwrap();
            panic!("FAIL:\n\n{s}");
        }

        server.clear_mocks();
    }
}

#[derive(Debug, Clone)]
struct TestResponse {
    status: u16,
    headers: Vec<(String, &'static [&'static str])>,
    body: Option<&'static str>,
}

impl Default for TestResponse {
    fn default() -> Self {
        Self {
            status: 200,
            headers: vec![],
            body: None,
        }
    }
}

struct TestCase {
    base: Option<&'static str>,
    method: &'static str,
    path: String,
    requ_headers: Vec<(String, &'static [&'static str])>,
    requ_body: Option<&'static str>,
    resp: Result<TestResponse, String>,
}

impl Default for TestCase {
    fn default() -> Self {
        Self {
            base: None,
            method: "GET",
            path: "/".to_owned(),
            requ_headers: vec![],
            requ_body: None,
            resp: Ok(TestResponse::default()),
        }
    }
}

impl TestCase {
    fn allow(&self, server: &MockServer, permissions: &mut AllowCertainHttpRequests) {
        let endpoint = permissions
            .allow_host(server.address().ip().to_string())
            .allow_port(HttpPort::new(server.address().port()).unwrap());
        endpoint.allow_mode(HttpConnectionMode::PlainText);
        endpoint.allow_method(self.method.try_into().unwrap());
    }

    fn mock(&self, server: &MockServer) {
        let Self {
            base,
            method,
            path,
            requ_headers,
            requ_body,
            resp,
        } = self;
        if base.is_some() {
            return;
        }

        let TestResponse {
            status: resp_status,
            headers: resp_headers,
            body: resp_body,
        } = resp.clone().unwrap_or_default();

        let matcher = Matcher {
            method: Some([Method::try_from(*method).unwrap()].into()),
            path: Some(path.clone()),
            headers: Some(
                requ_headers
                    .iter()
                    .filter(|(k, _v)| {
                        // Python `requests` sends this so we allow it but later drop it from the actual request.
                        k.as_str() != http::header::CONNECTION
                    })
                    .flat_map(|(k, v)| {
                        let k = HeaderName::try_from(k.clone()).unwrap();

                        v.iter()
                            .map(move |v| (k.clone(), HeaderValue::from_str(v).unwrap()))
                    })
                    .collect(),
            ),
            body: requ_body.map(|s| s.to_string()),
        };

        let response = SimpleResponseGen {
            status: resp_status.try_into().unwrap(),
            headers: Some(
                resp_headers
                    .iter()
                    .flat_map(|(k, v)| {
                        let k = HeaderName::try_from(k.clone()).unwrap();

                        v.iter()
                            .map(move |v| (k.clone(), HeaderValue::from_str(v).unwrap()))
                    })
                    .collect(),
            ),
            body: resp_body.map(|s| s.to_owned()).unwrap_or_default(),
        };

        let hits = if resp.is_ok() { 1 } else { 0 };

        server.mock(ServerMock {
            matcher,
            response: Box::new(response),
            hits: Some(hits),
        });
    }
}

fn headers_to_string(headers: &[(String, &[&str])]) -> Option<String> {
    if headers.is_empty() {
        None
    } else {
        let headers = headers
            .iter()
            .map(|(k, v)| format!("{k}:{}", v.join(",")))
            .collect::<Vec<_>>();
        Some(headers.join(";"))
    }
}

async fn python_udfs_with_permissions<V>(code: &'static str, permissions: V) -> Vec<WasmScalarUdf>
where
    V: HttpRequestValidator,
{
    WasmScalarUdf::new(
        python_component().await,
        &WasmPermissions::new().with_http(permissions),
        Handle::current(),
        &(Arc::new(UnboundedMemoryPool::default()) as _),
        code.to_owned(),
    )
    .await
    .unwrap()
}

async fn python_udf_with_permissions<V>(code: &'static str, permissions: V) -> WasmScalarUdf
where
    V: HttpRequestValidator,
{
    let udfs = python_udfs_with_permissions(code, permissions).await;
    assert_eq!(udfs.len(), 1);
    udfs.into_iter().next().unwrap()
}

#[test]
fn test_io_runtime() {
    const CODE: &str = r#"
import urllib3

def perform_request(url: str) -> str:
    resp = urllib3.request("GET", url)
    return resp.data.decode("utf-8")
"#;

    let rt_tmp = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let rt_cpu = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        // It would be nice if all the timeouts-related timers would also run within the within the I/O runtime, but
        // that requires some larger intervention (either upstream or with a custom WASI HTTP implementation).
        // Hence, we don't do that yet.
        .enable_time()
        .build()
        .unwrap();
    let rt_io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let server = rt_io.block_on(async {
        let server = MockServer::start().await;
        server.mock(ServerMock {
            response: Box::new(SimpleResponseGen {
                body: "hello world!".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        });
        server
    });

    // deliberately use a runtime what we are going to throw away later to prevent tricks like `Handle::current`
    let udf = rt_tmp.block_on(async {
        let mut permissions = AllowCertainHttpRequests::new();
        let endpoint = permissions
            .allow_host(server.address().ip().to_string())
            .allow_port(HttpPort::new(server.address().port()).unwrap());
        endpoint.allow_mode(HttpConnectionMode::PlainText);
        endpoint.allow_method(http::Method::GET);

        let udfs = WasmScalarUdf::new(
            python_component().await,
            &WasmPermissions::new().with_http(permissions),
            rt_io.handle().clone(),
            &(Arc::new(UnboundedMemoryPool::default()) as _),
            CODE.to_owned(),
        )
        .await
        .unwrap();
        assert_eq!(udfs.len(), 1);
        udfs.into_iter().next().unwrap()
    });
    rt_tmp.shutdown_timeout(Duration::from_secs(1));

    let array = rt_cpu.block_on(async {
        udf.invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(server.uri())))],
            arg_fields: vec![Arc::new(Field::new("uri", DataType::Utf8, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array()
    });

    assert_eq!(
        array.as_ref(),
        &StringArray::from_iter([Some("hello world!".to_owned()),]) as &dyn Array,
    );
}

/// This is the originally observed bug:
/// When trying to run a normal GET request with `requests`, large responses got truncated.
#[tokio::test]
async fn test_large_response_requests() {
    const CODE: &str = r#"
import requests

def perform_request(url: str) -> str:
    return requests.get(url).text
"#;

    assert_large_response_works(CODE).await;
}

/// Tests `urllib3` in the same way that `requests` would call it
#[tokio::test]
async fn test_large_response_urllib3_reproducer() {
    const CODE: &str = r#"
import urllib3

def perform_request(url: str) -> str:
    resp = urllib3.request("GET", url, preload_content=False, decode_content=False)
    data = b""
    for part in resp.stream():
        data += part
    return data.decode("utf-8")
"#;

    assert_large_response_works(CODE).await;
}

/// Tests `urllib3` with a super simple calling convention.
#[tokio::test]
async fn test_large_response_urllib3_plain() {
    const CODE: &str = r#"
import urllib3

def perform_request(url: str) -> str:
    resp = urllib3.request("GET", url)
    return resp.data.decode("utf-8")
"#;

    assert_large_response_works(CODE).await;
}

async fn assert_large_response_works(code: &'static str) {
    const CONTENT_LEN: usize = 10_000;
    let content = std::iter::repeat_n('x', CONTENT_LEN).collect::<String>();

    let server = MockServer::start().await;
    server.mock(ServerMock {
        response: Box::new(SimpleResponseGen {
            body: content.to_owned(),
            ..Default::default()
        }),
        ..Default::default()
    });

    let mut permissions = AllowCertainHttpRequests::new();
    let endpoint = permissions
        .allow_host(server.address().ip().to_string())
        .allow_port(HttpPort::new(server.address().port()).unwrap());
    endpoint.allow_mode(HttpConnectionMode::PlainText);
    endpoint.allow_method(http::Method::GET);
    let udf = python_udf_with_permissions(code, permissions).await;

    let array = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(server.uri())))],
            arg_fields: vec![Arc::new(Field::new("uri", DataType::Utf8, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();

    let out = as_string_array(&array)
        .unwrap()
        .iter()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(out.len(), CONTENT_LEN);
}

#[tokio::test]
async fn test_compression() {
    const CODE: &str = r#"
import requests

def perform_request(url: str) -> str:
    return requests.get(url).text
"#;

    const CONTENT: &str = "hello world!";

    let mut paths = Vec::new();
    let server = MockServer::start().await;
    for compression in [None]
        .into_iter()
        .chain(Compression::all().iter().map(Some))
    {
        let path_suffix = compression.map(Compression::as_str).unwrap_or("none");
        let path = format!("/{path_suffix}");
        paths.push(path.clone());

        server.mock(ServerMock {
            matcher: Matcher {
                path: Some(path),
                ..Default::default()
            },
            response: Box::new(ResponseGenFn::new(move |req: &Request| {
                // poor man's header parsing
                //
                // See:
                // - https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Accept-Encoding
                // - https://developer.mozilla.org/en-US/docs/Glossary/Quality_values
                let accepted = req
                    .headers()
                    .get(ACCEPT_ENCODING)
                    .into_iter()
                    .flat_map(|v| v.to_str().unwrap().split(","))
                    .filter_map(|v| {
                        let v = v.trim();

                        let compression = match v.split_once(";q=") {
                            Some((compression, q)) => {
                                let q: f32 = q.parse().unwrap();
                                assert!((0.0..=1.0).contains(&q));
                                if q == 0.0 {
                                    return None;
                                }
                                compression
                            }
                            None => v,
                        };
                        let compression = Compression::try_from(compression).unwrap();
                        Some(compression)
                    })
                    .collect::<HashSet<_>>();

                let mut body_bytes = CONTENT.as_bytes().to_vec();
                let mut content_encoding = None;
                if let Some(compression) = compression
                    && accepted.contains(compression)
                {
                    body_bytes = compression.encode(&body_bytes);
                    content_encoding = Some(compression.as_str());
                }

                let mut resp = http::Response::builder()
                    .status(200)
                    .header(CONTENT_TYPE, "text/plain");
                if let Some(content_encoding) = content_encoding {
                    resp = resp.header(CONTENT_ENCODING, content_encoding);
                }
                resp.body(Full::new(body_bytes.into()).boxed()).unwrap()
            })),
            ..Default::default()
        });
    }

    let mut permissions = AllowCertainHttpRequests::new();
    let endpoint = permissions
        .allow_host(server.address().ip().to_string())
        .allow_port(HttpPort::new(server.address().port()).unwrap());
    endpoint.allow_mode(HttpConnectionMode::PlainText);
    endpoint.allow_method(http::Method::GET);
    let udf = python_udf_with_permissions(CODE, permissions).await;

    let array = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from_iter(
                paths.iter().map(|p| Some(format!("{}{}", server.uri(), p))),
            )))],
            arg_fields: vec![Arc::new(Field::new("uri", DataType::Utf8, true))],
            number_rows: paths.len(),
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();

    assert_eq!(
        array.as_ref(),
        &StringArray::from_iter(std::iter::repeat_n(Some(CONTENT.to_owned()), paths.len()))
            as &dyn Array,
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Compression {
    Deflate,
    Gzip,
}

impl Compression {
    fn all() -> &'static [Self] {
        &[Self::Deflate, Self::Gzip]
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Deflate => "deflate",
            Self::Gzip => "gzip",
        }
    }

    fn encode(&self, data: &[u8]) -> Vec<u8> {
        match self {
            Self::Deflate => {
                let mut enc = flate2::write::DeflateEncoder::new(Vec::new(), Default::default());
                enc.write_all(data).unwrap();
                enc.finish().unwrap()
            }
            Self::Gzip => {
                let mut enc = flate2::write::GzEncoder::new(Vec::new(), Default::default());
                enc.write_all(data).unwrap();
                enc.finish().unwrap()
            }
        }
    }
}

impl TryFrom<&str> for Compression {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "deflate" => Ok(Self::Deflate),
            "gzip" => Ok(Self::Gzip),
            other => Err(format!("unknown accept-encoding value: `{other}`")),
        }
    }
}

/// Normalize line & column numbers in exception message, so that changing the code in the respective file does not change
/// the expected outcome. This makes it easier to add new test cases or update the code without needing to update all
/// results.
fn normalize_exception_location(e: impl ToString) -> String {
    let e = e.to_string();

    static REGEX: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r#"File "[^"]+", line [0-9]+, in (?<m>[a-zA-Z0-9_]+)"#).unwrap()
    });

    REGEX
        .replace_all(&e, r#"File "<FILE>", line <LINE>, in $m"#)
        .to_string()
}
