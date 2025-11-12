use std::{sync::Arc, time::Duration};

use arrow::{
    array::{Array, StringArray, StringBuilder},
    datatypes::{DataType, Field},
};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, async_udf::AsyncScalarUDFImpl,
};
use datafusion_udf_wasm_host::{
    WasmPermissions, WasmScalarUdf,
    http::{AllowCertainHttpRequests, HttpRequestValidator, Matcher},
};
use tokio::runtime::Handle;
use wasmtime_wasi_http::types::DEFAULT_FORBIDDEN_HEADERS;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers};

use crate::integration_tests::{
    python::test_utils::{python_component, python_scalar_udf},
    test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_requests_simple() {
    const CODE: &str = r#"
import requests

def perform_request(url: str) -> str:
    return requests.get(url).text
"#;

    let server = MockServer::start().await;
    Mock::given(matchers::any())
        .respond_with(ResponseTemplate::new(200).set_body_string("hello world!"))
        .expect(1)
        .mount(&server)
        .await;

    let mut permissions = AllowCertainHttpRequests::new();
    permissions.allow(Matcher {
        method: http::Method::GET,
        host: server.address().ip().to_string().into(),
        port: server.address().port(),
    });
    let udf = python_udf_with_permissions(CODE, permissions).await;

    let array = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(server.uri())))],
                arg_fields: vec![Arc::new(Field::new("uri", DataType::Utf8, true))],
                number_rows: 1,
                return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap();

    assert_eq!(
        array.as_ref(),
        &StringArray::from_iter([Some("hello world!".to_owned()),]) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_urllib3_unguarded_fail() {
    const CODE: &str = r#"
import urllib3

def perform_request(url: str) -> str:
    resp = urllib3.request("GET", url)
    return resp.data.decode("utf-8")
"#;
    let udf = python_scalar_udf(CODE).await.unwrap();

    let server = MockServer::start().await;
    Mock::given(matchers::any())
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let err = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(server.uri())))],
                arg_fields: vec![Arc::new(Field::new("uri", DataType::Utf8, true))],
                number_rows: 1,
                return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap_err();

    // the port number is part of the error message and not deterministic, so we replace it with a placeholder
    let err = err
        .to_string()
        .replace(&format!("port={}", server.address().port()), "port=???");

    insta::assert_snapshot!(
        err,
        @r#"
    cannot call function
    caused by
    Execution error: urllib3.exceptions.ProtocolError: ('Connection aborted.', WasiErrorCode('Request failed with wasi http error ErrorCode_HttpRequestDenied'))

    The above exception was the direct cause of the following exception:

    Traceback (most recent call last):
      File "<string>", line 5, in perform_request
      File "/lib/python3.14/site-packages/urllib3/__init__.py", line 193, in request
        return _DEFAULT_POOL.request(
               ~~~~~~~~~~~~~~~~~~~~~^
            method,
            ^^^^^^^
        ...<9 lines>...
            json=json,
            ^^^^^^^^^^
        )
        ^
      File "/lib/python3.14/site-packages/urllib3/_request_methods.py", line 135, in request
        return self.request_encode_url(
               ~~~~~~~~~~~~~~~~~~~~~~~^
            method,
            ^^^^^^^
        ...<3 lines>...
            **urlopen_kw,
            ^^^^^^^^^^^^^
        )
        ^
      File "/lib/python3.14/site-packages/urllib3/_request_methods.py", line 182, in request_encode_url
        return self.urlopen(method, url, **extra_kw)
               ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^
      File "/lib/python3.14/site-packages/urllib3/poolmanager.py", line 457, in urlopen
        response = conn.urlopen(method, u.request_uri, **kw)
      File "/lib/python3.14/site-packages/urllib3/connectionpool.py", line 871, in urlopen
        return self.urlopen(
               ~~~~~~~~~~~~^
            method,
            ^^^^^^^
        ...<13 lines>...
            **response_kw,
            ^^^^^^^^^^^^^^
        )
        ^
      File "/lib/python3.14/site-packages/urllib3/connectionpool.py", line 871, in urlopen
        return self.urlopen(
               ~~~~~~~~~~~~^
            method,
            ^^^^^^^
        ...<13 lines>...
            **response_kw,
            ^^^^^^^^^^^^^^
        )
        ^
      File "/lib/python3.14/site-packages/urllib3/connectionpool.py", line 871, in urlopen
        return self.urlopen(
               ~~~~~~~~~~~~^
            method,
            ^^^^^^^
        ...<13 lines>...
            **response_kw,
            ^^^^^^^^^^^^^^
        )
        ^
      File "/lib/python3.14/site-packages/urllib3/connectionpool.py", line 841, in urlopen
        retries = retries.increment(
            method, url, error=new_e, _pool=self, _stacktrace=sys.exc_info()[2]
        )
      File "/lib/python3.14/site-packages/urllib3/util/retry.py", line 519, in increment
        raise MaxRetryError(_pool, url, reason) from reason  # type: ignore[arg-type]
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    urllib3.exceptions.MaxRetryError: HTTPConnectionPool(host='127.0.0.1', port=???): Max retries exceeded with url: / (Caused by ProtocolError('Connection aborted.', WasiErrorCode('Request failed with wasi http error ErrorCode_HttpRequestDenied')))
    "#,
    );
}

#[tokio::test(flavor = "multi_thread")]
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
        if let Some(mock) = case.mock(&server, NUMBER_OF_IMPLEMENTATIONS) {
            mock.mount(&server).await;
        }
        permissions.allow(case.matcher(&server));

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
    };
    let array_result = builder_result.finish();

    let udfs = python_udfs_with_permissions(CODE, permissions).await;
    assert_eq!(udfs.len(), NUMBER_OF_IMPLEMENTATIONS);

    for udf in udfs {
        println!("{}", udf.name());

        let array = udf
            .invoke_async_with_args(args.clone(), &ConfigOptions::default())
            .await
            .unwrap();
        assert_eq!(array.as_ref(), &array_result as &dyn Array);
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
    fn matcher(&self, server: &MockServer) -> Matcher {
        Matcher {
            method: self.method.try_into().unwrap(),
            host: server.address().ip().to_string().into(),
            port: server.address().port(),
        }
    }

    fn mock(&self, server: &MockServer, hits: usize) -> Option<Mock> {
        let Self {
            base,
            method,
            path,
            requ_headers,
            requ_body,
            resp,
        } = self;
        if base.is_some() {
            return None;
        }

        let TestResponse {
            status: resp_status,
            headers: resp_headers,
            body: resp_body,
        } = resp.clone().unwrap_or_default();

        let mut builder = Mock::given(matchers::method(method))
            .and(matchers::path(path.as_str()))
            .and(NoForbiddenHeaders::new(
                server.address().ip().to_string(),
                server.address().port(),
            ));

        for (k, v) in requ_headers {
            // Python `requests` sends this so we allow it but later drop it from the actual request.
            if k.as_str() == http::header::CONNECTION {
                continue;
            }

            builder = builder.and(matchers::headers(k.as_str(), v.to_vec()));
        }

        if let Some(requ_body) = requ_body {
            builder = builder.and(matchers::body_string(*requ_body));
        }

        let mut resp_template = ResponseTemplate::new(resp_status)
            .append_headers(resp_headers.iter().map(|(k, v)| (k, v.join(","))));
        if let Some(resp_body) = resp_body {
            resp_template = resp_template.set_body_string(resp_body);
        }

        let expect = if resp.is_ok() { hits as u64 } else { 0 };

        let mock = builder.respond_with(resp_template).expect(expect);
        Some(mock)
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

struct NoForbiddenHeaders {
    host: String,
    port: u16,
}

impl NoForbiddenHeaders {
    fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }
}

impl wiremock::Match for NoForbiddenHeaders {
    fn matches(&self, request: &wiremock::Request) -> bool {
        // "host" is part of the forbidden headers that the client is not supposed to use, but it is set by our own
        // host HTTP lib
        let Some(host_val) = request.headers.get(http::header::HOST) else {
            return false;
        };
        if host_val.to_str().expect("always a string") != format!("{}:{}", self.host, self.port) {
            return false;
        }

        DEFAULT_FORBIDDEN_HEADERS
            .iter()
            .filter(|h| *h != http::header::HOST)
            .all(|h| !request.headers.contains_key(h))
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
        Mock::given(matchers::any())
            .respond_with(ResponseTemplate::new(200).set_body_string("hello world!"))
            .expect(1)
            .mount(&server)
            .await;
        server
    });

    // deliberately use a runtime what we are going to throw away later to prevent tricks like `Handle::current`
    let udf = rt_tmp.block_on(async {
        let mut permissions = AllowCertainHttpRequests::new();
        permissions.allow(Matcher {
            method: http::Method::GET,
            host: server.address().ip().to_string().into(),
            port: server.address().port(),
        });

        let udfs = WasmScalarUdf::new(
            python_component().await,
            &WasmPermissions::new().with_http(permissions),
            rt_io.handle().clone(),
            CODE.to_owned(),
        )
        .await
        .unwrap();
        assert_eq!(udfs.len(), 1);
        udfs.into_iter().next().unwrap()
    });
    rt_tmp.shutdown_timeout(Duration::from_secs(1));

    let array = rt_cpu.block_on(async {
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(server.uri())))],
            arg_fields: vec![Arc::new(Field::new("uri", DataType::Utf8, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
        })
        .unwrap()
        .unwrap_array()
    });

    assert_eq!(
        array.as_ref(),
        &StringArray::from_iter([Some("hello world!".to_owned()),]) as &dyn Array,
    );
}
