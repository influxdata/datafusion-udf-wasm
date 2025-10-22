use std::sync::Arc;

use arrow::{
    array::{Array, StringArray, StringBuilder},
    datatypes::{DataType, Field},
};
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_udf_wasm_host::{
    WasmPermissions, WasmScalarUdf,
    http::{AllowCertainHttpRequests, HttpRequestValidator, Matcher},
};
use http::Method;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers};

use crate::integration_tests::{
    python::test_utils::{python_component, python_scalar_udf},
    test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_requests() {
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
        method: Method::GET,
        host: server.address().ip().to_string().into(),
        port: server.address().port(),
    });
    let udf = python_udf_with_permissions(CODE, permissions).await;

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(server.uri())))],
            arg_fields: vec![Arc::new(Field::new("uri", DataType::Utf8, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
        })
        .unwrap()
        .unwrap_array();

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
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(server.uri())))],
            arg_fields: vec![Arc::new(Field::new("uri", DataType::Utf8, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
        })
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
async fn test_urllib3_happy_path() {
    const CODE: &str = r#"
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

def perform_request(method: str, url: str, headers: str | None, body: str | None) -> str:
    resp = urllib3.request(
        method=method,
        url=url,
        headers=_headers_str_to_dict(headers),
        body=body,
    )

    resp_status = resp.status
    resp_body = f"'{resp.data.decode("utf-8")}'" if resp.data else "n/a"
    resp_headers = _headers_dict_to_str(resp.headers)

    return f"status={resp_status} headers={resp_headers} body={resp_body}"
"#;

    let cases = [
        TestCase {
            resp_body: Some("case_1"),
            ..Default::default()
        },
        TestCase {
            resp_body: Some("case_2"),
            method: "POST",
            ..Default::default()
        },
        TestCase {
            resp_body: Some("case_3"),
            path: "/foo",
            ..Default::default()
        },
        TestCase {
            resp_body: Some("case_4"),
            path: "/500",
            resp_status: 500,
            ..Default::default()
        },
        TestCase {
            resp_body: Some("case_5"),
            path: "/headers_in",
            requ_headers: &[("foo", &["bar"]), ("multi", &["some", "thing"])],
            ..Default::default()
        },
        TestCase {
            resp_body: Some("case_6"),
            path: "/headers_out",
            resp_headers: &[("foo", &["bar"]), ("multi", &["some", "thing"])],
            ..Default::default()
        },
        TestCase {
            resp_body: Some("case_7"),
            path: "/body_in",
            requ_body: Some("foo"),
            ..Default::default()
        },
        TestCase {
            resp_body: None,
            path: "/no_body_out",
            ..Default::default()
        },
    ];

    let server = MockServer::start().await;
    let mut permissions = AllowCertainHttpRequests::default();

    let mut builder_method = StringBuilder::new();
    let mut builder_url = StringBuilder::new();
    let mut builder_headers = StringBuilder::new();
    let mut builder_body = StringBuilder::new();
    let mut builder_result = StringBuilder::new();

    for case in &cases {
        case.mock().mount(&server).await;
        permissions.allow(case.matcher(&server));

        let TestCase {
            method,
            path,
            requ_headers,
            requ_body,
            resp_status,
            resp_headers,
            resp_body,
        } = case;

        let resp_headers = headers_to_string(resp_headers).unwrap_or_else(|| "n/a".to_owned());
        let resp_body = resp_body
            .map(|s| format!("'{s}'"))
            .unwrap_or_else(|| "n/a".to_owned());

        builder_method.append_value(method);
        builder_url.append_value(format!("{}{}", server.uri(), path));
        builder_headers.append_option(headers_to_string(requ_headers));
        builder_body.append_option(requ_body.map(|s| s.to_owned()));
        builder_result.append_value(format!(
            "status={resp_status} headers={resp_headers} body={resp_body}"
        ));
    }

    let udf = python_udf_with_permissions(CODE, permissions).await;

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
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
        })
        .unwrap()
        .unwrap_array();

    assert_eq!(array.as_ref(), &builder_result.finish() as &dyn Array,);
}

struct TestCase {
    method: &'static str,
    path: &'static str,
    requ_headers: &'static [(&'static str, &'static [&'static str])],
    requ_body: Option<&'static str>,
    resp_status: u16,
    resp_headers: &'static [(&'static str, &'static [&'static str])],
    resp_body: Option<&'static str>,
}

impl Default for TestCase {
    fn default() -> Self {
        Self {
            method: "GET",
            path: "/",
            requ_headers: &[],
            requ_body: None,
            resp_status: 200,
            resp_headers: &[],
            resp_body: None,
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

    fn mock(&self) -> Mock {
        let Self {
            method,
            path,
            requ_headers,
            requ_body,
            resp_status,
            resp_headers,
            resp_body,
        } = self;

        let mut builder = Mock::given(matchers::method(method)).and(matchers::path(*path));

        for (k, v) in *requ_headers {
            builder = builder.and(matchers::headers(*k, v.to_vec()));
        }

        if let Some(requ_body) = requ_body {
            builder = builder.and(matchers::body_string(*requ_body));
        }

        let mut resp = ResponseTemplate::new(*resp_status)
            .append_headers(resp_headers.iter().map(|(k, v)| (*k, v.join(","))));
        if let Some(resp_body) = resp_body {
            resp = resp.set_body_string(*resp_body);
        }

        builder.respond_with(resp).expect(1)
    }
}

fn headers_to_string(headers: &[(&str, &[&str])]) -> Option<String> {
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

async fn python_udf_with_permissions<V>(code: &'static str, permissions: V) -> WasmScalarUdf
where
    V: HttpRequestValidator,
{
    let udfs = WasmScalarUdf::new(
        python_component().await,
        &WasmPermissions::new().with_http(permissions),
        code.to_owned(),
    )
    .await
    .unwrap();
    assert_eq!(udfs.len(), 1);
    udfs.into_iter().next().unwrap()
}
