use std::sync::Arc;

use arrow::{
    array::{Array, StringBuilder},
    datatypes::{DataType, Field},
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers};

use crate::integration_tests::{
    python::test_utils::python_scalar_udf, test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_http() {
    const CODE: &str = r#"
import urllib3

def perform_request(method: str, url: str) -> str:
    resp = urllib3.request(method, url)

    resp_status = resp.status
    resp_body = resp.data.decode("utf-8")

    return f"status={resp_status} body='{resp_body}'"
"#;
    let udf = python_scalar_udf(CODE).await.unwrap();

    let cases = [
        TestCase {
            resp_body: "case_1",
            ..Default::default()
        },
        TestCase {
            resp_body: "case_2",
            method: "POST",
            ..Default::default()
        },
        TestCase {
            resp_body: "case_3",
            path: "/foo",
            ..Default::default()
        },
        TestCase {
            resp_body: "case_4",
            path: "/201",
            resp_status: 500,
            ..Default::default()
        },
    ];

    let server = MockServer::start().await;
    let mut builder_method = StringBuilder::new();
    let mut builder_url = StringBuilder::new();
    let mut builder_result = StringBuilder::new();
    for case in &cases {
        case.mock().mount(&server).await;

        let TestCase {
            method,
            path,
            resp_body,
            resp_status,
        } = case;
        builder_method.append_value(method);
        builder_url.append_value(format!("{}{}", server.uri(), path));
        builder_result.append_value(format!("status={resp_status} body='{resp_body}'"));
    }

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(builder_method.finish())),
                ColumnarValue::Array(Arc::new(builder_url.finish())),
            ],
            arg_fields: vec![
                Arc::new(Field::new("method", DataType::Utf8, true)),
                Arc::new(Field::new("url", DataType::Utf8, true)),
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
    resp_body: &'static str,
    resp_status: u16,
}

impl Default for TestCase {
    fn default() -> Self {
        Self {
            method: "GET",
            path: "/",
            resp_body: "",
            resp_status: 200,
        }
    }
}

impl TestCase {
    fn mock(&self) -> Mock {
        let Self {
            method,
            path,
            resp_body,
            resp_status,
        } = self;

        Mock::given(matchers::method(method))
            .and(matchers::path(*path))
            .respond_with(ResponseTemplate::new(*resp_status).set_body_string(*resp_body))
            .expect(1)
    }
}
