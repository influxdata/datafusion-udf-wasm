use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_common::{cast::as_string_array, config::ConfigOptions};
use datafusion_expr::{ScalarFunctionArgs, async_udf::AsyncScalarUDFImpl};
use datafusion_udf_wasm_host::{WasmPermissions, WasmScalarUdf};
use tokio::runtime::Handle;

use crate::integration_tests::{
    python::test_utils::python_component, test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_env() {
    assert_env_roundrip(&[]).await;
    assert_env_roundrip(&[("FOO", "BAR")]).await;
    assert_env_roundrip(&[("foo", "bar")]).await;
    assert_env_roundrip(&[("", "")]).await;
    assert_env_roundrip(&[("FOO", "BAR"), ("X", "Y")]).await;
}

pub(crate) async fn assert_env_roundrip(env: &[(&'static str, &'static str)]) {
    const CODE: &str = r#"
import os

def env() -> str:
    return ",".join((
        f"{k}:{v}"
        for k, v in os.environ.items()
    ))
"#;

    let component = python_component().await;

    let mut permissions = WasmPermissions::default();
    for (k, v) in env {
        permissions = permissions.with_env((*k).to_owned(), (*v).to_owned());
    }

    let udfs = WasmScalarUdf::new(component, &permissions, Handle::current(), CODE.to_owned())
        .await
        .unwrap();
    assert_eq!(udfs.len(), 1);
    let udf = udfs.into_iter().next().unwrap();

    let array = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();

    let actual = as_string_array(&array)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .unwrap();

    let expected = env
        .iter()
        .map(|(k, v)| format!("{k}:{v}"))
        .collect::<Vec<_>>();
    let expected = expected.join(",");

    assert_eq!(actual, expected);
}
