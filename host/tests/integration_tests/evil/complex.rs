use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_common::{DataFusionError, config::ConfigOptions};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, async_udf::AsyncScalarUDFImpl};
use datafusion_udf_wasm_host::{WasmPermissions, conversion::limits::TrustedDataLimits};

use crate::integration_tests::evil::test_utils::{try_scalar_udfs, try_scalar_udfs_with_env};

#[tokio::test]
async fn test_err_long_ctx() {
    let err = run_err_udf(
        "long_ctx",
        TrustedDataLimits::default().max_aux_string_length,
    )
    .await;

    insta::assert_snapshot!(
        err,
        @r"
    convert error from WASI
    caused by
    Resources exhausted: auxiliary string length: got=10001, limit=10000
    ",
    );
}

#[tokio::test]
async fn test_err_long_msg() {
    let err = run_err_udf(
        "long_msg",
        TrustedDataLimits::default().max_aux_string_length,
    )
    .await;

    insta::assert_snapshot!(
        err,
        @r"
    convert error from WASI
    caused by
    Resources exhausted: auxiliary string length: got=10001, limit=10000
    ",
    );
}

#[tokio::test]
async fn test_err_nested_ctx() {
    let err = run_err_udf("nested_ctx", TrustedDataLimits::default().max_depth as _).await;

    insta::assert_snapshot!(
        err,
        @r"
    convert error from WASI
    caused by
    Resources exhausted: data structure depth: limit=10
    ",
    );
}

#[tokio::test]
async fn test_many_inputs() {
    let err = try_scalar_udfs_with_env(
        "complex::many_inputs",
        &[(
            "limit",
            &TrustedDataLimits::default().max_complexity.to_string(),
        )],
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    type signature
    caused by
    exact signature
    caused by
    child 48
    caused by
    Resources exhausted: data structure complexity: limit=100
    ");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_type_dt_depth() {
    let err = run_return_type_udf("dt_depth").await;

    insta::assert_snapshot!(
        err,
        @r"
    field
    caused by
    field data type
    caused by
    field
    caused by
    field data type
    caused by
    field
    caused by
    field data type
    caused by
    field
    caused by
    field data type
    caused by
    Resources exhausted: data structure depth: limit=10
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_type_field_name() {
    let err = run_return_type_udf("field_name").await;

    insta::assert_snapshot!(
        err,
        @r"
    field
    caused by
    field name
    caused by
    Resources exhausted: identifier length: got=51, limit=50
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_type_field_md_key() {
    let err = run_return_type_udf("field_md_key").await;

    insta::assert_snapshot!(
        err,
        @r"
    field
    caused by
    field metadata
    caused by
    metadata key
    caused by
    Resources exhausted: identifier length: got=51, limit=50
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_type_field_md_value() {
    let err = run_return_type_udf("field_md_value").await;

    insta::assert_snapshot!(
        err,
        @r"
    field
    caused by
    field metadata
    caused by
    metadata value
    caused by
    Resources exhausted: auxiliary string length: got=10001, limit=10000
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_type_field_md_items() {
    let err = run_return_type_udf("field_md_items").await;

    insta::assert_snapshot!(
        err,
        @r"
    field
    caused by
    field metadata
    caused by
    Resources exhausted: data structure complexity: limit=100
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_value_dt_depth_array() {
    let err = run_return_value_udf("dt_depth_array").await;

    insta::assert_snapshot!(
        err,
        @r"
    array
    caused by
    field 0
    caused by
    field data type
    caused by
    field 0
    caused by
    field data type
    caused by
    field 0
    caused by
    field data type
    caused by
    field 0
    caused by
    Resources exhausted: data structure depth: limit=10
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_value_dt_depth_scalar() {
    let err = run_return_value_udf("dt_depth_scalar").await;

    insta::assert_snapshot!(
        err,
        @r"
    scalar
    caused by
    array
    caused by
    field 0
    caused by
    field data type
    caused by
    field 0
    caused by
    field data type
    caused by
    field 0
    caused by
    field data type
    caused by
    Resources exhausted: data structure depth: limit=10
    ",
    );
}

#[tokio::test]
async fn test_udf_long_name() {
    let err = try_scalar_udfs_with_env(
        "complex::udf_long_name",
        &[(
            "limit",
            &TrustedDataLimits::default()
                .max_identifier_length
                .to_string(),
        )],
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    UDF name
    caused by
    Resources exhausted: identifier length: got=51, limit=50
    ");
}

#[tokio::test]
async fn test_udfs_duplicate_names() {
    let err = try_scalar_udfs("complex::udfs_duplicate_names")
        .await
        .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"External error: non-unique UDF name: 'foo'");
}

#[tokio::test]
async fn test_udfs_many() {
    let err = try_scalar_udfs_with_env(
        "complex::udfs_many",
        &[("limit", &WasmPermissions::default().max_udfs().to_string())],
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"Resources exhausted: guest returned too many UDFs: got=21, limit=20");
}

/// Test UDF related to Error handling.
async fn run_err_udf(name: &'static str, limit: usize) -> DataFusionError {
    let udf = try_scalar_udfs_with_env("complex::error", &[("limit", &limit.to_string())])
        .await
        .unwrap()
        .into_iter()
        .find(|udf| udf.name() == name)
        .unwrap();

    udf.invoke_async_with_args(ScalarFunctionArgs {
        args: vec![],
        arg_fields: vec![],
        number_rows: 1,
        return_field: Arc::new(Field::new("r", DataType::Null, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
    .await
    .unwrap_err()
}

/// Test UDF related to return type information.
async fn run_return_type_udf(name: &'static str) -> DataFusionError {
    let TrustedDataLimits {
        max_identifier_length,
        max_aux_string_length,
        max_depth,
        max_complexity,
    } = TrustedDataLimits::default();

    let udf = try_scalar_udfs_with_env(
        "complex::return_type",
        &[
            ("max_identifier_length", &max_identifier_length.to_string()),
            ("max_aux_string_length", &max_aux_string_length.to_string()),
            ("max_depth", &max_depth.to_string()),
            ("max_complexity", &max_complexity.to_string()),
        ],
    )
    .await
    .unwrap()
    .into_iter()
    .find(|udf| udf.name() == name)
    .unwrap();

    udf.return_type(&[]).unwrap_err()
}

/// Test UDF related to return values.
async fn run_return_value_udf(name: &'static str) -> DataFusionError {
    let TrustedDataLimits {
        max_identifier_length,
        max_aux_string_length,
        max_depth,
        max_complexity,
    } = TrustedDataLimits::default();

    let udf = try_scalar_udfs_with_env(
        "complex::return_value",
        &[
            ("max_identifier_length", &max_identifier_length.to_string()),
            ("max_aux_string_length", &max_aux_string_length.to_string()),
            ("max_depth", &max_depth.to_string()),
            ("max_complexity", &max_complexity.to_string()),
        ],
    )
    .await
    .unwrap()
    .into_iter()
    .find(|udf| udf.name() == name)
    .unwrap();

    udf.invoke_async_with_args(ScalarFunctionArgs {
        args: vec![],
        arg_fields: vec![],
        number_rows: 1,
        return_field: Arc::new(Field::new("r", DataType::Null, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
    .await
    .unwrap_err()
}
