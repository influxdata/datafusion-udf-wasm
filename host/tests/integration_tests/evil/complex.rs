use datafusion_udf_wasm_host::{WasmPermissions, conversion::limits::TrustedDataLimits};

use crate::integration_tests::evil::test_utils::{try_scalar_udfs, try_scalar_udfs_with_env};

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
        @"Resources exhausted: identifier length: got=51, limit=50");
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
