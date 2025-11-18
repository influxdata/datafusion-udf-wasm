use datafusion_udf_wasm_host::{WasmPermissions, vfs::VfsLimits};

use crate::integration_tests::evil::test_utils::{
    try_scalar_udfs, try_scalar_udfs_with_env, try_scalar_udfs_with_permissions,
};

#[tokio::test]
async fn test_invalid_entry() {
    let err = try_scalar_udfs("root::invalid_entry").await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @"IO error: numeric field was not a number:  when getting cksum for foo");
}

#[tokio::test]
async fn test_many_files() {
    let err = try_scalar_udfs_with_env(
        "root::many_files",
        &[("limit", &VfsLimits::default().inodes.to_string())],
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"IO error: inodes limit reached: limit<=10000 current==10000 requested+=1");
}

#[tokio::test]
async fn test_not_tar() {
    let err = try_scalar_udfs("root::not_tar").await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @"IO error: failed to read entire block");
}

#[tokio::test]
async fn test_path_long() {
    let limit = 10;
    let permissions = WasmPermissions::new()
        .with_env("limit".to_owned(), limit.to_string())
        .with_vfs_limits(VfsLimits {
            max_path_length: limit,
            ..Default::default()
        });
    let err = try_scalar_udfs_with_permissions("root::path_long", permissions)
        .await
        .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"IO error: path limit reached: limit<=10 current==0 requested+=11");
}

#[tokio::test]
async fn test_path_segment_long() {
    let err = try_scalar_udfs_with_env(
        "root::path_long",
        &[(
            "limit",
            &VfsLimits::default().max_path_segment_size.to_string(),
        )],
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"IO error: path segment limit reached: limit<=50 current==0 requested+=51");
}

#[tokio::test]
async fn test_unsupported_entry() {
    let err = try_scalar_udfs("root::unsupported_entry")
        .await
        .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"IO error: Unsupported TAR content: Symlink @ foo");
}
