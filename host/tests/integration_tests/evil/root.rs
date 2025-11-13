use datafusion_udf_wasm_host::vfs::VfsLimits;

use crate::integration_tests::evil::test_utils::{try_scalar_udfs, try_scalar_udfs_with_env};

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
async fn test_unsupported_entry() {
    let err = try_scalar_udfs("root::unsupported_entry")
        .await
        .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"IO error: Unsupported TAR content: Symlink @ foo");
}
