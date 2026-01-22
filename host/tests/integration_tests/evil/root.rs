use std::sync::Arc;

use datafusion_execution::memory_pool::UnboundedMemoryPool;
use datafusion_udf_wasm_host::{VfsLimits, WasmPermissions, WasmScalarUdf};

use crate::integration_tests::evil::test_utils::{
    IO_RUNTIME, MEMORY_LIMIT, component, try_scalar_udfs, try_scalar_udfs_with_env,
    try_scalar_udfs_with_permissions,
};

#[tokio::test]
async fn test_invalid_entry() {
    let err = try_scalar_udfs("root::invalid_entry").await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    populate root FS from TAR
    caused by
    IO error: numeric field was not a number:  when getting cksum for foo
    ");
}

#[tokio::test]
async fn test_large_file() {
    let err = try_scalar_udfs_with_env("root::large_file", &[("limit", &MEMORY_LIMIT.to_string())])
        .await
        .unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    call root_fs_tar() method

    stderr:
    memory allocation of 10485760 bytes failed
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
    ");
}

#[tokio::test]
async fn test_many_files() {
    let component = component().await;

    let permissions = WasmPermissions::default()
        .with_env("EVIL".to_owned(), "root::many_files".to_owned())
        .with_env("limit".to_owned(), VfsLimits::default().inodes.to_string());

    let err = WasmScalarUdf::new(
        component,
        &permissions,
        IO_RUNTIME.handle().clone(),
        &(Arc::new(UnboundedMemoryPool::default()) as _),
        "".to_owned(),
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    populate root FS from TAR
    caused by
    IO error: inodes limit reached: limit<=10000 current==10000 requested+=1
    ");
}

#[tokio::test]
async fn test_not_tar() {
    let err = try_scalar_udfs("root::not_tar").await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    populate root FS from TAR
    caused by
    IO error: failed to read entire block
    ");
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
        @r"
    populate root FS from TAR
    caused by
    IO error: path limit reached: limit<=10 current==0 requested+=11
    ");
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
        @r"
    populate root FS from TAR
    caused by
    IO error: path segment limit reached: limit<=50 current==0 requested+=51
    ");
}

#[tokio::test]
async fn test_sparse() {
    let err = try_scalar_udfs("root::sparse").await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    populate root FS from TAR
    caused by
    IO error: Unsupported TAR content: GNUSparse @ huge
    ");
}

#[tokio::test]
async fn test_unsupported_entry() {
    let err = try_scalar_udfs("root::unsupported_entry")
        .await
        .unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    populate root FS from TAR
    caused by
    IO error: Unsupported TAR content: Symlink @ foo
    ");
}
