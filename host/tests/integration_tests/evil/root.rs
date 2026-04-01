use std::sync::Arc;

use datafusion_execution::memory_pool::UnboundedMemoryPool;
use datafusion_udf_wasm_host::{VfsLimits, WasmPermissions, WasmScalarUdf};
use regex::Regex;

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
    calling scalar_udfs() method failed

    stderr:

    thread '<unnamed>' (1) panicked at guests/evil/src/lib.rs:104:22:
    unknown evil: root::invalid_entry
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
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
    calling scalar_udfs() method failed

    stderr:

    thread '<unnamed>' (1) panicked at guests/evil/src/lib.rs:104:22:
    unknown evil: root::large_file
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
    calling scalar_udfs() method failed

    stderr:

    thread '<unnamed>' (1) panicked at guests/evil/src/lib.rs:104:22:
    unknown evil: root::many_files
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
    ");
}

#[tokio::test]
async fn test_not_tar() {
    let err = try_scalar_udfs("root::not_tar").await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    calling scalar_udfs() method failed

    stderr:

    thread '<unnamed>' (1) panicked at guests/evil/src/lib.rs:104:22:
    unknown evil: root::not_tar
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
    ");
}

#[tokio::test]
async fn test_tar_too_large() {
    let tar_size = MEMORY_LIMIT / 5;
    let permissions = WasmPermissions::new()
        .with_env("tar_size".to_owned(), tar_size.to_string())
        .with_vfs_limits(VfsLimits::default());
    let err = try_scalar_udfs_with_permissions("root::tar_too_large", permissions)
        .await
        .unwrap_err();

    // Sanitize sizes in the error message.
    let err = err.to_string();
    let err = Regex::new(r#"[0-9.]+ [KMB]B"#)
        .unwrap()
        .replace_all(&err, "<SIZE>");

    insta::assert_snapshot!(
        err,
        @r"
    calling scalar_udfs() method failed

    stderr:

    thread '<unnamed>' (1) panicked at guests/evil/src/lib.rs:104:22:
    unknown evil: root::tar_too_large
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
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
    calling scalar_udfs() method failed

    stderr:

    thread '<unnamed>' (1) panicked at guests/evil/src/lib.rs:104:22:
    unknown evil: root::path_long
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
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
    calling scalar_udfs() method failed

    stderr:

    thread '<unnamed>' (1) panicked at guests/evil/src/lib.rs:104:22:
    unknown evil: root::path_long
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
    ");
}

#[tokio::test]
async fn test_sparse() {
    let err = try_scalar_udfs("root::sparse").await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    calling scalar_udfs() method failed

    stderr:

    thread '<unnamed>' (1) panicked at guests/evil/src/lib.rs:104:22:
    unknown evil: root::sparse
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
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
    calling scalar_udfs() method failed

    stderr:

    thread '<unnamed>' (1) panicked at guests/evil/src/lib.rs:104:22:
    unknown evil: root::unsupported_entry
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    caused by
    External error: wasm trap: wasm `unreachable` instruction executed
    ");
}
