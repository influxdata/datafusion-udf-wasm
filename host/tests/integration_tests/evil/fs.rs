use arrow::{
    array::StringArray,
    datatypes::{DataType, Field},
};
use datafusion_common::{cast::as_string_array, config::ConfigOptions};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, async_udf::AsyncScalarUDFImpl,
};
use datafusion_udf_wasm_host::WasmScalarUdf;
use std::{fmt::Write, sync::Arc};

use crate::integration_tests::{evil::test_utils::try_scalar_udfs, test_utils::ColumnarValueExt};

const PATHS: &[&str] = &[
    // NOTE: it seems that the WASI guest transforms `` (empty string) into `.` before it even reaches the host
    "",
    ".",
    "..",
    "/",
    "/dev",
    "/etc",
    "/etc/group",
    "/etc/passwd",
    "/etc/shadow",
    "/proc",
    "/proc/self",
    "/sys",
    "/tmp",
];

#[tokio::test]
async fn test_canonicalize() {
    let udf = udf("canonicalize").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | ERR: operation not supported on this platform
    .           | ERR: operation not supported on this platform
    ..          | ERR: operation not supported on this platform
    /           | ERR: operation not supported on this platform
    /dev        | ERR: operation not supported on this platform
    /etc        | ERR: operation not supported on this platform
    /etc/group  | ERR: operation not supported on this platform
    /etc/passwd | ERR: operation not supported on this platform
    /etc/shadow | ERR: operation not supported on this platform
    /proc       | ERR: operation not supported on this platform
    /proc/self  | ERR: operation not supported on this platform
    /sys        | ERR: operation not supported on this platform
    /tmp        | ERR: operation not supported on this platform
    ",
    );
}

#[tokio::test]
async fn test_copy() {
    let udf = udf("copy").await;

    insta::assert_snapshot!(
        run_2(&udf).await,
        @r"
               |             | ERR: Read-only file system (os error 69)
               | .           | ERR: Read-only file system (os error 69)
               | ..          | ERR: Read-only file system (os error 69)
               | /           | ERR: Read-only file system (os error 69)
               | /dev        | ERR: Read-only file system (os error 69)
               | /etc        | ERR: Read-only file system (os error 69)
               | /etc/group  | ERR: Read-only file system (os error 69)
               | /etc/passwd | ERR: Read-only file system (os error 69)
               | /etc/shadow | ERR: Read-only file system (os error 69)
               | /proc       | ERR: Read-only file system (os error 69)
               | /proc/self  | ERR: Read-only file system (os error 69)
               | /sys        | ERR: Read-only file system (os error 69)
               | /tmp        | ERR: Read-only file system (os error 69)
    .          |             | ERR: Read-only file system (os error 69)
    .          | .           | ERR: Read-only file system (os error 69)
    .          | ..          | ERR: Read-only file system (os error 69)
    .          | /           | ERR: Read-only file system (os error 69)
    .          | /dev        | ERR: Read-only file system (os error 69)
    .          | /etc        | ERR: Read-only file system (os error 69)
    .          | /etc/group  | ERR: Read-only file system (os error 69)
    .          | /etc/passwd | ERR: Read-only file system (os error 69)
    .          | /etc/shadow | ERR: Read-only file system (os error 69)
    .          | /proc       | ERR: Read-only file system (os error 69)
    .          | /proc/self  | ERR: Read-only file system (os error 69)
    .          | /sys        | ERR: Read-only file system (os error 69)
    .          | /tmp        | ERR: Read-only file system (os error 69)
    ..         |             | ERR: Read-only file system (os error 69)
    ..         | .           | ERR: Read-only file system (os error 69)
    ..         | ..          | ERR: Read-only file system (os error 69)
    ..         | /           | ERR: Read-only file system (os error 69)
    ..         | /dev        | ERR: Read-only file system (os error 69)
    ..         | /etc        | ERR: Read-only file system (os error 69)
    ..         | /etc/group  | ERR: Read-only file system (os error 69)
    ..         | /etc/passwd | ERR: Read-only file system (os error 69)
    ..         | /etc/shadow | ERR: Read-only file system (os error 69)
    ..         | /proc       | ERR: Read-only file system (os error 69)
    ..         | /proc/self  | ERR: Read-only file system (os error 69)
    ..         | /sys        | ERR: Read-only file system (os error 69)
    ..         | /tmp        | ERR: Read-only file system (os error 69)
    /          |             | ERR: Read-only file system (os error 69)
    /          | .           | ERR: Read-only file system (os error 69)
    /          | ..          | ERR: Read-only file system (os error 69)
    /          | /           | ERR: Read-only file system (os error 69)
    /          | /dev        | ERR: Read-only file system (os error 69)
    /          | /etc        | ERR: Read-only file system (os error 69)
    /          | /etc/group  | ERR: Read-only file system (os error 69)
    /          | /etc/passwd | ERR: Read-only file system (os error 69)
    /          | /etc/shadow | ERR: Read-only file system (os error 69)
    /          | /proc       | ERR: Read-only file system (os error 69)
    /          | /proc/self  | ERR: Read-only file system (os error 69)
    /          | /sys        | ERR: Read-only file system (os error 69)
    /          | /tmp        | ERR: Read-only file system (os error 69)
    /dev       |             | ERR: No such file or directory (os error 44)
    /dev       | .           | ERR: No such file or directory (os error 44)
    /dev       | ..          | ERR: No such file or directory (os error 44)
    /dev       | /           | ERR: No such file or directory (os error 44)
    /dev       | /dev        | ERR: No such file or directory (os error 44)
    /dev       | /etc        | ERR: No such file or directory (os error 44)
    /dev       | /etc/group  | ERR: No such file or directory (os error 44)
    /dev       | /etc/passwd | ERR: No such file or directory (os error 44)
    /dev       | /etc/shadow | ERR: No such file or directory (os error 44)
    /dev       | /proc       | ERR: No such file or directory (os error 44)
    /dev       | /proc/self  | ERR: No such file or directory (os error 44)
    /dev       | /sys        | ERR: No such file or directory (os error 44)
    /dev       | /tmp        | ERR: No such file or directory (os error 44)
    /etc       |             | ERR: No such file or directory (os error 44)
    /etc       | .           | ERR: No such file or directory (os error 44)
    /etc       | ..          | ERR: No such file or directory (os error 44)
    /etc       | /           | ERR: No such file or directory (os error 44)
    /etc       | /dev        | ERR: No such file or directory (os error 44)
    /etc       | /etc        | ERR: No such file or directory (os error 44)
    /etc       | /etc/group  | ERR: No such file or directory (os error 44)
    /etc       | /etc/passwd | ERR: No such file or directory (os error 44)
    /etc       | /etc/shadow | ERR: No such file or directory (os error 44)
    /etc       | /proc       | ERR: No such file or directory (os error 44)
    /etc       | /proc/self  | ERR: No such file or directory (os error 44)
    /etc       | /sys        | ERR: No such file or directory (os error 44)
    /etc       | /tmp        | ERR: No such file or directory (os error 44)
    /etc/group |             | ERR: No such file or directory (os error 44)
    /etc/group | .           | ERR: No such file or directory (os error 44)
    /etc/group | ..          | ERR: No such file or directory (os error 44)
    /etc/group | /           | ERR: No such file or directory (os error 44)
    /etc/group | /dev        | ERR: No such file or directory (os error 44)
    /etc/group | /etc        | ERR: No such file or directory (os error 44)
    /etc/group | /etc/group  | ERR: No such file or directory (os error 44)
    /etc/group | /etc/passwd | ERR: No such file or directory (os error 44)
    /etc/group | /etc/shadow | ERR: No such file or directory (os error 44)
    /etc/group | /proc       | ERR: No such file or directory (os error 44)
    /etc/group | /proc/self  | ERR: No such file or directory (os error 44)
    /etc/group | /sys        | ERR: No such file or directory (os error 44)
    /etc/group | /tmp        | ERR: No such file or directory (os error 44)
    /etc/passwd|             | ERR: No such file or directory (os error 44)
    /etc/passwd| .           | ERR: No such file or directory (os error 44)
    /etc/passwd| ..          | ERR: No such file or directory (os error 44)
    /etc/passwd| /           | ERR: No such file or directory (os error 44)
    /etc/passwd| /dev        | ERR: No such file or directory (os error 44)
    /etc/passwd| /etc        | ERR: No such file or directory (os error 44)
    /etc/passwd| /etc/group  | ERR: No such file or directory (os error 44)
    /etc/passwd| /etc/passwd | ERR: No such file or directory (os error 44)
    /etc/passwd| /etc/shadow | ERR: No such file or directory (os error 44)
    /etc/passwd| /proc       | ERR: No such file or directory (os error 44)
    /etc/passwd| /proc/self  | ERR: No such file or directory (os error 44)
    /etc/passwd| /sys        | ERR: No such file or directory (os error 44)
    /etc/passwd| /tmp        | ERR: No such file or directory (os error 44)
    /etc/shadow|             | ERR: No such file or directory (os error 44)
    /etc/shadow| .           | ERR: No such file or directory (os error 44)
    /etc/shadow| ..          | ERR: No such file or directory (os error 44)
    /etc/shadow| /           | ERR: No such file or directory (os error 44)
    /etc/shadow| /dev        | ERR: No such file or directory (os error 44)
    /etc/shadow| /etc        | ERR: No such file or directory (os error 44)
    /etc/shadow| /etc/group  | ERR: No such file or directory (os error 44)
    /etc/shadow| /etc/passwd | ERR: No such file or directory (os error 44)
    /etc/shadow| /etc/shadow | ERR: No such file or directory (os error 44)
    /etc/shadow| /proc       | ERR: No such file or directory (os error 44)
    /etc/shadow| /proc/self  | ERR: No such file or directory (os error 44)
    /etc/shadow| /sys        | ERR: No such file or directory (os error 44)
    /etc/shadow| /tmp        | ERR: No such file or directory (os error 44)
    /proc      |             | ERR: No such file or directory (os error 44)
    /proc      | .           | ERR: No such file or directory (os error 44)
    /proc      | ..          | ERR: No such file or directory (os error 44)
    /proc      | /           | ERR: No such file or directory (os error 44)
    /proc      | /dev        | ERR: No such file or directory (os error 44)
    /proc      | /etc        | ERR: No such file or directory (os error 44)
    /proc      | /etc/group  | ERR: No such file or directory (os error 44)
    /proc      | /etc/passwd | ERR: No such file or directory (os error 44)
    /proc      | /etc/shadow | ERR: No such file or directory (os error 44)
    /proc      | /proc       | ERR: No such file or directory (os error 44)
    /proc      | /proc/self  | ERR: No such file or directory (os error 44)
    /proc      | /sys        | ERR: No such file or directory (os error 44)
    /proc      | /tmp        | ERR: No such file or directory (os error 44)
    /proc/self |             | ERR: No such file or directory (os error 44)
    /proc/self | .           | ERR: No such file or directory (os error 44)
    /proc/self | ..          | ERR: No such file or directory (os error 44)
    /proc/self | /           | ERR: No such file or directory (os error 44)
    /proc/self | /dev        | ERR: No such file or directory (os error 44)
    /proc/self | /etc        | ERR: No such file or directory (os error 44)
    /proc/self | /etc/group  | ERR: No such file or directory (os error 44)
    /proc/self | /etc/passwd | ERR: No such file or directory (os error 44)
    /proc/self | /etc/shadow | ERR: No such file or directory (os error 44)
    /proc/self | /proc       | ERR: No such file or directory (os error 44)
    /proc/self | /proc/self  | ERR: No such file or directory (os error 44)
    /proc/self | /sys        | ERR: No such file or directory (os error 44)
    /proc/self | /tmp        | ERR: No such file or directory (os error 44)
    /sys       |             | ERR: No such file or directory (os error 44)
    /sys       | .           | ERR: No such file or directory (os error 44)
    /sys       | ..          | ERR: No such file or directory (os error 44)
    /sys       | /           | ERR: No such file or directory (os error 44)
    /sys       | /dev        | ERR: No such file or directory (os error 44)
    /sys       | /etc        | ERR: No such file or directory (os error 44)
    /sys       | /etc/group  | ERR: No such file or directory (os error 44)
    /sys       | /etc/passwd | ERR: No such file or directory (os error 44)
    /sys       | /etc/shadow | ERR: No such file or directory (os error 44)
    /sys       | /proc       | ERR: No such file or directory (os error 44)
    /sys       | /proc/self  | ERR: No such file or directory (os error 44)
    /sys       | /sys        | ERR: No such file or directory (os error 44)
    /sys       | /tmp        | ERR: No such file or directory (os error 44)
    /tmp       |             | ERR: No such file or directory (os error 44)
    /tmp       | .           | ERR: No such file or directory (os error 44)
    /tmp       | ..          | ERR: No such file or directory (os error 44)
    /tmp       | /           | ERR: No such file or directory (os error 44)
    /tmp       | /dev        | ERR: No such file or directory (os error 44)
    /tmp       | /etc        | ERR: No such file or directory (os error 44)
    /tmp       | /etc/group  | ERR: No such file or directory (os error 44)
    /tmp       | /etc/passwd | ERR: No such file or directory (os error 44)
    /tmp       | /etc/shadow | ERR: No such file or directory (os error 44)
    /tmp       | /proc       | ERR: No such file or directory (os error 44)
    /tmp       | /proc/self  | ERR: No such file or directory (os error 44)
    /tmp       | /sys        | ERR: No such file or directory (os error 44)
    /tmp       | /tmp        | ERR: No such file or directory (os error 44)
    ",
    );
}

#[tokio::test]
async fn test_create_dir() {
    let udf = udf("create_dir").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | ERR: Read-only file system (os error 69)
    .           | ERR: Read-only file system (os error 69)
    ..          | ERR: Read-only file system (os error 69)
    /           | ERR: Read-only file system (os error 69)
    /dev        | ERR: Read-only file system (os error 69)
    /etc        | ERR: Read-only file system (os error 69)
    /etc/group  | ERR: Read-only file system (os error 69)
    /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/shadow | ERR: Read-only file system (os error 69)
    /proc       | ERR: Read-only file system (os error 69)
    /proc/self  | ERR: Read-only file system (os error 69)
    /sys        | ERR: Read-only file system (os error 69)
    /tmp        | ERR: Read-only file system (os error 69)
    ",
    );
}

#[tokio::test]
async fn test_exists() {
    let udf = udf("exists").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | OK: true
    .           | OK: true
    ..          | OK: true
    /           | OK: true
    /dev        | OK: false
    /etc        | OK: false
    /etc/group  | OK: false
    /etc/passwd | OK: false
    /etc/shadow | OK: false
    /proc       | OK: false
    /proc/self  | OK: false
    /sys        | OK: false
    /tmp        | OK: false
    ",
    );
}

#[tokio::test]
async fn test_hard_link() {
    let udf = udf("hard_link").await;

    insta::assert_snapshot!(
        run_2(&udf).await,
        @r"
               |             | ERR: Read-only file system (os error 69)
               | .           | ERR: Read-only file system (os error 69)
               | ..          | ERR: Read-only file system (os error 69)
               | /           | ERR: Read-only file system (os error 69)
               | /dev        | ERR: Read-only file system (os error 69)
               | /etc        | ERR: Read-only file system (os error 69)
               | /etc/group  | ERR: Read-only file system (os error 69)
               | /etc/passwd | ERR: Read-only file system (os error 69)
               | /etc/shadow | ERR: Read-only file system (os error 69)
               | /proc       | ERR: Read-only file system (os error 69)
               | /proc/self  | ERR: Read-only file system (os error 69)
               | /sys        | ERR: Read-only file system (os error 69)
               | /tmp        | ERR: Read-only file system (os error 69)
    .          |             | ERR: Read-only file system (os error 69)
    .          | .           | ERR: Read-only file system (os error 69)
    .          | ..          | ERR: Read-only file system (os error 69)
    .          | /           | ERR: Read-only file system (os error 69)
    .          | /dev        | ERR: Read-only file system (os error 69)
    .          | /etc        | ERR: Read-only file system (os error 69)
    .          | /etc/group  | ERR: Read-only file system (os error 69)
    .          | /etc/passwd | ERR: Read-only file system (os error 69)
    .          | /etc/shadow | ERR: Read-only file system (os error 69)
    .          | /proc       | ERR: Read-only file system (os error 69)
    .          | /proc/self  | ERR: Read-only file system (os error 69)
    .          | /sys        | ERR: Read-only file system (os error 69)
    .          | /tmp        | ERR: Read-only file system (os error 69)
    ..         |             | ERR: Read-only file system (os error 69)
    ..         | .           | ERR: Read-only file system (os error 69)
    ..         | ..          | ERR: Read-only file system (os error 69)
    ..         | /           | ERR: Read-only file system (os error 69)
    ..         | /dev        | ERR: Read-only file system (os error 69)
    ..         | /etc        | ERR: Read-only file system (os error 69)
    ..         | /etc/group  | ERR: Read-only file system (os error 69)
    ..         | /etc/passwd | ERR: Read-only file system (os error 69)
    ..         | /etc/shadow | ERR: Read-only file system (os error 69)
    ..         | /proc       | ERR: Read-only file system (os error 69)
    ..         | /proc/self  | ERR: Read-only file system (os error 69)
    ..         | /sys        | ERR: Read-only file system (os error 69)
    ..         | /tmp        | ERR: Read-only file system (os error 69)
    /          |             | ERR: Read-only file system (os error 69)
    /          | .           | ERR: Read-only file system (os error 69)
    /          | ..          | ERR: Read-only file system (os error 69)
    /          | /           | ERR: Read-only file system (os error 69)
    /          | /dev        | ERR: Read-only file system (os error 69)
    /          | /etc        | ERR: Read-only file system (os error 69)
    /          | /etc/group  | ERR: Read-only file system (os error 69)
    /          | /etc/passwd | ERR: Read-only file system (os error 69)
    /          | /etc/shadow | ERR: Read-only file system (os error 69)
    /          | /proc       | ERR: Read-only file system (os error 69)
    /          | /proc/self  | ERR: Read-only file system (os error 69)
    /          | /sys        | ERR: Read-only file system (os error 69)
    /          | /tmp        | ERR: Read-only file system (os error 69)
    /dev       |             | ERR: Read-only file system (os error 69)
    /dev       | .           | ERR: Read-only file system (os error 69)
    /dev       | ..          | ERR: Read-only file system (os error 69)
    /dev       | /           | ERR: Read-only file system (os error 69)
    /dev       | /dev        | ERR: Read-only file system (os error 69)
    /dev       | /etc        | ERR: Read-only file system (os error 69)
    /dev       | /etc/group  | ERR: Read-only file system (os error 69)
    /dev       | /etc/passwd | ERR: Read-only file system (os error 69)
    /dev       | /etc/shadow | ERR: Read-only file system (os error 69)
    /dev       | /proc       | ERR: Read-only file system (os error 69)
    /dev       | /proc/self  | ERR: Read-only file system (os error 69)
    /dev       | /sys        | ERR: Read-only file system (os error 69)
    /dev       | /tmp        | ERR: Read-only file system (os error 69)
    /etc       |             | ERR: Read-only file system (os error 69)
    /etc       | .           | ERR: Read-only file system (os error 69)
    /etc       | ..          | ERR: Read-only file system (os error 69)
    /etc       | /           | ERR: Read-only file system (os error 69)
    /etc       | /dev        | ERR: Read-only file system (os error 69)
    /etc       | /etc        | ERR: Read-only file system (os error 69)
    /etc       | /etc/group  | ERR: Read-only file system (os error 69)
    /etc       | /etc/passwd | ERR: Read-only file system (os error 69)
    /etc       | /etc/shadow | ERR: Read-only file system (os error 69)
    /etc       | /proc       | ERR: Read-only file system (os error 69)
    /etc       | /proc/self  | ERR: Read-only file system (os error 69)
    /etc       | /sys        | ERR: Read-only file system (os error 69)
    /etc       | /tmp        | ERR: Read-only file system (os error 69)
    /etc/group |             | ERR: Read-only file system (os error 69)
    /etc/group | .           | ERR: Read-only file system (os error 69)
    /etc/group | ..          | ERR: Read-only file system (os error 69)
    /etc/group | /           | ERR: Read-only file system (os error 69)
    /etc/group | /dev        | ERR: Read-only file system (os error 69)
    /etc/group | /etc        | ERR: Read-only file system (os error 69)
    /etc/group | /etc/group  | ERR: Read-only file system (os error 69)
    /etc/group | /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/group | /etc/shadow | ERR: Read-only file system (os error 69)
    /etc/group | /proc       | ERR: Read-only file system (os error 69)
    /etc/group | /proc/self  | ERR: Read-only file system (os error 69)
    /etc/group | /sys        | ERR: Read-only file system (os error 69)
    /etc/group | /tmp        | ERR: Read-only file system (os error 69)
    /etc/passwd|             | ERR: Read-only file system (os error 69)
    /etc/passwd| .           | ERR: Read-only file system (os error 69)
    /etc/passwd| ..          | ERR: Read-only file system (os error 69)
    /etc/passwd| /           | ERR: Read-only file system (os error 69)
    /etc/passwd| /dev        | ERR: Read-only file system (os error 69)
    /etc/passwd| /etc        | ERR: Read-only file system (os error 69)
    /etc/passwd| /etc/group  | ERR: Read-only file system (os error 69)
    /etc/passwd| /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/passwd| /etc/shadow | ERR: Read-only file system (os error 69)
    /etc/passwd| /proc       | ERR: Read-only file system (os error 69)
    /etc/passwd| /proc/self  | ERR: Read-only file system (os error 69)
    /etc/passwd| /sys        | ERR: Read-only file system (os error 69)
    /etc/passwd| /tmp        | ERR: Read-only file system (os error 69)
    /etc/shadow|             | ERR: Read-only file system (os error 69)
    /etc/shadow| .           | ERR: Read-only file system (os error 69)
    /etc/shadow| ..          | ERR: Read-only file system (os error 69)
    /etc/shadow| /           | ERR: Read-only file system (os error 69)
    /etc/shadow| /dev        | ERR: Read-only file system (os error 69)
    /etc/shadow| /etc        | ERR: Read-only file system (os error 69)
    /etc/shadow| /etc/group  | ERR: Read-only file system (os error 69)
    /etc/shadow| /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/shadow| /etc/shadow | ERR: Read-only file system (os error 69)
    /etc/shadow| /proc       | ERR: Read-only file system (os error 69)
    /etc/shadow| /proc/self  | ERR: Read-only file system (os error 69)
    /etc/shadow| /sys        | ERR: Read-only file system (os error 69)
    /etc/shadow| /tmp        | ERR: Read-only file system (os error 69)
    /proc      |             | ERR: Read-only file system (os error 69)
    /proc      | .           | ERR: Read-only file system (os error 69)
    /proc      | ..          | ERR: Read-only file system (os error 69)
    /proc      | /           | ERR: Read-only file system (os error 69)
    /proc      | /dev        | ERR: Read-only file system (os error 69)
    /proc      | /etc        | ERR: Read-only file system (os error 69)
    /proc      | /etc/group  | ERR: Read-only file system (os error 69)
    /proc      | /etc/passwd | ERR: Read-only file system (os error 69)
    /proc      | /etc/shadow | ERR: Read-only file system (os error 69)
    /proc      | /proc       | ERR: Read-only file system (os error 69)
    /proc      | /proc/self  | ERR: Read-only file system (os error 69)
    /proc      | /sys        | ERR: Read-only file system (os error 69)
    /proc      | /tmp        | ERR: Read-only file system (os error 69)
    /proc/self |             | ERR: Read-only file system (os error 69)
    /proc/self | .           | ERR: Read-only file system (os error 69)
    /proc/self | ..          | ERR: Read-only file system (os error 69)
    /proc/self | /           | ERR: Read-only file system (os error 69)
    /proc/self | /dev        | ERR: Read-only file system (os error 69)
    /proc/self | /etc        | ERR: Read-only file system (os error 69)
    /proc/self | /etc/group  | ERR: Read-only file system (os error 69)
    /proc/self | /etc/passwd | ERR: Read-only file system (os error 69)
    /proc/self | /etc/shadow | ERR: Read-only file system (os error 69)
    /proc/self | /proc       | ERR: Read-only file system (os error 69)
    /proc/self | /proc/self  | ERR: Read-only file system (os error 69)
    /proc/self | /sys        | ERR: Read-only file system (os error 69)
    /proc/self | /tmp        | ERR: Read-only file system (os error 69)
    /sys       |             | ERR: Read-only file system (os error 69)
    /sys       | .           | ERR: Read-only file system (os error 69)
    /sys       | ..          | ERR: Read-only file system (os error 69)
    /sys       | /           | ERR: Read-only file system (os error 69)
    /sys       | /dev        | ERR: Read-only file system (os error 69)
    /sys       | /etc        | ERR: Read-only file system (os error 69)
    /sys       | /etc/group  | ERR: Read-only file system (os error 69)
    /sys       | /etc/passwd | ERR: Read-only file system (os error 69)
    /sys       | /etc/shadow | ERR: Read-only file system (os error 69)
    /sys       | /proc       | ERR: Read-only file system (os error 69)
    /sys       | /proc/self  | ERR: Read-only file system (os error 69)
    /sys       | /sys        | ERR: Read-only file system (os error 69)
    /sys       | /tmp        | ERR: Read-only file system (os error 69)
    /tmp       |             | ERR: Read-only file system (os error 69)
    /tmp       | .           | ERR: Read-only file system (os error 69)
    /tmp       | ..          | ERR: Read-only file system (os error 69)
    /tmp       | /           | ERR: Read-only file system (os error 69)
    /tmp       | /dev        | ERR: Read-only file system (os error 69)
    /tmp       | /etc        | ERR: Read-only file system (os error 69)
    /tmp       | /etc/group  | ERR: Read-only file system (os error 69)
    /tmp       | /etc/passwd | ERR: Read-only file system (os error 69)
    /tmp       | /etc/shadow | ERR: Read-only file system (os error 69)
    /tmp       | /proc       | ERR: Read-only file system (os error 69)
    /tmp       | /proc/self  | ERR: Read-only file system (os error 69)
    /tmp       | /sys        | ERR: Read-only file system (os error 69)
    /tmp       | /tmp        | ERR: Read-only file system (os error 69)
    ",
    );
}

#[tokio::test]
async fn test_metadata() {
    let udf = udf("metadata").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | OK: got data
    .           | OK: got data
    ..          | OK: got data
    /           | OK: got data
    /dev        | ERR: No such file or directory (os error 44)
    /etc        | ERR: No such file or directory (os error 44)
    /etc/group  | ERR: No such file or directory (os error 44)
    /etc/passwd | ERR: No such file or directory (os error 44)
    /etc/shadow | ERR: No such file or directory (os error 44)
    /proc       | ERR: No such file or directory (os error 44)
    /proc/self  | ERR: No such file or directory (os error 44)
    /sys        | ERR: No such file or directory (os error 44)
    /tmp        | ERR: No such file or directory (os error 44)
    ",
    );
}

#[tokio::test]
async fn test_open_append() {
    let udf = udf("open_append").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | ERR: Read-only file system (os error 69)
    .           | ERR: Read-only file system (os error 69)
    ..          | ERR: Read-only file system (os error 69)
    /           | ERR: Read-only file system (os error 69)
    /dev        | ERR: Read-only file system (os error 69)
    /etc        | ERR: Read-only file system (os error 69)
    /etc/group  | ERR: Read-only file system (os error 69)
    /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/shadow | ERR: Read-only file system (os error 69)
    /proc       | ERR: Read-only file system (os error 69)
    /proc/self  | ERR: Read-only file system (os error 69)
    /sys        | ERR: Read-only file system (os error 69)
    /tmp        | ERR: Read-only file system (os error 69)
    ",
    );
}

#[tokio::test]
async fn test_open_create() {
    let udf = udf("open_create").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | ERR: Read-only file system (os error 69)
    .           | ERR: Read-only file system (os error 69)
    ..          | ERR: Read-only file system (os error 69)
    /           | ERR: Read-only file system (os error 69)
    /dev        | ERR: Read-only file system (os error 69)
    /etc        | ERR: Read-only file system (os error 69)
    /etc/group  | ERR: Read-only file system (os error 69)
    /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/shadow | ERR: Read-only file system (os error 69)
    /proc       | ERR: Read-only file system (os error 69)
    /proc/self  | ERR: Read-only file system (os error 69)
    /sys        | ERR: Read-only file system (os error 69)
    /tmp        | ERR: Read-only file system (os error 69)
    ",
    );
}

#[tokio::test]
async fn test_open_create_new() {
    let udf = udf("open_create_new").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | ERR: Read-only file system (os error 69)
    .           | ERR: Read-only file system (os error 69)
    ..          | ERR: Read-only file system (os error 69)
    /           | ERR: Read-only file system (os error 69)
    /dev        | ERR: Read-only file system (os error 69)
    /etc        | ERR: Read-only file system (os error 69)
    /etc/group  | ERR: Read-only file system (os error 69)
    /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/shadow | ERR: Read-only file system (os error 69)
    /proc       | ERR: Read-only file system (os error 69)
    /proc/self  | ERR: Read-only file system (os error 69)
    /sys        | ERR: Read-only file system (os error 69)
    /tmp        | ERR: Read-only file system (os error 69)
    ",
    );
}

#[tokio::test]
async fn test_open_read() {
    let udf = udf("open_read").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | OK: opened
    .           | OK: opened
    ..          | OK: opened
    /           | OK: opened
    /dev        | ERR: No such file or directory (os error 44)
    /etc        | ERR: No such file or directory (os error 44)
    /etc/group  | ERR: No such file or directory (os error 44)
    /etc/passwd | ERR: No such file or directory (os error 44)
    /etc/shadow | ERR: No such file or directory (os error 44)
    /proc       | ERR: No such file or directory (os error 44)
    /proc/self  | ERR: No such file or directory (os error 44)
    /sys        | ERR: No such file or directory (os error 44)
    /tmp        | ERR: No such file or directory (os error 44)
    ",
    );
}

#[tokio::test]
async fn test_open_truncate() {
    let udf = udf("open_truncate").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | ERR: Read-only file system (os error 69)
    .           | ERR: Read-only file system (os error 69)
    ..          | ERR: Read-only file system (os error 69)
    /           | ERR: Read-only file system (os error 69)
    /dev        | ERR: Read-only file system (os error 69)
    /etc        | ERR: Read-only file system (os error 69)
    /etc/group  | ERR: Read-only file system (os error 69)
    /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/shadow | ERR: Read-only file system (os error 69)
    /proc       | ERR: Read-only file system (os error 69)
    /proc/self  | ERR: Read-only file system (os error 69)
    /sys        | ERR: Read-only file system (os error 69)
    /tmp        | ERR: Read-only file system (os error 69)
    ",
    );
}

#[tokio::test]
async fn test_open_write() {
    let udf = udf("open_write").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | ERR: Read-only file system (os error 69)
    .           | ERR: Read-only file system (os error 69)
    ..          | ERR: Read-only file system (os error 69)
    /           | ERR: Read-only file system (os error 69)
    /dev        | ERR: Read-only file system (os error 69)
    /etc        | ERR: Read-only file system (os error 69)
    /etc/group  | ERR: Read-only file system (os error 69)
    /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/shadow | ERR: Read-only file system (os error 69)
    /proc       | ERR: Read-only file system (os error 69)
    /proc/self  | ERR: Read-only file system (os error 69)
    /sys        | ERR: Read-only file system (os error 69)
    /tmp        | ERR: Read-only file system (os error 69)
    ",
    );
}

#[tokio::test]
async fn test_read_dir() {
    let udf = udf("read_dir").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | OK: <EMPTY>
    .           | OK: <EMPTY>
    ..          | OK: <EMPTY>
    /           | OK: <EMPTY>
    /dev        | ERR: No such file or directory (os error 44)
    /etc        | ERR: No such file or directory (os error 44)
    /etc/group  | ERR: No such file or directory (os error 44)
    /etc/passwd | ERR: No such file or directory (os error 44)
    /etc/shadow | ERR: No such file or directory (os error 44)
    /proc       | ERR: No such file or directory (os error 44)
    /proc/self  | ERR: No such file or directory (os error 44)
    /sys        | ERR: No such file or directory (os error 44)
    /tmp        | ERR: No such file or directory (os error 44)
    ",
    );
}

#[tokio::test]
async fn test_read_link() {
    let udf = udf("read_link").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | ERR: Not supported (os error 58)
    .           | ERR: Not supported (os error 58)
    ..          | ERR: Not supported (os error 58)
    /           | ERR: Not supported (os error 58)
    /dev        | ERR: No such file or directory (os error 44)
    /etc        | ERR: No such file or directory (os error 44)
    /etc/group  | ERR: No such file or directory (os error 44)
    /etc/passwd | ERR: No such file or directory (os error 44)
    /etc/shadow | ERR: No such file or directory (os error 44)
    /proc       | ERR: No such file or directory (os error 44)
    /proc/self  | ERR: No such file or directory (os error 44)
    /sys        | ERR: No such file or directory (os error 44)
    /tmp        | ERR: No such file or directory (os error 44)
    ",
    );
}

#[tokio::test]
async fn test_remove_dir() {
    let udf = udf("remove_dir").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | ERR: Read-only file system (os error 69)
    .           | ERR: Read-only file system (os error 69)
    ..          | ERR: Read-only file system (os error 69)
    /           | ERR: Read-only file system (os error 69)
    /dev        | ERR: Read-only file system (os error 69)
    /etc        | ERR: Read-only file system (os error 69)
    /etc/group  | ERR: Read-only file system (os error 69)
    /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/shadow | ERR: Read-only file system (os error 69)
    /proc       | ERR: Read-only file system (os error 69)
    /proc/self  | ERR: Read-only file system (os error 69)
    /sys        | ERR: Read-only file system (os error 69)
    /tmp        | ERR: Read-only file system (os error 69)
    ",
    );
}

#[tokio::test]
async fn test_remove_file() {
    let udf = udf("remove_file").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | ERR: Read-only file system (os error 69)
    .           | ERR: Read-only file system (os error 69)
    ..          | ERR: Read-only file system (os error 69)
    /           | ERR: Read-only file system (os error 69)
    /dev        | ERR: Read-only file system (os error 69)
    /etc        | ERR: Read-only file system (os error 69)
    /etc/group  | ERR: Read-only file system (os error 69)
    /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/shadow | ERR: Read-only file system (os error 69)
    /proc       | ERR: Read-only file system (os error 69)
    /proc/self  | ERR: Read-only file system (os error 69)
    /sys        | ERR: Read-only file system (os error 69)
    /tmp        | ERR: Read-only file system (os error 69)
    ",
    );
}

#[tokio::test]
async fn test_rename() {
    let udf = udf("rename").await;

    insta::assert_snapshot!(
        run_2(&udf).await,
        @r"
               |             | ERR: Read-only file system (os error 69)
               | .           | ERR: Read-only file system (os error 69)
               | ..          | ERR: Read-only file system (os error 69)
               | /           | ERR: Read-only file system (os error 69)
               | /dev        | ERR: Read-only file system (os error 69)
               | /etc        | ERR: Read-only file system (os error 69)
               | /etc/group  | ERR: Read-only file system (os error 69)
               | /etc/passwd | ERR: Read-only file system (os error 69)
               | /etc/shadow | ERR: Read-only file system (os error 69)
               | /proc       | ERR: Read-only file system (os error 69)
               | /proc/self  | ERR: Read-only file system (os error 69)
               | /sys        | ERR: Read-only file system (os error 69)
               | /tmp        | ERR: Read-only file system (os error 69)
    .          |             | ERR: Read-only file system (os error 69)
    .          | .           | ERR: Read-only file system (os error 69)
    .          | ..          | ERR: Read-only file system (os error 69)
    .          | /           | ERR: Read-only file system (os error 69)
    .          | /dev        | ERR: Read-only file system (os error 69)
    .          | /etc        | ERR: Read-only file system (os error 69)
    .          | /etc/group  | ERR: Read-only file system (os error 69)
    .          | /etc/passwd | ERR: Read-only file system (os error 69)
    .          | /etc/shadow | ERR: Read-only file system (os error 69)
    .          | /proc       | ERR: Read-only file system (os error 69)
    .          | /proc/self  | ERR: Read-only file system (os error 69)
    .          | /sys        | ERR: Read-only file system (os error 69)
    .          | /tmp        | ERR: Read-only file system (os error 69)
    ..         |             | ERR: Read-only file system (os error 69)
    ..         | .           | ERR: Read-only file system (os error 69)
    ..         | ..          | ERR: Read-only file system (os error 69)
    ..         | /           | ERR: Read-only file system (os error 69)
    ..         | /dev        | ERR: Read-only file system (os error 69)
    ..         | /etc        | ERR: Read-only file system (os error 69)
    ..         | /etc/group  | ERR: Read-only file system (os error 69)
    ..         | /etc/passwd | ERR: Read-only file system (os error 69)
    ..         | /etc/shadow | ERR: Read-only file system (os error 69)
    ..         | /proc       | ERR: Read-only file system (os error 69)
    ..         | /proc/self  | ERR: Read-only file system (os error 69)
    ..         | /sys        | ERR: Read-only file system (os error 69)
    ..         | /tmp        | ERR: Read-only file system (os error 69)
    /          |             | ERR: Read-only file system (os error 69)
    /          | .           | ERR: Read-only file system (os error 69)
    /          | ..          | ERR: Read-only file system (os error 69)
    /          | /           | ERR: Read-only file system (os error 69)
    /          | /dev        | ERR: Read-only file system (os error 69)
    /          | /etc        | ERR: Read-only file system (os error 69)
    /          | /etc/group  | ERR: Read-only file system (os error 69)
    /          | /etc/passwd | ERR: Read-only file system (os error 69)
    /          | /etc/shadow | ERR: Read-only file system (os error 69)
    /          | /proc       | ERR: Read-only file system (os error 69)
    /          | /proc/self  | ERR: Read-only file system (os error 69)
    /          | /sys        | ERR: Read-only file system (os error 69)
    /          | /tmp        | ERR: Read-only file system (os error 69)
    /dev       |             | ERR: Read-only file system (os error 69)
    /dev       | .           | ERR: Read-only file system (os error 69)
    /dev       | ..          | ERR: Read-only file system (os error 69)
    /dev       | /           | ERR: Read-only file system (os error 69)
    /dev       | /dev        | ERR: Read-only file system (os error 69)
    /dev       | /etc        | ERR: Read-only file system (os error 69)
    /dev       | /etc/group  | ERR: Read-only file system (os error 69)
    /dev       | /etc/passwd | ERR: Read-only file system (os error 69)
    /dev       | /etc/shadow | ERR: Read-only file system (os error 69)
    /dev       | /proc       | ERR: Read-only file system (os error 69)
    /dev       | /proc/self  | ERR: Read-only file system (os error 69)
    /dev       | /sys        | ERR: Read-only file system (os error 69)
    /dev       | /tmp        | ERR: Read-only file system (os error 69)
    /etc       |             | ERR: Read-only file system (os error 69)
    /etc       | .           | ERR: Read-only file system (os error 69)
    /etc       | ..          | ERR: Read-only file system (os error 69)
    /etc       | /           | ERR: Read-only file system (os error 69)
    /etc       | /dev        | ERR: Read-only file system (os error 69)
    /etc       | /etc        | ERR: Read-only file system (os error 69)
    /etc       | /etc/group  | ERR: Read-only file system (os error 69)
    /etc       | /etc/passwd | ERR: Read-only file system (os error 69)
    /etc       | /etc/shadow | ERR: Read-only file system (os error 69)
    /etc       | /proc       | ERR: Read-only file system (os error 69)
    /etc       | /proc/self  | ERR: Read-only file system (os error 69)
    /etc       | /sys        | ERR: Read-only file system (os error 69)
    /etc       | /tmp        | ERR: Read-only file system (os error 69)
    /etc/group |             | ERR: Read-only file system (os error 69)
    /etc/group | .           | ERR: Read-only file system (os error 69)
    /etc/group | ..          | ERR: Read-only file system (os error 69)
    /etc/group | /           | ERR: Read-only file system (os error 69)
    /etc/group | /dev        | ERR: Read-only file system (os error 69)
    /etc/group | /etc        | ERR: Read-only file system (os error 69)
    /etc/group | /etc/group  | ERR: Read-only file system (os error 69)
    /etc/group | /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/group | /etc/shadow | ERR: Read-only file system (os error 69)
    /etc/group | /proc       | ERR: Read-only file system (os error 69)
    /etc/group | /proc/self  | ERR: Read-only file system (os error 69)
    /etc/group | /sys        | ERR: Read-only file system (os error 69)
    /etc/group | /tmp        | ERR: Read-only file system (os error 69)
    /etc/passwd|             | ERR: Read-only file system (os error 69)
    /etc/passwd| .           | ERR: Read-only file system (os error 69)
    /etc/passwd| ..          | ERR: Read-only file system (os error 69)
    /etc/passwd| /           | ERR: Read-only file system (os error 69)
    /etc/passwd| /dev        | ERR: Read-only file system (os error 69)
    /etc/passwd| /etc        | ERR: Read-only file system (os error 69)
    /etc/passwd| /etc/group  | ERR: Read-only file system (os error 69)
    /etc/passwd| /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/passwd| /etc/shadow | ERR: Read-only file system (os error 69)
    /etc/passwd| /proc       | ERR: Read-only file system (os error 69)
    /etc/passwd| /proc/self  | ERR: Read-only file system (os error 69)
    /etc/passwd| /sys        | ERR: Read-only file system (os error 69)
    /etc/passwd| /tmp        | ERR: Read-only file system (os error 69)
    /etc/shadow|             | ERR: Read-only file system (os error 69)
    /etc/shadow| .           | ERR: Read-only file system (os error 69)
    /etc/shadow| ..          | ERR: Read-only file system (os error 69)
    /etc/shadow| /           | ERR: Read-only file system (os error 69)
    /etc/shadow| /dev        | ERR: Read-only file system (os error 69)
    /etc/shadow| /etc        | ERR: Read-only file system (os error 69)
    /etc/shadow| /etc/group  | ERR: Read-only file system (os error 69)
    /etc/shadow| /etc/passwd | ERR: Read-only file system (os error 69)
    /etc/shadow| /etc/shadow | ERR: Read-only file system (os error 69)
    /etc/shadow| /proc       | ERR: Read-only file system (os error 69)
    /etc/shadow| /proc/self  | ERR: Read-only file system (os error 69)
    /etc/shadow| /sys        | ERR: Read-only file system (os error 69)
    /etc/shadow| /tmp        | ERR: Read-only file system (os error 69)
    /proc      |             | ERR: Read-only file system (os error 69)
    /proc      | .           | ERR: Read-only file system (os error 69)
    /proc      | ..          | ERR: Read-only file system (os error 69)
    /proc      | /           | ERR: Read-only file system (os error 69)
    /proc      | /dev        | ERR: Read-only file system (os error 69)
    /proc      | /etc        | ERR: Read-only file system (os error 69)
    /proc      | /etc/group  | ERR: Read-only file system (os error 69)
    /proc      | /etc/passwd | ERR: Read-only file system (os error 69)
    /proc      | /etc/shadow | ERR: Read-only file system (os error 69)
    /proc      | /proc       | ERR: Read-only file system (os error 69)
    /proc      | /proc/self  | ERR: Read-only file system (os error 69)
    /proc      | /sys        | ERR: Read-only file system (os error 69)
    /proc      | /tmp        | ERR: Read-only file system (os error 69)
    /proc/self |             | ERR: Read-only file system (os error 69)
    /proc/self | .           | ERR: Read-only file system (os error 69)
    /proc/self | ..          | ERR: Read-only file system (os error 69)
    /proc/self | /           | ERR: Read-only file system (os error 69)
    /proc/self | /dev        | ERR: Read-only file system (os error 69)
    /proc/self | /etc        | ERR: Read-only file system (os error 69)
    /proc/self | /etc/group  | ERR: Read-only file system (os error 69)
    /proc/self | /etc/passwd | ERR: Read-only file system (os error 69)
    /proc/self | /etc/shadow | ERR: Read-only file system (os error 69)
    /proc/self | /proc       | ERR: Read-only file system (os error 69)
    /proc/self | /proc/self  | ERR: Read-only file system (os error 69)
    /proc/self | /sys        | ERR: Read-only file system (os error 69)
    /proc/self | /tmp        | ERR: Read-only file system (os error 69)
    /sys       |             | ERR: Read-only file system (os error 69)
    /sys       | .           | ERR: Read-only file system (os error 69)
    /sys       | ..          | ERR: Read-only file system (os error 69)
    /sys       | /           | ERR: Read-only file system (os error 69)
    /sys       | /dev        | ERR: Read-only file system (os error 69)
    /sys       | /etc        | ERR: Read-only file system (os error 69)
    /sys       | /etc/group  | ERR: Read-only file system (os error 69)
    /sys       | /etc/passwd | ERR: Read-only file system (os error 69)
    /sys       | /etc/shadow | ERR: Read-only file system (os error 69)
    /sys       | /proc       | ERR: Read-only file system (os error 69)
    /sys       | /proc/self  | ERR: Read-only file system (os error 69)
    /sys       | /sys        | ERR: Read-only file system (os error 69)
    /sys       | /tmp        | ERR: Read-only file system (os error 69)
    /tmp       |             | ERR: Read-only file system (os error 69)
    /tmp       | .           | ERR: Read-only file system (os error 69)
    /tmp       | ..          | ERR: Read-only file system (os error 69)
    /tmp       | /           | ERR: Read-only file system (os error 69)
    /tmp       | /dev        | ERR: Read-only file system (os error 69)
    /tmp       | /etc        | ERR: Read-only file system (os error 69)
    /tmp       | /etc/group  | ERR: Read-only file system (os error 69)
    /tmp       | /etc/passwd | ERR: Read-only file system (os error 69)
    /tmp       | /etc/shadow | ERR: Read-only file system (os error 69)
    /tmp       | /proc       | ERR: Read-only file system (os error 69)
    /tmp       | /proc/self  | ERR: Read-only file system (os error 69)
    /tmp       | /sys        | ERR: Read-only file system (os error 69)
    /tmp       | /tmp        | ERR: Read-only file system (os error 69)
    ",
    );
}

#[tokio::test]
async fn test_set_permissions() {
    let udf = udf("set_permissions").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | ERR: operation not supported on this platform
    .           | ERR: operation not supported on this platform
    ..          | ERR: operation not supported on this platform
    /           | ERR: operation not supported on this platform
    /dev        | ERR: operation not supported on this platform
    /etc        | ERR: operation not supported on this platform
    /etc/group  | ERR: operation not supported on this platform
    /etc/passwd | ERR: operation not supported on this platform
    /etc/shadow | ERR: operation not supported on this platform
    /proc       | ERR: operation not supported on this platform
    /proc/self  | ERR: operation not supported on this platform
    /sys        | ERR: operation not supported on this platform
    /tmp        | ERR: operation not supported on this platform
    ",
    );
}

#[tokio::test]
async fn test_symlink_metadata() {
    let udf = udf("symlink_metadata").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
                | OK: got data
    .           | OK: got data
    ..          | OK: got data
    /           | OK: got data
    /dev        | ERR: No such file or directory (os error 44)
    /etc        | ERR: No such file or directory (os error 44)
    /etc/group  | ERR: No such file or directory (os error 44)
    /etc/passwd | ERR: No such file or directory (os error 44)
    /etc/shadow | ERR: No such file or directory (os error 44)
    /proc       | ERR: No such file or directory (os error 44)
    /proc/self  | ERR: No such file or directory (os error 44)
    /sys        | ERR: No such file or directory (os error 44)
    /tmp        | ERR: No such file or directory (os error 44)
    ",
    );
}

/// Get evil UDF.
async fn udf(name: &'static str) -> WasmScalarUdf {
    try_scalar_udfs("fs")
        .await
        .unwrap()
        .into_iter()
        .find(|udf| udf.name() == name)
        .unwrap()
}

/// Cross-product of array with itself.
fn cross(array: &[&'static str]) -> (Vec<&'static str>, Vec<&'static str>) {
    let mut out_a = Vec::with_capacity(array.len() * array.len());
    let mut out_b = Vec::with_capacity(array.len() * array.len());

    for a in array {
        for b in array {
            out_a.push(*a);
            out_b.push(*b);
        }
    }

    (out_a, out_b)
}

/// Run UDF that expects one string input.
async fn run_1(udf: &WasmScalarUdf) -> String {
    let array = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                PATHS
                    .iter()
                    .map(|p| Some(p.to_owned()))
                    .collect::<StringArray>(),
            ))],
            arg_fields: vec![Arc::new(Field::new("a", DataType::Utf8, true))],
            number_rows: PATHS.len(),
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();
    let array = as_string_array(&array).unwrap();

    let mut out = String::new();
    let longest_path = PATHS.iter().map(|p| p.len()).max().unwrap();
    for (path, res) in PATHS.iter().zip(array) {
        let res = res.unwrap();
        write!(&mut out, "{path}").unwrap();
        for _ in 0..(longest_path - path.len()) {
            write!(&mut out, " ").unwrap();
        }
        writeln!(&mut out, " | {res}").unwrap();
    }

    out
}

/// Run UDF that expects two string inputs.
async fn run_2(udf: &WasmScalarUdf) -> String {
    let (paths_a, paths_b) = cross(PATHS);

    let array = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(
                    paths_a
                        .iter()
                        .map(|p| Some(p.to_owned()))
                        .collect::<StringArray>(),
                )),
                ColumnarValue::Array(Arc::new(
                    paths_b
                        .iter()
                        .map(|p| Some(p.to_owned()))
                        .collect::<StringArray>(),
                )),
            ],
            arg_fields: vec![
                Arc::new(Field::new("a", DataType::Utf8, true)),
                Arc::new(Field::new("b", DataType::Utf8, true)),
            ],
            number_rows: paths_a.len(),
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();
    let array = as_string_array(&array).unwrap();

    let mut out = String::new();
    let longest_path = PATHS.iter().map(|p| p.len()).max().unwrap();
    for ((path_a, path_b), res) in paths_a.iter().zip(paths_b).zip(array) {
        let res = res.unwrap();
        write!(&mut out, "{path_a}").unwrap();
        for _ in 0..(longest_path - path_a.len()) {
            write!(&mut out, " ").unwrap();
        }
        write!(&mut out, "| {path_b}").unwrap();
        for _ in 0..(longest_path - path_b.len()) {
            write!(&mut out, " ").unwrap();
        }
        writeln!(&mut out, " | {res}").unwrap();
    }

    out
}
