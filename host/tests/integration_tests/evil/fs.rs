use arrow::{
    array::{Array, RecordBatch, StringArray},
    datatypes::{DataType, Field},
};
use datafusion_common::{config::ConfigOptions, test_util::batches_to_string};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, async_udf::AsyncScalarUDFImpl,
};
use datafusion_udf_wasm_host::WasmScalarUdf;
use std::sync::Arc;

use crate::integration_tests::{evil::test_utils::try_scalar_udfs, test_utils::ColumnarValueExt};

const PATHS: &[&str] = &[
    // NOTE: it seems that the WASI guest transforms `` (empty string) into `.` before it even reaches the host
    "",
    ".",
    "..",
    "/",
    "/bin",
    "/boot",
    "/dev",
    "/etc",
    "/etc/group",
    "/etc/passwd",
    "/etc/shadow",
    "/home",
    "/lib",
    "/lib64",
    "/opt",
    "/proc",
    "/proc/self",
    "/root",
    "/run",
    "/sbin",
    "/srv",
    "/sys",
    "/tmp",
    "/usr",
    "/var",
    "\0",
    "/x/..",
];

#[tokio::test]
async fn test_canonicalize() {
    let udf = udf("canonicalize").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-----------------------------------------------+
    | path        | result                                        |
    +-------------+-----------------------------------------------+
    |             | ERR: operation not supported on this platform |
    | .           | ERR: operation not supported on this platform |
    | ..          | ERR: operation not supported on this platform |
    | /           | ERR: operation not supported on this platform |
    | /bin        | ERR: operation not supported on this platform |
    | /boot       | ERR: operation not supported on this platform |
    | /dev        | ERR: operation not supported on this platform |
    | /etc        | ERR: operation not supported on this platform |
    | /etc/group  | ERR: operation not supported on this platform |
    | /etc/passwd | ERR: operation not supported on this platform |
    | /etc/shadow | ERR: operation not supported on this platform |
    | /home       | ERR: operation not supported on this platform |
    | /lib        | ERR: operation not supported on this platform |
    | /lib64      | ERR: operation not supported on this platform |
    | /opt        | ERR: operation not supported on this platform |
    | /proc       | ERR: operation not supported on this platform |
    | /proc/self  | ERR: operation not supported on this platform |
    | /root       | ERR: operation not supported on this platform |
    | /run        | ERR: operation not supported on this platform |
    | /sbin       | ERR: operation not supported on this platform |
    | /srv        | ERR: operation not supported on this platform |
    | /sys        | ERR: operation not supported on this platform |
    | /tmp        | ERR: operation not supported on this platform |
    | /usr        | ERR: operation not supported on this platform |
    | /var        | ERR: operation not supported on this platform |
    | \0          | ERR: operation not supported on this platform |
    | /x/..       | ERR: operation not supported on this platform |
    +-------------+-----------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_copy() {
    let udf = udf("copy").await;

    insta::assert_snapshot!(
        run_2(&udf).await,
        @r"
    +-------------+-------------+-------------------------------------------------+
    | from        | to          | output                                          |
    +-------------+-------------+-------------------------------------------------+
    |             |             | ERR: Is a directory (os error 31)               |
    |             | .           | ERR: Is a directory (os error 31)               |
    |             | ..          | ERR: Is a directory (os error 31)               |
    |             | /           | ERR: Is a directory (os error 31)               |
    |             | /bin        | ERR: Bad file descriptor (os error 8)           |
    |             | /boot       | ERR: Bad file descriptor (os error 8)           |
    |             | /dev        | ERR: Bad file descriptor (os error 8)           |
    |             | /etc        | ERR: Bad file descriptor (os error 8)           |
    |             | /etc/group  | ERR: Not a directory (os error 54)              |
    |             | /etc/passwd | ERR: Not a directory (os error 54)              |
    |             | /etc/shadow | ERR: Not a directory (os error 54)              |
    |             | /home       | ERR: Bad file descriptor (os error 8)           |
    |             | /lib        | ERR: Bad file descriptor (os error 8)           |
    |             | /lib64      | ERR: Bad file descriptor (os error 8)           |
    |             | /opt        | ERR: Bad file descriptor (os error 8)           |
    |             | /proc       | ERR: Bad file descriptor (os error 8)           |
    |             | /proc/self  | ERR: Not a directory (os error 54)              |
    |             | /root       | ERR: Bad file descriptor (os error 8)           |
    |             | /run        | ERR: Bad file descriptor (os error 8)           |
    |             | /sbin       | ERR: Bad file descriptor (os error 8)           |
    |             | /srv        | ERR: Bad file descriptor (os error 8)           |
    |             | /sys        | ERR: Bad file descriptor (os error 8)           |
    |             | /tmp        | ERR: Bad file descriptor (os error 8)           |
    |             | /usr        | ERR: Bad file descriptor (os error 8)           |
    |             | /var        | ERR: Bad file descriptor (os error 8)           |
    |             | \0          | ERR: file name contained an unexpected NUL byte |
    |             | /x/..       | ERR: Invalid argument (os error 28)             |
    | .           |             | ERR: Is a directory (os error 31)               |
    | .           | .           | ERR: Is a directory (os error 31)               |
    | .           | ..          | ERR: Is a directory (os error 31)               |
    | .           | /           | ERR: Is a directory (os error 31)               |
    | .           | /bin        | ERR: Bad file descriptor (os error 8)           |
    | .           | /boot       | ERR: Bad file descriptor (os error 8)           |
    | .           | /dev        | ERR: Bad file descriptor (os error 8)           |
    | .           | /etc        | ERR: Bad file descriptor (os error 8)           |
    | .           | /etc/group  | ERR: Not a directory (os error 54)              |
    | .           | /etc/passwd | ERR: Not a directory (os error 54)              |
    | .           | /etc/shadow | ERR: Not a directory (os error 54)              |
    | .           | /home       | ERR: Bad file descriptor (os error 8)           |
    | .           | /lib        | ERR: Bad file descriptor (os error 8)           |
    | .           | /lib64      | ERR: Bad file descriptor (os error 8)           |
    | .           | /opt        | ERR: Bad file descriptor (os error 8)           |
    | .           | /proc       | ERR: Bad file descriptor (os error 8)           |
    | .           | /proc/self  | ERR: Not a directory (os error 54)              |
    | .           | /root       | ERR: Bad file descriptor (os error 8)           |
    | .           | /run        | ERR: Bad file descriptor (os error 8)           |
    | .           | /sbin       | ERR: Bad file descriptor (os error 8)           |
    | .           | /srv        | ERR: Bad file descriptor (os error 8)           |
    | .           | /sys        | ERR: Bad file descriptor (os error 8)           |
    | .           | /tmp        | ERR: Bad file descriptor (os error 8)           |
    | .           | /usr        | ERR: Bad file descriptor (os error 8)           |
    | .           | /var        | ERR: Bad file descriptor (os error 8)           |
    | .           | \0          | ERR: file name contained an unexpected NUL byte |
    | .           | /x/..       | ERR: Invalid argument (os error 28)             |
    | ..          |             | ERR: Is a directory (os error 31)               |
    | ..          | .           | ERR: Is a directory (os error 31)               |
    | ..          | ..          | ERR: Is a directory (os error 31)               |
    | ..          | /           | ERR: Is a directory (os error 31)               |
    | ..          | /bin        | ERR: Bad file descriptor (os error 8)           |
    | ..          | /boot       | ERR: Bad file descriptor (os error 8)           |
    | ..          | /dev        | ERR: Bad file descriptor (os error 8)           |
    | ..          | /etc        | ERR: Bad file descriptor (os error 8)           |
    | ..          | /etc/group  | ERR: Not a directory (os error 54)              |
    | ..          | /etc/passwd | ERR: Not a directory (os error 54)              |
    | ..          | /etc/shadow | ERR: Not a directory (os error 54)              |
    | ..          | /home       | ERR: Bad file descriptor (os error 8)           |
    | ..          | /lib        | ERR: Bad file descriptor (os error 8)           |
    | ..          | /lib64      | ERR: Bad file descriptor (os error 8)           |
    | ..          | /opt        | ERR: Bad file descriptor (os error 8)           |
    | ..          | /proc       | ERR: Bad file descriptor (os error 8)           |
    | ..          | /proc/self  | ERR: Not a directory (os error 54)              |
    | ..          | /root       | ERR: Bad file descriptor (os error 8)           |
    | ..          | /run        | ERR: Bad file descriptor (os error 8)           |
    | ..          | /sbin       | ERR: Bad file descriptor (os error 8)           |
    | ..          | /srv        | ERR: Bad file descriptor (os error 8)           |
    | ..          | /sys        | ERR: Bad file descriptor (os error 8)           |
    | ..          | /tmp        | ERR: Bad file descriptor (os error 8)           |
    | ..          | /usr        | ERR: Bad file descriptor (os error 8)           |
    | ..          | /var        | ERR: Bad file descriptor (os error 8)           |
    | ..          | \0          | ERR: file name contained an unexpected NUL byte |
    | ..          | /x/..       | ERR: Invalid argument (os error 28)             |
    | /           |             | ERR: Is a directory (os error 31)               |
    | /           | .           | ERR: Is a directory (os error 31)               |
    | /           | ..          | ERR: Is a directory (os error 31)               |
    | /           | /           | ERR: Is a directory (os error 31)               |
    | /           | /bin        | ERR: Bad file descriptor (os error 8)           |
    | /           | /boot       | ERR: Bad file descriptor (os error 8)           |
    | /           | /dev        | ERR: Bad file descriptor (os error 8)           |
    | /           | /etc        | ERR: Bad file descriptor (os error 8)           |
    | /           | /etc/group  | ERR: Not a directory (os error 54)              |
    | /           | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /           | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /           | /home       | ERR: Bad file descriptor (os error 8)           |
    | /           | /lib        | ERR: Bad file descriptor (os error 8)           |
    | /           | /lib64      | ERR: Bad file descriptor (os error 8)           |
    | /           | /opt        | ERR: Bad file descriptor (os error 8)           |
    | /           | /proc       | ERR: Bad file descriptor (os error 8)           |
    | /           | /proc/self  | ERR: Not a directory (os error 54)              |
    | /           | /root       | ERR: Bad file descriptor (os error 8)           |
    | /           | /run        | ERR: Bad file descriptor (os error 8)           |
    | /           | /sbin       | ERR: Bad file descriptor (os error 8)           |
    | /           | /srv        | ERR: Bad file descriptor (os error 8)           |
    | /           | /sys        | ERR: Bad file descriptor (os error 8)           |
    | /           | /tmp        | ERR: Bad file descriptor (os error 8)           |
    | /           | /usr        | ERR: Bad file descriptor (os error 8)           |
    | /           | /var        | ERR: Bad file descriptor (os error 8)           |
    | /           | \0          | ERR: file name contained an unexpected NUL byte |
    | /           | /x/..       | ERR: Invalid argument (os error 28)             |
    | /bin        |             | ERR: Is a directory (os error 31)               |
    | /bin        | .           | ERR: Is a directory (os error 31)               |
    | /bin        | ..          | ERR: Is a directory (os error 31)               |
    | /bin        | /           | ERR: Is a directory (os error 31)               |
    | /bin        | /bin        | OK: 0                                           |
    | /bin        | /boot       | OK: 0                                           |
    | /bin        | /dev        | OK: 0                                           |
    | /bin        | /etc        | OK: 0                                           |
    | /bin        | /etc/group  | ERR: Not a directory (os error 54)              |
    | /bin        | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /bin        | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /bin        | /home       | OK: 0                                           |
    | /bin        | /lib        | OK: 0                                           |
    | /bin        | /lib64      | OK: 0                                           |
    | /bin        | /opt        | OK: 0                                           |
    | /bin        | /proc       | OK: 0                                           |
    | /bin        | /proc/self  | ERR: Not a directory (os error 54)              |
    | /bin        | /root       | OK: 0                                           |
    | /bin        | /run        | OK: 0                                           |
    | /bin        | /sbin       | OK: 0                                           |
    | /bin        | /srv        | OK: 0                                           |
    | /bin        | /sys        | OK: 0                                           |
    | /bin        | /tmp        | OK: 0                                           |
    | /bin        | /usr        | OK: 0                                           |
    | /bin        | /var        | OK: 0                                           |
    | /bin        | \0          | ERR: file name contained an unexpected NUL byte |
    | /bin        | /x/..       | ERR: Invalid argument (os error 28)             |
    | /boot       |             | ERR: Is a directory (os error 31)               |
    | /boot       | .           | ERR: Is a directory (os error 31)               |
    | /boot       | ..          | ERR: Is a directory (os error 31)               |
    | /boot       | /           | ERR: Is a directory (os error 31)               |
    | /boot       | /bin        | OK: 0                                           |
    | /boot       | /boot       | OK: 0                                           |
    | /boot       | /dev        | OK: 0                                           |
    | /boot       | /etc        | OK: 0                                           |
    | /boot       | /etc/group  | ERR: Not a directory (os error 54)              |
    | /boot       | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /boot       | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /boot       | /home       | OK: 0                                           |
    | /boot       | /lib        | OK: 0                                           |
    | /boot       | /lib64      | OK: 0                                           |
    | /boot       | /opt        | OK: 0                                           |
    | /boot       | /proc       | OK: 0                                           |
    | /boot       | /proc/self  | ERR: Not a directory (os error 54)              |
    | /boot       | /root       | OK: 0                                           |
    | /boot       | /run        | OK: 0                                           |
    | /boot       | /sbin       | OK: 0                                           |
    | /boot       | /srv        | OK: 0                                           |
    | /boot       | /sys        | OK: 0                                           |
    | /boot       | /tmp        | OK: 0                                           |
    | /boot       | /usr        | OK: 0                                           |
    | /boot       | /var        | OK: 0                                           |
    | /boot       | \0          | ERR: file name contained an unexpected NUL byte |
    | /boot       | /x/..       | ERR: Invalid argument (os error 28)             |
    | /dev        |             | ERR: Is a directory (os error 31)               |
    | /dev        | .           | ERR: Is a directory (os error 31)               |
    | /dev        | ..          | ERR: Is a directory (os error 31)               |
    | /dev        | /           | ERR: Is a directory (os error 31)               |
    | /dev        | /bin        | OK: 0                                           |
    | /dev        | /boot       | OK: 0                                           |
    | /dev        | /dev        | OK: 0                                           |
    | /dev        | /etc        | OK: 0                                           |
    | /dev        | /etc/group  | ERR: Not a directory (os error 54)              |
    | /dev        | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /dev        | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /dev        | /home       | OK: 0                                           |
    | /dev        | /lib        | OK: 0                                           |
    | /dev        | /lib64      | OK: 0                                           |
    | /dev        | /opt        | OK: 0                                           |
    | /dev        | /proc       | OK: 0                                           |
    | /dev        | /proc/self  | ERR: Not a directory (os error 54)              |
    | /dev        | /root       | OK: 0                                           |
    | /dev        | /run        | OK: 0                                           |
    | /dev        | /sbin       | OK: 0                                           |
    | /dev        | /srv        | OK: 0                                           |
    | /dev        | /sys        | OK: 0                                           |
    | /dev        | /tmp        | OK: 0                                           |
    | /dev        | /usr        | OK: 0                                           |
    | /dev        | /var        | OK: 0                                           |
    | /dev        | \0          | ERR: file name contained an unexpected NUL byte |
    | /dev        | /x/..       | ERR: Invalid argument (os error 28)             |
    | /etc        |             | ERR: Is a directory (os error 31)               |
    | /etc        | .           | ERR: Is a directory (os error 31)               |
    | /etc        | ..          | ERR: Is a directory (os error 31)               |
    | /etc        | /           | ERR: Is a directory (os error 31)               |
    | /etc        | /bin        | OK: 0                                           |
    | /etc        | /boot       | OK: 0                                           |
    | /etc        | /dev        | OK: 0                                           |
    | /etc        | /etc        | OK: 0                                           |
    | /etc        | /etc/group  | ERR: Not a directory (os error 54)              |
    | /etc        | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /etc        | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /etc        | /home       | OK: 0                                           |
    | /etc        | /lib        | OK: 0                                           |
    | /etc        | /lib64      | OK: 0                                           |
    | /etc        | /opt        | OK: 0                                           |
    | /etc        | /proc       | OK: 0                                           |
    | /etc        | /proc/self  | ERR: Not a directory (os error 54)              |
    | /etc        | /root       | OK: 0                                           |
    | /etc        | /run        | OK: 0                                           |
    | /etc        | /sbin       | OK: 0                                           |
    | /etc        | /srv        | OK: 0                                           |
    | /etc        | /sys        | OK: 0                                           |
    | /etc        | /tmp        | OK: 0                                           |
    | /etc        | /usr        | OK: 0                                           |
    | /etc        | /var        | OK: 0                                           |
    | /etc        | \0          | ERR: file name contained an unexpected NUL byte |
    | /etc        | /x/..       | ERR: Invalid argument (os error 28)             |
    | /etc/group  |             | ERR: Not a directory (os error 54)              |
    | /etc/group  | .           | ERR: Not a directory (os error 54)              |
    | /etc/group  | ..          | ERR: Not a directory (os error 54)              |
    | /etc/group  | /           | ERR: Not a directory (os error 54)              |
    | /etc/group  | /bin        | ERR: Not a directory (os error 54)              |
    | /etc/group  | /boot       | ERR: Not a directory (os error 54)              |
    | /etc/group  | /dev        | ERR: Not a directory (os error 54)              |
    | /etc/group  | /etc        | ERR: Not a directory (os error 54)              |
    | /etc/group  | /etc/group  | ERR: Not a directory (os error 54)              |
    | /etc/group  | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /etc/group  | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /etc/group  | /home       | ERR: Not a directory (os error 54)              |
    | /etc/group  | /lib        | ERR: Not a directory (os error 54)              |
    | /etc/group  | /lib64      | ERR: Not a directory (os error 54)              |
    | /etc/group  | /opt        | ERR: Not a directory (os error 54)              |
    | /etc/group  | /proc       | ERR: Not a directory (os error 54)              |
    | /etc/group  | /proc/self  | ERR: Not a directory (os error 54)              |
    | /etc/group  | /root       | ERR: Not a directory (os error 54)              |
    | /etc/group  | /run        | ERR: Not a directory (os error 54)              |
    | /etc/group  | /sbin       | ERR: Not a directory (os error 54)              |
    | /etc/group  | /srv        | ERR: Not a directory (os error 54)              |
    | /etc/group  | /sys        | ERR: Not a directory (os error 54)              |
    | /etc/group  | /tmp        | ERR: Not a directory (os error 54)              |
    | /etc/group  | /usr        | ERR: Not a directory (os error 54)              |
    | /etc/group  | /var        | ERR: Not a directory (os error 54)              |
    | /etc/group  | \0          | ERR: Not a directory (os error 54)              |
    | /etc/group  | /x/..       | ERR: Not a directory (os error 54)              |
    | /etc/passwd |             | ERR: Not a directory (os error 54)              |
    | /etc/passwd | .           | ERR: Not a directory (os error 54)              |
    | /etc/passwd | ..          | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /           | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /bin        | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /boot       | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /dev        | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /etc        | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /etc/group  | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /home       | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /lib        | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /lib64      | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /opt        | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /proc       | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /proc/self  | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /root       | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /run        | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /sbin       | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /srv        | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /sys        | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /tmp        | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /usr        | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /var        | ERR: Not a directory (os error 54)              |
    | /etc/passwd | \0          | ERR: Not a directory (os error 54)              |
    | /etc/passwd | /x/..       | ERR: Not a directory (os error 54)              |
    | /etc/shadow |             | ERR: Not a directory (os error 54)              |
    | /etc/shadow | .           | ERR: Not a directory (os error 54)              |
    | /etc/shadow | ..          | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /           | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /bin        | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /boot       | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /dev        | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /etc        | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /etc/group  | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /home       | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /lib        | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /lib64      | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /opt        | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /proc       | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /proc/self  | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /root       | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /run        | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /sbin       | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /srv        | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /sys        | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /tmp        | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /usr        | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /var        | ERR: Not a directory (os error 54)              |
    | /etc/shadow | \0          | ERR: Not a directory (os error 54)              |
    | /etc/shadow | /x/..       | ERR: Not a directory (os error 54)              |
    | /home       |             | ERR: Is a directory (os error 31)               |
    | /home       | .           | ERR: Is a directory (os error 31)               |
    | /home       | ..          | ERR: Is a directory (os error 31)               |
    | /home       | /           | ERR: Is a directory (os error 31)               |
    | /home       | /bin        | OK: 0                                           |
    | /home       | /boot       | OK: 0                                           |
    | /home       | /dev        | OK: 0                                           |
    | /home       | /etc        | OK: 0                                           |
    | /home       | /etc/group  | ERR: Not a directory (os error 54)              |
    | /home       | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /home       | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /home       | /home       | OK: 0                                           |
    | /home       | /lib        | OK: 0                                           |
    | /home       | /lib64      | OK: 0                                           |
    | /home       | /opt        | OK: 0                                           |
    | /home       | /proc       | OK: 0                                           |
    | /home       | /proc/self  | ERR: Not a directory (os error 54)              |
    | /home       | /root       | OK: 0                                           |
    | /home       | /run        | OK: 0                                           |
    | /home       | /sbin       | OK: 0                                           |
    | /home       | /srv        | OK: 0                                           |
    | /home       | /sys        | OK: 0                                           |
    | /home       | /tmp        | OK: 0                                           |
    | /home       | /usr        | OK: 0                                           |
    | /home       | /var        | OK: 0                                           |
    | /home       | \0          | ERR: file name contained an unexpected NUL byte |
    | /home       | /x/..       | ERR: Invalid argument (os error 28)             |
    | /lib        |             | ERR: Is a directory (os error 31)               |
    | /lib        | .           | ERR: Is a directory (os error 31)               |
    | /lib        | ..          | ERR: Is a directory (os error 31)               |
    | /lib        | /           | ERR: Is a directory (os error 31)               |
    | /lib        | /bin        | OK: 0                                           |
    | /lib        | /boot       | OK: 0                                           |
    | /lib        | /dev        | OK: 0                                           |
    | /lib        | /etc        | OK: 0                                           |
    | /lib        | /etc/group  | ERR: Not a directory (os error 54)              |
    | /lib        | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /lib        | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /lib        | /home       | OK: 0                                           |
    | /lib        | /lib        | OK: 0                                           |
    | /lib        | /lib64      | OK: 0                                           |
    | /lib        | /opt        | OK: 0                                           |
    | /lib        | /proc       | OK: 0                                           |
    | /lib        | /proc/self  | ERR: Not a directory (os error 54)              |
    | /lib        | /root       | OK: 0                                           |
    | /lib        | /run        | OK: 0                                           |
    | /lib        | /sbin       | OK: 0                                           |
    | /lib        | /srv        | OK: 0                                           |
    | /lib        | /sys        | OK: 0                                           |
    | /lib        | /tmp        | OK: 0                                           |
    | /lib        | /usr        | OK: 0                                           |
    | /lib        | /var        | OK: 0                                           |
    | /lib        | \0          | ERR: file name contained an unexpected NUL byte |
    | /lib        | /x/..       | ERR: Invalid argument (os error 28)             |
    | /lib64      |             | ERR: Is a directory (os error 31)               |
    | /lib64      | .           | ERR: Is a directory (os error 31)               |
    | /lib64      | ..          | ERR: Is a directory (os error 31)               |
    | /lib64      | /           | ERR: Is a directory (os error 31)               |
    | /lib64      | /bin        | OK: 0                                           |
    | /lib64      | /boot       | OK: 0                                           |
    | /lib64      | /dev        | OK: 0                                           |
    | /lib64      | /etc        | OK: 0                                           |
    | /lib64      | /etc/group  | ERR: Not a directory (os error 54)              |
    | /lib64      | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /lib64      | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /lib64      | /home       | OK: 0                                           |
    | /lib64      | /lib        | OK: 0                                           |
    | /lib64      | /lib64      | OK: 0                                           |
    | /lib64      | /opt        | OK: 0                                           |
    | /lib64      | /proc       | OK: 0                                           |
    | /lib64      | /proc/self  | ERR: Not a directory (os error 54)              |
    | /lib64      | /root       | OK: 0                                           |
    | /lib64      | /run        | OK: 0                                           |
    | /lib64      | /sbin       | OK: 0                                           |
    | /lib64      | /srv        | OK: 0                                           |
    | /lib64      | /sys        | OK: 0                                           |
    | /lib64      | /tmp        | OK: 0                                           |
    | /lib64      | /usr        | OK: 0                                           |
    | /lib64      | /var        | OK: 0                                           |
    | /lib64      | \0          | ERR: file name contained an unexpected NUL byte |
    | /lib64      | /x/..       | ERR: Invalid argument (os error 28)             |
    | /opt        |             | ERR: Is a directory (os error 31)               |
    | /opt        | .           | ERR: Is a directory (os error 31)               |
    | /opt        | ..          | ERR: Is a directory (os error 31)               |
    | /opt        | /           | ERR: Is a directory (os error 31)               |
    | /opt        | /bin        | OK: 0                                           |
    | /opt        | /boot       | OK: 0                                           |
    | /opt        | /dev        | OK: 0                                           |
    | /opt        | /etc        | OK: 0                                           |
    | /opt        | /etc/group  | ERR: Not a directory (os error 54)              |
    | /opt        | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /opt        | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /opt        | /home       | OK: 0                                           |
    | /opt        | /lib        | OK: 0                                           |
    | /opt        | /lib64      | OK: 0                                           |
    | /opt        | /opt        | OK: 0                                           |
    | /opt        | /proc       | OK: 0                                           |
    | /opt        | /proc/self  | ERR: Not a directory (os error 54)              |
    | /opt        | /root       | OK: 0                                           |
    | /opt        | /run        | OK: 0                                           |
    | /opt        | /sbin       | OK: 0                                           |
    | /opt        | /srv        | OK: 0                                           |
    | /opt        | /sys        | OK: 0                                           |
    | /opt        | /tmp        | OK: 0                                           |
    | /opt        | /usr        | OK: 0                                           |
    | /opt        | /var        | OK: 0                                           |
    | /opt        | \0          | ERR: file name contained an unexpected NUL byte |
    | /opt        | /x/..       | ERR: Invalid argument (os error 28)             |
    | /proc       |             | ERR: Is a directory (os error 31)               |
    | /proc       | .           | ERR: Is a directory (os error 31)               |
    | /proc       | ..          | ERR: Is a directory (os error 31)               |
    | /proc       | /           | ERR: Is a directory (os error 31)               |
    | /proc       | /bin        | OK: 0                                           |
    | /proc       | /boot       | OK: 0                                           |
    | /proc       | /dev        | OK: 0                                           |
    | /proc       | /etc        | OK: 0                                           |
    | /proc       | /etc/group  | ERR: Not a directory (os error 54)              |
    | /proc       | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /proc       | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /proc       | /home       | OK: 0                                           |
    | /proc       | /lib        | OK: 0                                           |
    | /proc       | /lib64      | OK: 0                                           |
    | /proc       | /opt        | OK: 0                                           |
    | /proc       | /proc       | OK: 0                                           |
    | /proc       | /proc/self  | ERR: Not a directory (os error 54)              |
    | /proc       | /root       | OK: 0                                           |
    | /proc       | /run        | OK: 0                                           |
    | /proc       | /sbin       | OK: 0                                           |
    | /proc       | /srv        | OK: 0                                           |
    | /proc       | /sys        | OK: 0                                           |
    | /proc       | /tmp        | OK: 0                                           |
    | /proc       | /usr        | OK: 0                                           |
    | /proc       | /var        | OK: 0                                           |
    | /proc       | \0          | ERR: file name contained an unexpected NUL byte |
    | /proc       | /x/..       | ERR: Invalid argument (os error 28)             |
    | /proc/self  |             | ERR: Not a directory (os error 54)              |
    | /proc/self  | .           | ERR: Not a directory (os error 54)              |
    | /proc/self  | ..          | ERR: Not a directory (os error 54)              |
    | /proc/self  | /           | ERR: Not a directory (os error 54)              |
    | /proc/self  | /bin        | ERR: Not a directory (os error 54)              |
    | /proc/self  | /boot       | ERR: Not a directory (os error 54)              |
    | /proc/self  | /dev        | ERR: Not a directory (os error 54)              |
    | /proc/self  | /etc        | ERR: Not a directory (os error 54)              |
    | /proc/self  | /etc/group  | ERR: Not a directory (os error 54)              |
    | /proc/self  | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /proc/self  | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /proc/self  | /home       | ERR: Not a directory (os error 54)              |
    | /proc/self  | /lib        | ERR: Not a directory (os error 54)              |
    | /proc/self  | /lib64      | ERR: Not a directory (os error 54)              |
    | /proc/self  | /opt        | ERR: Not a directory (os error 54)              |
    | /proc/self  | /proc       | ERR: Not a directory (os error 54)              |
    | /proc/self  | /proc/self  | ERR: Not a directory (os error 54)              |
    | /proc/self  | /root       | ERR: Not a directory (os error 54)              |
    | /proc/self  | /run        | ERR: Not a directory (os error 54)              |
    | /proc/self  | /sbin       | ERR: Not a directory (os error 54)              |
    | /proc/self  | /srv        | ERR: Not a directory (os error 54)              |
    | /proc/self  | /sys        | ERR: Not a directory (os error 54)              |
    | /proc/self  | /tmp        | ERR: Not a directory (os error 54)              |
    | /proc/self  | /usr        | ERR: Not a directory (os error 54)              |
    | /proc/self  | /var        | ERR: Not a directory (os error 54)              |
    | /proc/self  | \0          | ERR: Not a directory (os error 54)              |
    | /proc/self  | /x/..       | ERR: Not a directory (os error 54)              |
    | /root       |             | ERR: Is a directory (os error 31)               |
    | /root       | .           | ERR: Is a directory (os error 31)               |
    | /root       | ..          | ERR: Is a directory (os error 31)               |
    | /root       | /           | ERR: Is a directory (os error 31)               |
    | /root       | /bin        | OK: 0                                           |
    | /root       | /boot       | OK: 0                                           |
    | /root       | /dev        | OK: 0                                           |
    | /root       | /etc        | OK: 0                                           |
    | /root       | /etc/group  | ERR: Not a directory (os error 54)              |
    | /root       | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /root       | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /root       | /home       | OK: 0                                           |
    | /root       | /lib        | OK: 0                                           |
    | /root       | /lib64      | OK: 0                                           |
    | /root       | /opt        | OK: 0                                           |
    | /root       | /proc       | OK: 0                                           |
    | /root       | /proc/self  | ERR: Not a directory (os error 54)              |
    | /root       | /root       | OK: 0                                           |
    | /root       | /run        | OK: 0                                           |
    | /root       | /sbin       | OK: 0                                           |
    | /root       | /srv        | OK: 0                                           |
    | /root       | /sys        | OK: 0                                           |
    | /root       | /tmp        | OK: 0                                           |
    | /root       | /usr        | OK: 0                                           |
    | /root       | /var        | OK: 0                                           |
    | /root       | \0          | ERR: file name contained an unexpected NUL byte |
    | /root       | /x/..       | ERR: Invalid argument (os error 28)             |
    | /run        |             | ERR: Is a directory (os error 31)               |
    | /run        | .           | ERR: Is a directory (os error 31)               |
    | /run        | ..          | ERR: Is a directory (os error 31)               |
    | /run        | /           | ERR: Is a directory (os error 31)               |
    | /run        | /bin        | OK: 0                                           |
    | /run        | /boot       | OK: 0                                           |
    | /run        | /dev        | OK: 0                                           |
    | /run        | /etc        | OK: 0                                           |
    | /run        | /etc/group  | ERR: Not a directory (os error 54)              |
    | /run        | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /run        | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /run        | /home       | OK: 0                                           |
    | /run        | /lib        | OK: 0                                           |
    | /run        | /lib64      | OK: 0                                           |
    | /run        | /opt        | OK: 0                                           |
    | /run        | /proc       | OK: 0                                           |
    | /run        | /proc/self  | ERR: Not a directory (os error 54)              |
    | /run        | /root       | OK: 0                                           |
    | /run        | /run        | OK: 0                                           |
    | /run        | /sbin       | OK: 0                                           |
    | /run        | /srv        | OK: 0                                           |
    | /run        | /sys        | OK: 0                                           |
    | /run        | /tmp        | OK: 0                                           |
    | /run        | /usr        | OK: 0                                           |
    | /run        | /var        | OK: 0                                           |
    | /run        | \0          | ERR: file name contained an unexpected NUL byte |
    | /run        | /x/..       | ERR: Invalid argument (os error 28)             |
    | /sbin       |             | ERR: Is a directory (os error 31)               |
    | /sbin       | .           | ERR: Is a directory (os error 31)               |
    | /sbin       | ..          | ERR: Is a directory (os error 31)               |
    | /sbin       | /           | ERR: Is a directory (os error 31)               |
    | /sbin       | /bin        | OK: 0                                           |
    | /sbin       | /boot       | OK: 0                                           |
    | /sbin       | /dev        | OK: 0                                           |
    | /sbin       | /etc        | OK: 0                                           |
    | /sbin       | /etc/group  | ERR: Not a directory (os error 54)              |
    | /sbin       | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /sbin       | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /sbin       | /home       | OK: 0                                           |
    | /sbin       | /lib        | OK: 0                                           |
    | /sbin       | /lib64      | OK: 0                                           |
    | /sbin       | /opt        | OK: 0                                           |
    | /sbin       | /proc       | OK: 0                                           |
    | /sbin       | /proc/self  | ERR: Not a directory (os error 54)              |
    | /sbin       | /root       | OK: 0                                           |
    | /sbin       | /run        | OK: 0                                           |
    | /sbin       | /sbin       | OK: 0                                           |
    | /sbin       | /srv        | OK: 0                                           |
    | /sbin       | /sys        | OK: 0                                           |
    | /sbin       | /tmp        | OK: 0                                           |
    | /sbin       | /usr        | OK: 0                                           |
    | /sbin       | /var        | OK: 0                                           |
    | /sbin       | \0          | ERR: file name contained an unexpected NUL byte |
    | /sbin       | /x/..       | ERR: Invalid argument (os error 28)             |
    | /srv        |             | ERR: Is a directory (os error 31)               |
    | /srv        | .           | ERR: Is a directory (os error 31)               |
    | /srv        | ..          | ERR: Is a directory (os error 31)               |
    | /srv        | /           | ERR: Is a directory (os error 31)               |
    | /srv        | /bin        | OK: 0                                           |
    | /srv        | /boot       | OK: 0                                           |
    | /srv        | /dev        | OK: 0                                           |
    | /srv        | /etc        | OK: 0                                           |
    | /srv        | /etc/group  | ERR: Not a directory (os error 54)              |
    | /srv        | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /srv        | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /srv        | /home       | OK: 0                                           |
    | /srv        | /lib        | OK: 0                                           |
    | /srv        | /lib64      | OK: 0                                           |
    | /srv        | /opt        | OK: 0                                           |
    | /srv        | /proc       | OK: 0                                           |
    | /srv        | /proc/self  | ERR: Not a directory (os error 54)              |
    | /srv        | /root       | OK: 0                                           |
    | /srv        | /run        | OK: 0                                           |
    | /srv        | /sbin       | OK: 0                                           |
    | /srv        | /srv        | OK: 0                                           |
    | /srv        | /sys        | OK: 0                                           |
    | /srv        | /tmp        | OK: 0                                           |
    | /srv        | /usr        | OK: 0                                           |
    | /srv        | /var        | OK: 0                                           |
    | /srv        | \0          | ERR: file name contained an unexpected NUL byte |
    | /srv        | /x/..       | ERR: Invalid argument (os error 28)             |
    | /sys        |             | ERR: Is a directory (os error 31)               |
    | /sys        | .           | ERR: Is a directory (os error 31)               |
    | /sys        | ..          | ERR: Is a directory (os error 31)               |
    | /sys        | /           | ERR: Is a directory (os error 31)               |
    | /sys        | /bin        | OK: 0                                           |
    | /sys        | /boot       | OK: 0                                           |
    | /sys        | /dev        | OK: 0                                           |
    | /sys        | /etc        | OK: 0                                           |
    | /sys        | /etc/group  | ERR: Not a directory (os error 54)              |
    | /sys        | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /sys        | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /sys        | /home       | OK: 0                                           |
    | /sys        | /lib        | OK: 0                                           |
    | /sys        | /lib64      | OK: 0                                           |
    | /sys        | /opt        | OK: 0                                           |
    | /sys        | /proc       | OK: 0                                           |
    | /sys        | /proc/self  | ERR: Not a directory (os error 54)              |
    | /sys        | /root       | OK: 0                                           |
    | /sys        | /run        | OK: 0                                           |
    | /sys        | /sbin       | OK: 0                                           |
    | /sys        | /srv        | OK: 0                                           |
    | /sys        | /sys        | OK: 0                                           |
    | /sys        | /tmp        | OK: 0                                           |
    | /sys        | /usr        | OK: 0                                           |
    | /sys        | /var        | OK: 0                                           |
    | /sys        | \0          | ERR: file name contained an unexpected NUL byte |
    | /sys        | /x/..       | ERR: Invalid argument (os error 28)             |
    | /tmp        |             | ERR: Is a directory (os error 31)               |
    | /tmp        | .           | ERR: Is a directory (os error 31)               |
    | /tmp        | ..          | ERR: Is a directory (os error 31)               |
    | /tmp        | /           | ERR: Is a directory (os error 31)               |
    | /tmp        | /bin        | OK: 0                                           |
    | /tmp        | /boot       | OK: 0                                           |
    | /tmp        | /dev        | OK: 0                                           |
    | /tmp        | /etc        | OK: 0                                           |
    | /tmp        | /etc/group  | ERR: Not a directory (os error 54)              |
    | /tmp        | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /tmp        | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /tmp        | /home       | OK: 0                                           |
    | /tmp        | /lib        | OK: 0                                           |
    | /tmp        | /lib64      | OK: 0                                           |
    | /tmp        | /opt        | OK: 0                                           |
    | /tmp        | /proc       | OK: 0                                           |
    | /tmp        | /proc/self  | ERR: Not a directory (os error 54)              |
    | /tmp        | /root       | OK: 0                                           |
    | /tmp        | /run        | OK: 0                                           |
    | /tmp        | /sbin       | OK: 0                                           |
    | /tmp        | /srv        | OK: 0                                           |
    | /tmp        | /sys        | OK: 0                                           |
    | /tmp        | /tmp        | OK: 0                                           |
    | /tmp        | /usr        | OK: 0                                           |
    | /tmp        | /var        | OK: 0                                           |
    | /tmp        | \0          | ERR: file name contained an unexpected NUL byte |
    | /tmp        | /x/..       | ERR: Invalid argument (os error 28)             |
    | /usr        |             | ERR: Is a directory (os error 31)               |
    | /usr        | .           | ERR: Is a directory (os error 31)               |
    | /usr        | ..          | ERR: Is a directory (os error 31)               |
    | /usr        | /           | ERR: Is a directory (os error 31)               |
    | /usr        | /bin        | OK: 0                                           |
    | /usr        | /boot       | OK: 0                                           |
    | /usr        | /dev        | OK: 0                                           |
    | /usr        | /etc        | OK: 0                                           |
    | /usr        | /etc/group  | ERR: Not a directory (os error 54)              |
    | /usr        | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /usr        | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /usr        | /home       | OK: 0                                           |
    | /usr        | /lib        | OK: 0                                           |
    | /usr        | /lib64      | OK: 0                                           |
    | /usr        | /opt        | OK: 0                                           |
    | /usr        | /proc       | OK: 0                                           |
    | /usr        | /proc/self  | ERR: Not a directory (os error 54)              |
    | /usr        | /root       | OK: 0                                           |
    | /usr        | /run        | OK: 0                                           |
    | /usr        | /sbin       | OK: 0                                           |
    | /usr        | /srv        | OK: 0                                           |
    | /usr        | /sys        | OK: 0                                           |
    | /usr        | /tmp        | OK: 0                                           |
    | /usr        | /usr        | OK: 0                                           |
    | /usr        | /var        | OK: 0                                           |
    | /usr        | \0          | ERR: file name contained an unexpected NUL byte |
    | /usr        | /x/..       | ERR: Invalid argument (os error 28)             |
    | /var        |             | ERR: Is a directory (os error 31)               |
    | /var        | .           | ERR: Is a directory (os error 31)               |
    | /var        | ..          | ERR: Is a directory (os error 31)               |
    | /var        | /           | ERR: Is a directory (os error 31)               |
    | /var        | /bin        | OK: 0                                           |
    | /var        | /boot       | OK: 0                                           |
    | /var        | /dev        | OK: 0                                           |
    | /var        | /etc        | OK: 0                                           |
    | /var        | /etc/group  | ERR: Not a directory (os error 54)              |
    | /var        | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /var        | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /var        | /home       | OK: 0                                           |
    | /var        | /lib        | OK: 0                                           |
    | /var        | /lib64      | OK: 0                                           |
    | /var        | /opt        | OK: 0                                           |
    | /var        | /proc       | OK: 0                                           |
    | /var        | /proc/self  | ERR: Not a directory (os error 54)              |
    | /var        | /root       | OK: 0                                           |
    | /var        | /run        | OK: 0                                           |
    | /var        | /sbin       | OK: 0                                           |
    | /var        | /srv        | OK: 0                                           |
    | /var        | /sys        | OK: 0                                           |
    | /var        | /tmp        | OK: 0                                           |
    | /var        | /usr        | OK: 0                                           |
    | /var        | /var        | OK: 0                                           |
    | /var        | \0          | ERR: file name contained an unexpected NUL byte |
    | /var        | /x/..       | ERR: Invalid argument (os error 28)             |
    | \0          |             | ERR: file name contained an unexpected NUL byte |
    | \0          | .           | ERR: file name contained an unexpected NUL byte |
    | \0          | ..          | ERR: file name contained an unexpected NUL byte |
    | \0          | /           | ERR: file name contained an unexpected NUL byte |
    | \0          | /bin        | ERR: file name contained an unexpected NUL byte |
    | \0          | /boot       | ERR: file name contained an unexpected NUL byte |
    | \0          | /dev        | ERR: file name contained an unexpected NUL byte |
    | \0          | /etc        | ERR: file name contained an unexpected NUL byte |
    | \0          | /etc/group  | ERR: file name contained an unexpected NUL byte |
    | \0          | /etc/passwd | ERR: file name contained an unexpected NUL byte |
    | \0          | /etc/shadow | ERR: file name contained an unexpected NUL byte |
    | \0          | /home       | ERR: file name contained an unexpected NUL byte |
    | \0          | /lib        | ERR: file name contained an unexpected NUL byte |
    | \0          | /lib64      | ERR: file name contained an unexpected NUL byte |
    | \0          | /opt        | ERR: file name contained an unexpected NUL byte |
    | \0          | /proc       | ERR: file name contained an unexpected NUL byte |
    | \0          | /proc/self  | ERR: file name contained an unexpected NUL byte |
    | \0          | /root       | ERR: file name contained an unexpected NUL byte |
    | \0          | /run        | ERR: file name contained an unexpected NUL byte |
    | \0          | /sbin       | ERR: file name contained an unexpected NUL byte |
    | \0          | /srv        | ERR: file name contained an unexpected NUL byte |
    | \0          | /sys        | ERR: file name contained an unexpected NUL byte |
    | \0          | /tmp        | ERR: file name contained an unexpected NUL byte |
    | \0          | /usr        | ERR: file name contained an unexpected NUL byte |
    | \0          | /var        | ERR: file name contained an unexpected NUL byte |
    | \0          | \0          | ERR: file name contained an unexpected NUL byte |
    | \0          | /x/..       | ERR: file name contained an unexpected NUL byte |
    | /x/..       |             | ERR: No such file or directory (os error 44)    |
    | /x/..       | .           | ERR: No such file or directory (os error 44)    |
    | /x/..       | ..          | ERR: No such file or directory (os error 44)    |
    | /x/..       | /           | ERR: No such file or directory (os error 44)    |
    | /x/..       | /bin        | ERR: No such file or directory (os error 44)    |
    | /x/..       | /boot       | ERR: No such file or directory (os error 44)    |
    | /x/..       | /dev        | ERR: No such file or directory (os error 44)    |
    | /x/..       | /etc        | ERR: No such file or directory (os error 44)    |
    | /x/..       | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /x/..       | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /x/..       | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /x/..       | /home       | ERR: No such file or directory (os error 44)    |
    | /x/..       | /lib        | ERR: No such file or directory (os error 44)    |
    | /x/..       | /lib64      | ERR: No such file or directory (os error 44)    |
    | /x/..       | /opt        | ERR: No such file or directory (os error 44)    |
    | /x/..       | /proc       | ERR: No such file or directory (os error 44)    |
    | /x/..       | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /x/..       | /root       | ERR: No such file or directory (os error 44)    |
    | /x/..       | /run        | ERR: No such file or directory (os error 44)    |
    | /x/..       | /sbin       | ERR: No such file or directory (os error 44)    |
    | /x/..       | /srv        | ERR: No such file or directory (os error 44)    |
    | /x/..       | /sys        | ERR: No such file or directory (os error 44)    |
    | /x/..       | /tmp        | ERR: No such file or directory (os error 44)    |
    | /x/..       | /usr        | ERR: No such file or directory (os error 44)    |
    | /x/..       | /var        | ERR: No such file or directory (os error 44)    |
    | /x/..       | \0          | ERR: No such file or directory (os error 44)    |
    | /x/..       | /x/..       | ERR: No such file or directory (os error 44)    |
    +-------------+-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_create_dir() {
    let udf = udf("create_dir").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | ERR: Invalid argument (os error 28)             |
    | .           | ERR: Invalid argument (os error 28)             |
    | ..          | ERR: Invalid argument (os error 28)             |
    | /           | ERR: Invalid argument (os error 28)             |
    | /bin        | OK: created                                     |
    | /boot       | OK: created                                     |
    | /dev        | OK: created                                     |
    | /etc        | OK: created                                     |
    | /etc/group  | OK: created                                     |
    | /etc/passwd | OK: created                                     |
    | /etc/shadow | OK: created                                     |
    | /home       | OK: created                                     |
    | /lib        | OK: created                                     |
    | /lib64      | OK: created                                     |
    | /opt        | OK: created                                     |
    | /proc       | OK: created                                     |
    | /proc/self  | OK: created                                     |
    | /root       | OK: created                                     |
    | /run        | OK: created                                     |
    | /sbin       | OK: created                                     |
    | /srv        | OK: created                                     |
    | /sys        | OK: created                                     |
    | /tmp        | OK: created                                     |
    | /usr        | OK: created                                     |
    | /var        | OK: created                                     |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | ERR: Invalid argument (os error 28)             |
    +-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_exists() {
    let udf = udf("exists").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | OK: true                                        |
    | .           | OK: true                                        |
    | ..          | OK: true                                        |
    | /           | OK: true                                        |
    | /bin        | OK: false                                       |
    | /boot       | OK: false                                       |
    | /dev        | OK: false                                       |
    | /etc        | OK: false                                       |
    | /etc/group  | OK: false                                       |
    | /etc/passwd | OK: false                                       |
    | /etc/shadow | OK: false                                       |
    | /home       | OK: false                                       |
    | /lib        | OK: false                                       |
    | /lib64      | OK: false                                       |
    | /opt        | OK: false                                       |
    | /proc       | OK: false                                       |
    | /proc/self  | OK: false                                       |
    | /root       | OK: false                                       |
    | /run        | OK: false                                       |
    | /sbin       | OK: false                                       |
    | /srv        | OK: false                                       |
    | /sys        | OK: false                                       |
    | /tmp        | OK: false                                       |
    | /usr        | OK: false                                       |
    | /var        | OK: false                                       |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | OK: false                                       |
    +-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_hard_link() {
    let udf = udf("hard_link").await;

    insta::assert_snapshot!(
        run_2(&udf).await,
        @r"
    +-------------+-------------+-------------------------------------------------+
    | from        | to          | output                                          |
    +-------------+-------------+-------------------------------------------------+
    |             |             | ERR: Read-only file system (os error 69)        |
    |             | .           | ERR: Read-only file system (os error 69)        |
    |             | ..          | ERR: Read-only file system (os error 69)        |
    |             | /           | ERR: Read-only file system (os error 69)        |
    |             | /bin        | ERR: Read-only file system (os error 69)        |
    |             | /boot       | ERR: Read-only file system (os error 69)        |
    |             | /dev        | ERR: Read-only file system (os error 69)        |
    |             | /etc        | ERR: Read-only file system (os error 69)        |
    |             | /etc/group  | ERR: Read-only file system (os error 69)        |
    |             | /etc/passwd | ERR: Read-only file system (os error 69)        |
    |             | /etc/shadow | ERR: Read-only file system (os error 69)        |
    |             | /home       | ERR: Read-only file system (os error 69)        |
    |             | /lib        | ERR: Read-only file system (os error 69)        |
    |             | /lib64      | ERR: Read-only file system (os error 69)        |
    |             | /opt        | ERR: Read-only file system (os error 69)        |
    |             | /proc       | ERR: Read-only file system (os error 69)        |
    |             | /proc/self  | ERR: Read-only file system (os error 69)        |
    |             | /root       | ERR: Read-only file system (os error 69)        |
    |             | /run        | ERR: Read-only file system (os error 69)        |
    |             | /sbin       | ERR: Read-only file system (os error 69)        |
    |             | /srv        | ERR: Read-only file system (os error 69)        |
    |             | /sys        | ERR: Read-only file system (os error 69)        |
    |             | /tmp        | ERR: Read-only file system (os error 69)        |
    |             | /usr        | ERR: Read-only file system (os error 69)        |
    |             | /var        | ERR: Read-only file system (os error 69)        |
    |             | \0          | ERR: file name contained an unexpected NUL byte |
    |             | /x/..       | ERR: Read-only file system (os error 69)        |
    | .           |             | ERR: Read-only file system (os error 69)        |
    | .           | .           | ERR: Read-only file system (os error 69)        |
    | .           | ..          | ERR: Read-only file system (os error 69)        |
    | .           | /           | ERR: Read-only file system (os error 69)        |
    | .           | /bin        | ERR: Read-only file system (os error 69)        |
    | .           | /boot       | ERR: Read-only file system (os error 69)        |
    | .           | /dev        | ERR: Read-only file system (os error 69)        |
    | .           | /etc        | ERR: Read-only file system (os error 69)        |
    | .           | /etc/group  | ERR: Read-only file system (os error 69)        |
    | .           | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | .           | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | .           | /home       | ERR: Read-only file system (os error 69)        |
    | .           | /lib        | ERR: Read-only file system (os error 69)        |
    | .           | /lib64      | ERR: Read-only file system (os error 69)        |
    | .           | /opt        | ERR: Read-only file system (os error 69)        |
    | .           | /proc       | ERR: Read-only file system (os error 69)        |
    | .           | /proc/self  | ERR: Read-only file system (os error 69)        |
    | .           | /root       | ERR: Read-only file system (os error 69)        |
    | .           | /run        | ERR: Read-only file system (os error 69)        |
    | .           | /sbin       | ERR: Read-only file system (os error 69)        |
    | .           | /srv        | ERR: Read-only file system (os error 69)        |
    | .           | /sys        | ERR: Read-only file system (os error 69)        |
    | .           | /tmp        | ERR: Read-only file system (os error 69)        |
    | .           | /usr        | ERR: Read-only file system (os error 69)        |
    | .           | /var        | ERR: Read-only file system (os error 69)        |
    | .           | \0          | ERR: file name contained an unexpected NUL byte |
    | .           | /x/..       | ERR: Read-only file system (os error 69)        |
    | ..          |             | ERR: Read-only file system (os error 69)        |
    | ..          | .           | ERR: Read-only file system (os error 69)        |
    | ..          | ..          | ERR: Read-only file system (os error 69)        |
    | ..          | /           | ERR: Read-only file system (os error 69)        |
    | ..          | /bin        | ERR: Read-only file system (os error 69)        |
    | ..          | /boot       | ERR: Read-only file system (os error 69)        |
    | ..          | /dev        | ERR: Read-only file system (os error 69)        |
    | ..          | /etc        | ERR: Read-only file system (os error 69)        |
    | ..          | /etc/group  | ERR: Read-only file system (os error 69)        |
    | ..          | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | ..          | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | ..          | /home       | ERR: Read-only file system (os error 69)        |
    | ..          | /lib        | ERR: Read-only file system (os error 69)        |
    | ..          | /lib64      | ERR: Read-only file system (os error 69)        |
    | ..          | /opt        | ERR: Read-only file system (os error 69)        |
    | ..          | /proc       | ERR: Read-only file system (os error 69)        |
    | ..          | /proc/self  | ERR: Read-only file system (os error 69)        |
    | ..          | /root       | ERR: Read-only file system (os error 69)        |
    | ..          | /run        | ERR: Read-only file system (os error 69)        |
    | ..          | /sbin       | ERR: Read-only file system (os error 69)        |
    | ..          | /srv        | ERR: Read-only file system (os error 69)        |
    | ..          | /sys        | ERR: Read-only file system (os error 69)        |
    | ..          | /tmp        | ERR: Read-only file system (os error 69)        |
    | ..          | /usr        | ERR: Read-only file system (os error 69)        |
    | ..          | /var        | ERR: Read-only file system (os error 69)        |
    | ..          | \0          | ERR: file name contained an unexpected NUL byte |
    | ..          | /x/..       | ERR: Read-only file system (os error 69)        |
    | /           |             | ERR: Read-only file system (os error 69)        |
    | /           | .           | ERR: Read-only file system (os error 69)        |
    | /           | ..          | ERR: Read-only file system (os error 69)        |
    | /           | /           | ERR: Read-only file system (os error 69)        |
    | /           | /bin        | ERR: Read-only file system (os error 69)        |
    | /           | /boot       | ERR: Read-only file system (os error 69)        |
    | /           | /dev        | ERR: Read-only file system (os error 69)        |
    | /           | /etc        | ERR: Read-only file system (os error 69)        |
    | /           | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /           | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /           | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /           | /home       | ERR: Read-only file system (os error 69)        |
    | /           | /lib        | ERR: Read-only file system (os error 69)        |
    | /           | /lib64      | ERR: Read-only file system (os error 69)        |
    | /           | /opt        | ERR: Read-only file system (os error 69)        |
    | /           | /proc       | ERR: Read-only file system (os error 69)        |
    | /           | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /           | /root       | ERR: Read-only file system (os error 69)        |
    | /           | /run        | ERR: Read-only file system (os error 69)        |
    | /           | /sbin       | ERR: Read-only file system (os error 69)        |
    | /           | /srv        | ERR: Read-only file system (os error 69)        |
    | /           | /sys        | ERR: Read-only file system (os error 69)        |
    | /           | /tmp        | ERR: Read-only file system (os error 69)        |
    | /           | /usr        | ERR: Read-only file system (os error 69)        |
    | /           | /var        | ERR: Read-only file system (os error 69)        |
    | /           | \0          | ERR: file name contained an unexpected NUL byte |
    | /           | /x/..       | ERR: Read-only file system (os error 69)        |
    | /bin        |             | ERR: Read-only file system (os error 69)        |
    | /bin        | .           | ERR: Read-only file system (os error 69)        |
    | /bin        | ..          | ERR: Read-only file system (os error 69)        |
    | /bin        | /           | ERR: Read-only file system (os error 69)        |
    | /bin        | /bin        | ERR: Read-only file system (os error 69)        |
    | /bin        | /boot       | ERR: Read-only file system (os error 69)        |
    | /bin        | /dev        | ERR: Read-only file system (os error 69)        |
    | /bin        | /etc        | ERR: Read-only file system (os error 69)        |
    | /bin        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /bin        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /bin        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /bin        | /home       | ERR: Read-only file system (os error 69)        |
    | /bin        | /lib        | ERR: Read-only file system (os error 69)        |
    | /bin        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /bin        | /opt        | ERR: Read-only file system (os error 69)        |
    | /bin        | /proc       | ERR: Read-only file system (os error 69)        |
    | /bin        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /bin        | /root       | ERR: Read-only file system (os error 69)        |
    | /bin        | /run        | ERR: Read-only file system (os error 69)        |
    | /bin        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /bin        | /srv        | ERR: Read-only file system (os error 69)        |
    | /bin        | /sys        | ERR: Read-only file system (os error 69)        |
    | /bin        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /bin        | /usr        | ERR: Read-only file system (os error 69)        |
    | /bin        | /var        | ERR: Read-only file system (os error 69)        |
    | /bin        | \0          | ERR: file name contained an unexpected NUL byte |
    | /bin        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /boot       |             | ERR: Read-only file system (os error 69)        |
    | /boot       | .           | ERR: Read-only file system (os error 69)        |
    | /boot       | ..          | ERR: Read-only file system (os error 69)        |
    | /boot       | /           | ERR: Read-only file system (os error 69)        |
    | /boot       | /bin        | ERR: Read-only file system (os error 69)        |
    | /boot       | /boot       | ERR: Read-only file system (os error 69)        |
    | /boot       | /dev        | ERR: Read-only file system (os error 69)        |
    | /boot       | /etc        | ERR: Read-only file system (os error 69)        |
    | /boot       | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /boot       | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /boot       | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /boot       | /home       | ERR: Read-only file system (os error 69)        |
    | /boot       | /lib        | ERR: Read-only file system (os error 69)        |
    | /boot       | /lib64      | ERR: Read-only file system (os error 69)        |
    | /boot       | /opt        | ERR: Read-only file system (os error 69)        |
    | /boot       | /proc       | ERR: Read-only file system (os error 69)        |
    | /boot       | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /boot       | /root       | ERR: Read-only file system (os error 69)        |
    | /boot       | /run        | ERR: Read-only file system (os error 69)        |
    | /boot       | /sbin       | ERR: Read-only file system (os error 69)        |
    | /boot       | /srv        | ERR: Read-only file system (os error 69)        |
    | /boot       | /sys        | ERR: Read-only file system (os error 69)        |
    | /boot       | /tmp        | ERR: Read-only file system (os error 69)        |
    | /boot       | /usr        | ERR: Read-only file system (os error 69)        |
    | /boot       | /var        | ERR: Read-only file system (os error 69)        |
    | /boot       | \0          | ERR: file name contained an unexpected NUL byte |
    | /boot       | /x/..       | ERR: Read-only file system (os error 69)        |
    | /dev        |             | ERR: Read-only file system (os error 69)        |
    | /dev        | .           | ERR: Read-only file system (os error 69)        |
    | /dev        | ..          | ERR: Read-only file system (os error 69)        |
    | /dev        | /           | ERR: Read-only file system (os error 69)        |
    | /dev        | /bin        | ERR: Read-only file system (os error 69)        |
    | /dev        | /boot       | ERR: Read-only file system (os error 69)        |
    | /dev        | /dev        | ERR: Read-only file system (os error 69)        |
    | /dev        | /etc        | ERR: Read-only file system (os error 69)        |
    | /dev        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /dev        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /dev        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /dev        | /home       | ERR: Read-only file system (os error 69)        |
    | /dev        | /lib        | ERR: Read-only file system (os error 69)        |
    | /dev        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /dev        | /opt        | ERR: Read-only file system (os error 69)        |
    | /dev        | /proc       | ERR: Read-only file system (os error 69)        |
    | /dev        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /dev        | /root       | ERR: Read-only file system (os error 69)        |
    | /dev        | /run        | ERR: Read-only file system (os error 69)        |
    | /dev        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /dev        | /srv        | ERR: Read-only file system (os error 69)        |
    | /dev        | /sys        | ERR: Read-only file system (os error 69)        |
    | /dev        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /dev        | /usr        | ERR: Read-only file system (os error 69)        |
    | /dev        | /var        | ERR: Read-only file system (os error 69)        |
    | /dev        | \0          | ERR: file name contained an unexpected NUL byte |
    | /dev        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /etc        |             | ERR: Read-only file system (os error 69)        |
    | /etc        | .           | ERR: Read-only file system (os error 69)        |
    | /etc        | ..          | ERR: Read-only file system (os error 69)        |
    | /etc        | /           | ERR: Read-only file system (os error 69)        |
    | /etc        | /bin        | ERR: Read-only file system (os error 69)        |
    | /etc        | /boot       | ERR: Read-only file system (os error 69)        |
    | /etc        | /dev        | ERR: Read-only file system (os error 69)        |
    | /etc        | /etc        | ERR: Read-only file system (os error 69)        |
    | /etc        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /etc        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /etc        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /etc        | /home       | ERR: Read-only file system (os error 69)        |
    | /etc        | /lib        | ERR: Read-only file system (os error 69)        |
    | /etc        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /etc        | /opt        | ERR: Read-only file system (os error 69)        |
    | /etc        | /proc       | ERR: Read-only file system (os error 69)        |
    | /etc        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /etc        | /root       | ERR: Read-only file system (os error 69)        |
    | /etc        | /run        | ERR: Read-only file system (os error 69)        |
    | /etc        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /etc        | /srv        | ERR: Read-only file system (os error 69)        |
    | /etc        | /sys        | ERR: Read-only file system (os error 69)        |
    | /etc        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /etc        | /usr        | ERR: Read-only file system (os error 69)        |
    | /etc        | /var        | ERR: Read-only file system (os error 69)        |
    | /etc        | \0          | ERR: file name contained an unexpected NUL byte |
    | /etc        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /etc/group  |             | ERR: Read-only file system (os error 69)        |
    | /etc/group  | .           | ERR: Read-only file system (os error 69)        |
    | /etc/group  | ..          | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /           | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /bin        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /boot       | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /dev        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /etc        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /home       | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /lib        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /lib64      | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /opt        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /proc       | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /root       | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /run        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /sbin       | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /srv        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /sys        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /tmp        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /usr        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /var        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | \0          | ERR: file name contained an unexpected NUL byte |
    | /etc/group  | /x/..       | ERR: Read-only file system (os error 69)        |
    | /etc/passwd |             | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | .           | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | ..          | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /           | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /bin        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /boot       | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /dev        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /etc        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /home       | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /lib        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /lib64      | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /opt        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /proc       | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /root       | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /run        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /sbin       | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /srv        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /sys        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /tmp        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /usr        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /var        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | \0          | ERR: file name contained an unexpected NUL byte |
    | /etc/passwd | /x/..       | ERR: Read-only file system (os error 69)        |
    | /etc/shadow |             | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | .           | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | ..          | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /           | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /bin        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /boot       | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /dev        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /etc        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /home       | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /lib        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /lib64      | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /opt        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /proc       | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /root       | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /run        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /sbin       | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /srv        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /sys        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /tmp        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /usr        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /var        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | \0          | ERR: file name contained an unexpected NUL byte |
    | /etc/shadow | /x/..       | ERR: Read-only file system (os error 69)        |
    | /home       |             | ERR: Read-only file system (os error 69)        |
    | /home       | .           | ERR: Read-only file system (os error 69)        |
    | /home       | ..          | ERR: Read-only file system (os error 69)        |
    | /home       | /           | ERR: Read-only file system (os error 69)        |
    | /home       | /bin        | ERR: Read-only file system (os error 69)        |
    | /home       | /boot       | ERR: Read-only file system (os error 69)        |
    | /home       | /dev        | ERR: Read-only file system (os error 69)        |
    | /home       | /etc        | ERR: Read-only file system (os error 69)        |
    | /home       | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /home       | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /home       | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /home       | /home       | ERR: Read-only file system (os error 69)        |
    | /home       | /lib        | ERR: Read-only file system (os error 69)        |
    | /home       | /lib64      | ERR: Read-only file system (os error 69)        |
    | /home       | /opt        | ERR: Read-only file system (os error 69)        |
    | /home       | /proc       | ERR: Read-only file system (os error 69)        |
    | /home       | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /home       | /root       | ERR: Read-only file system (os error 69)        |
    | /home       | /run        | ERR: Read-only file system (os error 69)        |
    | /home       | /sbin       | ERR: Read-only file system (os error 69)        |
    | /home       | /srv        | ERR: Read-only file system (os error 69)        |
    | /home       | /sys        | ERR: Read-only file system (os error 69)        |
    | /home       | /tmp        | ERR: Read-only file system (os error 69)        |
    | /home       | /usr        | ERR: Read-only file system (os error 69)        |
    | /home       | /var        | ERR: Read-only file system (os error 69)        |
    | /home       | \0          | ERR: file name contained an unexpected NUL byte |
    | /home       | /x/..       | ERR: Read-only file system (os error 69)        |
    | /lib        |             | ERR: Read-only file system (os error 69)        |
    | /lib        | .           | ERR: Read-only file system (os error 69)        |
    | /lib        | ..          | ERR: Read-only file system (os error 69)        |
    | /lib        | /           | ERR: Read-only file system (os error 69)        |
    | /lib        | /bin        | ERR: Read-only file system (os error 69)        |
    | /lib        | /boot       | ERR: Read-only file system (os error 69)        |
    | /lib        | /dev        | ERR: Read-only file system (os error 69)        |
    | /lib        | /etc        | ERR: Read-only file system (os error 69)        |
    | /lib        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /lib        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /lib        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /lib        | /home       | ERR: Read-only file system (os error 69)        |
    | /lib        | /lib        | ERR: Read-only file system (os error 69)        |
    | /lib        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /lib        | /opt        | ERR: Read-only file system (os error 69)        |
    | /lib        | /proc       | ERR: Read-only file system (os error 69)        |
    | /lib        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /lib        | /root       | ERR: Read-only file system (os error 69)        |
    | /lib        | /run        | ERR: Read-only file system (os error 69)        |
    | /lib        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /lib        | /srv        | ERR: Read-only file system (os error 69)        |
    | /lib        | /sys        | ERR: Read-only file system (os error 69)        |
    | /lib        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /lib        | /usr        | ERR: Read-only file system (os error 69)        |
    | /lib        | /var        | ERR: Read-only file system (os error 69)        |
    | /lib        | \0          | ERR: file name contained an unexpected NUL byte |
    | /lib        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /lib64      |             | ERR: Read-only file system (os error 69)        |
    | /lib64      | .           | ERR: Read-only file system (os error 69)        |
    | /lib64      | ..          | ERR: Read-only file system (os error 69)        |
    | /lib64      | /           | ERR: Read-only file system (os error 69)        |
    | /lib64      | /bin        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /boot       | ERR: Read-only file system (os error 69)        |
    | /lib64      | /dev        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /etc        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /lib64      | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /lib64      | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /lib64      | /home       | ERR: Read-only file system (os error 69)        |
    | /lib64      | /lib        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /lib64      | ERR: Read-only file system (os error 69)        |
    | /lib64      | /opt        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /proc       | ERR: Read-only file system (os error 69)        |
    | /lib64      | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /lib64      | /root       | ERR: Read-only file system (os error 69)        |
    | /lib64      | /run        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /sbin       | ERR: Read-only file system (os error 69)        |
    | /lib64      | /srv        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /sys        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /tmp        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /usr        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /var        | ERR: Read-only file system (os error 69)        |
    | /lib64      | \0          | ERR: file name contained an unexpected NUL byte |
    | /lib64      | /x/..       | ERR: Read-only file system (os error 69)        |
    | /opt        |             | ERR: Read-only file system (os error 69)        |
    | /opt        | .           | ERR: Read-only file system (os error 69)        |
    | /opt        | ..          | ERR: Read-only file system (os error 69)        |
    | /opt        | /           | ERR: Read-only file system (os error 69)        |
    | /opt        | /bin        | ERR: Read-only file system (os error 69)        |
    | /opt        | /boot       | ERR: Read-only file system (os error 69)        |
    | /opt        | /dev        | ERR: Read-only file system (os error 69)        |
    | /opt        | /etc        | ERR: Read-only file system (os error 69)        |
    | /opt        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /opt        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /opt        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /opt        | /home       | ERR: Read-only file system (os error 69)        |
    | /opt        | /lib        | ERR: Read-only file system (os error 69)        |
    | /opt        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /opt        | /opt        | ERR: Read-only file system (os error 69)        |
    | /opt        | /proc       | ERR: Read-only file system (os error 69)        |
    | /opt        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /opt        | /root       | ERR: Read-only file system (os error 69)        |
    | /opt        | /run        | ERR: Read-only file system (os error 69)        |
    | /opt        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /opt        | /srv        | ERR: Read-only file system (os error 69)        |
    | /opt        | /sys        | ERR: Read-only file system (os error 69)        |
    | /opt        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /opt        | /usr        | ERR: Read-only file system (os error 69)        |
    | /opt        | /var        | ERR: Read-only file system (os error 69)        |
    | /opt        | \0          | ERR: file name contained an unexpected NUL byte |
    | /opt        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /proc       |             | ERR: Read-only file system (os error 69)        |
    | /proc       | .           | ERR: Read-only file system (os error 69)        |
    | /proc       | ..          | ERR: Read-only file system (os error 69)        |
    | /proc       | /           | ERR: Read-only file system (os error 69)        |
    | /proc       | /bin        | ERR: Read-only file system (os error 69)        |
    | /proc       | /boot       | ERR: Read-only file system (os error 69)        |
    | /proc       | /dev        | ERR: Read-only file system (os error 69)        |
    | /proc       | /etc        | ERR: Read-only file system (os error 69)        |
    | /proc       | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /proc       | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /proc       | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /proc       | /home       | ERR: Read-only file system (os error 69)        |
    | /proc       | /lib        | ERR: Read-only file system (os error 69)        |
    | /proc       | /lib64      | ERR: Read-only file system (os error 69)        |
    | /proc       | /opt        | ERR: Read-only file system (os error 69)        |
    | /proc       | /proc       | ERR: Read-only file system (os error 69)        |
    | /proc       | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /proc       | /root       | ERR: Read-only file system (os error 69)        |
    | /proc       | /run        | ERR: Read-only file system (os error 69)        |
    | /proc       | /sbin       | ERR: Read-only file system (os error 69)        |
    | /proc       | /srv        | ERR: Read-only file system (os error 69)        |
    | /proc       | /sys        | ERR: Read-only file system (os error 69)        |
    | /proc       | /tmp        | ERR: Read-only file system (os error 69)        |
    | /proc       | /usr        | ERR: Read-only file system (os error 69)        |
    | /proc       | /var        | ERR: Read-only file system (os error 69)        |
    | /proc       | \0          | ERR: file name contained an unexpected NUL byte |
    | /proc       | /x/..       | ERR: Read-only file system (os error 69)        |
    | /proc/self  |             | ERR: Read-only file system (os error 69)        |
    | /proc/self  | .           | ERR: Read-only file system (os error 69)        |
    | /proc/self  | ..          | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /           | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /bin        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /boot       | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /dev        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /etc        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /home       | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /lib        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /lib64      | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /opt        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /proc       | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /root       | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /run        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /sbin       | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /srv        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /sys        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /tmp        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /usr        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /var        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | \0          | ERR: file name contained an unexpected NUL byte |
    | /proc/self  | /x/..       | ERR: Read-only file system (os error 69)        |
    | /root       |             | ERR: Read-only file system (os error 69)        |
    | /root       | .           | ERR: Read-only file system (os error 69)        |
    | /root       | ..          | ERR: Read-only file system (os error 69)        |
    | /root       | /           | ERR: Read-only file system (os error 69)        |
    | /root       | /bin        | ERR: Read-only file system (os error 69)        |
    | /root       | /boot       | ERR: Read-only file system (os error 69)        |
    | /root       | /dev        | ERR: Read-only file system (os error 69)        |
    | /root       | /etc        | ERR: Read-only file system (os error 69)        |
    | /root       | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /root       | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /root       | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /root       | /home       | ERR: Read-only file system (os error 69)        |
    | /root       | /lib        | ERR: Read-only file system (os error 69)        |
    | /root       | /lib64      | ERR: Read-only file system (os error 69)        |
    | /root       | /opt        | ERR: Read-only file system (os error 69)        |
    | /root       | /proc       | ERR: Read-only file system (os error 69)        |
    | /root       | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /root       | /root       | ERR: Read-only file system (os error 69)        |
    | /root       | /run        | ERR: Read-only file system (os error 69)        |
    | /root       | /sbin       | ERR: Read-only file system (os error 69)        |
    | /root       | /srv        | ERR: Read-only file system (os error 69)        |
    | /root       | /sys        | ERR: Read-only file system (os error 69)        |
    | /root       | /tmp        | ERR: Read-only file system (os error 69)        |
    | /root       | /usr        | ERR: Read-only file system (os error 69)        |
    | /root       | /var        | ERR: Read-only file system (os error 69)        |
    | /root       | \0          | ERR: file name contained an unexpected NUL byte |
    | /root       | /x/..       | ERR: Read-only file system (os error 69)        |
    | /run        |             | ERR: Read-only file system (os error 69)        |
    | /run        | .           | ERR: Read-only file system (os error 69)        |
    | /run        | ..          | ERR: Read-only file system (os error 69)        |
    | /run        | /           | ERR: Read-only file system (os error 69)        |
    | /run        | /bin        | ERR: Read-only file system (os error 69)        |
    | /run        | /boot       | ERR: Read-only file system (os error 69)        |
    | /run        | /dev        | ERR: Read-only file system (os error 69)        |
    | /run        | /etc        | ERR: Read-only file system (os error 69)        |
    | /run        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /run        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /run        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /run        | /home       | ERR: Read-only file system (os error 69)        |
    | /run        | /lib        | ERR: Read-only file system (os error 69)        |
    | /run        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /run        | /opt        | ERR: Read-only file system (os error 69)        |
    | /run        | /proc       | ERR: Read-only file system (os error 69)        |
    | /run        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /run        | /root       | ERR: Read-only file system (os error 69)        |
    | /run        | /run        | ERR: Read-only file system (os error 69)        |
    | /run        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /run        | /srv        | ERR: Read-only file system (os error 69)        |
    | /run        | /sys        | ERR: Read-only file system (os error 69)        |
    | /run        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /run        | /usr        | ERR: Read-only file system (os error 69)        |
    | /run        | /var        | ERR: Read-only file system (os error 69)        |
    | /run        | \0          | ERR: file name contained an unexpected NUL byte |
    | /run        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /sbin       |             | ERR: Read-only file system (os error 69)        |
    | /sbin       | .           | ERR: Read-only file system (os error 69)        |
    | /sbin       | ..          | ERR: Read-only file system (os error 69)        |
    | /sbin       | /           | ERR: Read-only file system (os error 69)        |
    | /sbin       | /bin        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /boot       | ERR: Read-only file system (os error 69)        |
    | /sbin       | /dev        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /etc        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /sbin       | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /sbin       | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /sbin       | /home       | ERR: Read-only file system (os error 69)        |
    | /sbin       | /lib        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /lib64      | ERR: Read-only file system (os error 69)        |
    | /sbin       | /opt        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /proc       | ERR: Read-only file system (os error 69)        |
    | /sbin       | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /sbin       | /root       | ERR: Read-only file system (os error 69)        |
    | /sbin       | /run        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /sbin       | ERR: Read-only file system (os error 69)        |
    | /sbin       | /srv        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /sys        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /tmp        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /usr        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /var        | ERR: Read-only file system (os error 69)        |
    | /sbin       | \0          | ERR: file name contained an unexpected NUL byte |
    | /sbin       | /x/..       | ERR: Read-only file system (os error 69)        |
    | /srv        |             | ERR: Read-only file system (os error 69)        |
    | /srv        | .           | ERR: Read-only file system (os error 69)        |
    | /srv        | ..          | ERR: Read-only file system (os error 69)        |
    | /srv        | /           | ERR: Read-only file system (os error 69)        |
    | /srv        | /bin        | ERR: Read-only file system (os error 69)        |
    | /srv        | /boot       | ERR: Read-only file system (os error 69)        |
    | /srv        | /dev        | ERR: Read-only file system (os error 69)        |
    | /srv        | /etc        | ERR: Read-only file system (os error 69)        |
    | /srv        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /srv        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /srv        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /srv        | /home       | ERR: Read-only file system (os error 69)        |
    | /srv        | /lib        | ERR: Read-only file system (os error 69)        |
    | /srv        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /srv        | /opt        | ERR: Read-only file system (os error 69)        |
    | /srv        | /proc       | ERR: Read-only file system (os error 69)        |
    | /srv        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /srv        | /root       | ERR: Read-only file system (os error 69)        |
    | /srv        | /run        | ERR: Read-only file system (os error 69)        |
    | /srv        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /srv        | /srv        | ERR: Read-only file system (os error 69)        |
    | /srv        | /sys        | ERR: Read-only file system (os error 69)        |
    | /srv        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /srv        | /usr        | ERR: Read-only file system (os error 69)        |
    | /srv        | /var        | ERR: Read-only file system (os error 69)        |
    | /srv        | \0          | ERR: file name contained an unexpected NUL byte |
    | /srv        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /sys        |             | ERR: Read-only file system (os error 69)        |
    | /sys        | .           | ERR: Read-only file system (os error 69)        |
    | /sys        | ..          | ERR: Read-only file system (os error 69)        |
    | /sys        | /           | ERR: Read-only file system (os error 69)        |
    | /sys        | /bin        | ERR: Read-only file system (os error 69)        |
    | /sys        | /boot       | ERR: Read-only file system (os error 69)        |
    | /sys        | /dev        | ERR: Read-only file system (os error 69)        |
    | /sys        | /etc        | ERR: Read-only file system (os error 69)        |
    | /sys        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /sys        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /sys        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /sys        | /home       | ERR: Read-only file system (os error 69)        |
    | /sys        | /lib        | ERR: Read-only file system (os error 69)        |
    | /sys        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /sys        | /opt        | ERR: Read-only file system (os error 69)        |
    | /sys        | /proc       | ERR: Read-only file system (os error 69)        |
    | /sys        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /sys        | /root       | ERR: Read-only file system (os error 69)        |
    | /sys        | /run        | ERR: Read-only file system (os error 69)        |
    | /sys        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /sys        | /srv        | ERR: Read-only file system (os error 69)        |
    | /sys        | /sys        | ERR: Read-only file system (os error 69)        |
    | /sys        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /sys        | /usr        | ERR: Read-only file system (os error 69)        |
    | /sys        | /var        | ERR: Read-only file system (os error 69)        |
    | /sys        | \0          | ERR: file name contained an unexpected NUL byte |
    | /sys        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /tmp        |             | ERR: Read-only file system (os error 69)        |
    | /tmp        | .           | ERR: Read-only file system (os error 69)        |
    | /tmp        | ..          | ERR: Read-only file system (os error 69)        |
    | /tmp        | /           | ERR: Read-only file system (os error 69)        |
    | /tmp        | /bin        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /boot       | ERR: Read-only file system (os error 69)        |
    | /tmp        | /dev        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /etc        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /tmp        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /tmp        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /tmp        | /home       | ERR: Read-only file system (os error 69)        |
    | /tmp        | /lib        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /tmp        | /opt        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /proc       | ERR: Read-only file system (os error 69)        |
    | /tmp        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /tmp        | /root       | ERR: Read-only file system (os error 69)        |
    | /tmp        | /run        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /tmp        | /srv        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /sys        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /usr        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /var        | ERR: Read-only file system (os error 69)        |
    | /tmp        | \0          | ERR: file name contained an unexpected NUL byte |
    | /tmp        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /usr        |             | ERR: Read-only file system (os error 69)        |
    | /usr        | .           | ERR: Read-only file system (os error 69)        |
    | /usr        | ..          | ERR: Read-only file system (os error 69)        |
    | /usr        | /           | ERR: Read-only file system (os error 69)        |
    | /usr        | /bin        | ERR: Read-only file system (os error 69)        |
    | /usr        | /boot       | ERR: Read-only file system (os error 69)        |
    | /usr        | /dev        | ERR: Read-only file system (os error 69)        |
    | /usr        | /etc        | ERR: Read-only file system (os error 69)        |
    | /usr        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /usr        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /usr        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /usr        | /home       | ERR: Read-only file system (os error 69)        |
    | /usr        | /lib        | ERR: Read-only file system (os error 69)        |
    | /usr        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /usr        | /opt        | ERR: Read-only file system (os error 69)        |
    | /usr        | /proc       | ERR: Read-only file system (os error 69)        |
    | /usr        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /usr        | /root       | ERR: Read-only file system (os error 69)        |
    | /usr        | /run        | ERR: Read-only file system (os error 69)        |
    | /usr        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /usr        | /srv        | ERR: Read-only file system (os error 69)        |
    | /usr        | /sys        | ERR: Read-only file system (os error 69)        |
    | /usr        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /usr        | /usr        | ERR: Read-only file system (os error 69)        |
    | /usr        | /var        | ERR: Read-only file system (os error 69)        |
    | /usr        | \0          | ERR: file name contained an unexpected NUL byte |
    | /usr        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /var        |             | ERR: Read-only file system (os error 69)        |
    | /var        | .           | ERR: Read-only file system (os error 69)        |
    | /var        | ..          | ERR: Read-only file system (os error 69)        |
    | /var        | /           | ERR: Read-only file system (os error 69)        |
    | /var        | /bin        | ERR: Read-only file system (os error 69)        |
    | /var        | /boot       | ERR: Read-only file system (os error 69)        |
    | /var        | /dev        | ERR: Read-only file system (os error 69)        |
    | /var        | /etc        | ERR: Read-only file system (os error 69)        |
    | /var        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /var        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /var        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /var        | /home       | ERR: Read-only file system (os error 69)        |
    | /var        | /lib        | ERR: Read-only file system (os error 69)        |
    | /var        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /var        | /opt        | ERR: Read-only file system (os error 69)        |
    | /var        | /proc       | ERR: Read-only file system (os error 69)        |
    | /var        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /var        | /root       | ERR: Read-only file system (os error 69)        |
    | /var        | /run        | ERR: Read-only file system (os error 69)        |
    | /var        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /var        | /srv        | ERR: Read-only file system (os error 69)        |
    | /var        | /sys        | ERR: Read-only file system (os error 69)        |
    | /var        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /var        | /usr        | ERR: Read-only file system (os error 69)        |
    | /var        | /var        | ERR: Read-only file system (os error 69)        |
    | /var        | \0          | ERR: file name contained an unexpected NUL byte |
    | /var        | /x/..       | ERR: Read-only file system (os error 69)        |
    | \0          |             | ERR: file name contained an unexpected NUL byte |
    | \0          | .           | ERR: file name contained an unexpected NUL byte |
    | \0          | ..          | ERR: file name contained an unexpected NUL byte |
    | \0          | /           | ERR: file name contained an unexpected NUL byte |
    | \0          | /bin        | ERR: file name contained an unexpected NUL byte |
    | \0          | /boot       | ERR: file name contained an unexpected NUL byte |
    | \0          | /dev        | ERR: file name contained an unexpected NUL byte |
    | \0          | /etc        | ERR: file name contained an unexpected NUL byte |
    | \0          | /etc/group  | ERR: file name contained an unexpected NUL byte |
    | \0          | /etc/passwd | ERR: file name contained an unexpected NUL byte |
    | \0          | /etc/shadow | ERR: file name contained an unexpected NUL byte |
    | \0          | /home       | ERR: file name contained an unexpected NUL byte |
    | \0          | /lib        | ERR: file name contained an unexpected NUL byte |
    | \0          | /lib64      | ERR: file name contained an unexpected NUL byte |
    | \0          | /opt        | ERR: file name contained an unexpected NUL byte |
    | \0          | /proc       | ERR: file name contained an unexpected NUL byte |
    | \0          | /proc/self  | ERR: file name contained an unexpected NUL byte |
    | \0          | /root       | ERR: file name contained an unexpected NUL byte |
    | \0          | /run        | ERR: file name contained an unexpected NUL byte |
    | \0          | /sbin       | ERR: file name contained an unexpected NUL byte |
    | \0          | /srv        | ERR: file name contained an unexpected NUL byte |
    | \0          | /sys        | ERR: file name contained an unexpected NUL byte |
    | \0          | /tmp        | ERR: file name contained an unexpected NUL byte |
    | \0          | /usr        | ERR: file name contained an unexpected NUL byte |
    | \0          | /var        | ERR: file name contained an unexpected NUL byte |
    | \0          | \0          | ERR: file name contained an unexpected NUL byte |
    | \0          | /x/..       | ERR: file name contained an unexpected NUL byte |
    | /x/..       |             | ERR: Read-only file system (os error 69)        |
    | /x/..       | .           | ERR: Read-only file system (os error 69)        |
    | /x/..       | ..          | ERR: Read-only file system (os error 69)        |
    | /x/..       | /           | ERR: Read-only file system (os error 69)        |
    | /x/..       | /bin        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /boot       | ERR: Read-only file system (os error 69)        |
    | /x/..       | /dev        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /etc        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /x/..       | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /x/..       | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /x/..       | /home       | ERR: Read-only file system (os error 69)        |
    | /x/..       | /lib        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /lib64      | ERR: Read-only file system (os error 69)        |
    | /x/..       | /opt        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /proc       | ERR: Read-only file system (os error 69)        |
    | /x/..       | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /x/..       | /root       | ERR: Read-only file system (os error 69)        |
    | /x/..       | /run        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /sbin       | ERR: Read-only file system (os error 69)        |
    | /x/..       | /srv        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /sys        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /tmp        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /usr        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /var        | ERR: Read-only file system (os error 69)        |
    | /x/..       | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | /x/..       | ERR: Read-only file system (os error 69)        |
    +-------------+-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_metadata() {
    let udf = udf("metadata").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | OK: got data                                    |
    | .           | OK: got data                                    |
    | ..          | OK: got data                                    |
    | /           | OK: got data                                    |
    | /bin        | ERR: No such file or directory (os error 44)    |
    | /boot       | ERR: No such file or directory (os error 44)    |
    | /dev        | ERR: No such file or directory (os error 44)    |
    | /etc        | ERR: No such file or directory (os error 44)    |
    | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /home       | ERR: No such file or directory (os error 44)    |
    | /lib        | ERR: No such file or directory (os error 44)    |
    | /lib64      | ERR: No such file or directory (os error 44)    |
    | /opt        | ERR: No such file or directory (os error 44)    |
    | /proc       | ERR: No such file or directory (os error 44)    |
    | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /root       | ERR: No such file or directory (os error 44)    |
    | /run        | ERR: No such file or directory (os error 44)    |
    | /sbin       | ERR: No such file or directory (os error 44)    |
    | /srv        | ERR: No such file or directory (os error 44)    |
    | /sys        | ERR: No such file or directory (os error 44)    |
    | /tmp        | ERR: No such file or directory (os error 44)    |
    | /usr        | ERR: No such file or directory (os error 44)    |
    | /var        | ERR: No such file or directory (os error 44)    |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | ERR: No such file or directory (os error 44)    |
    +-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_open_append() {
    let udf = udf("open_append").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | OK: opened                                      |
    | .           | OK: opened                                      |
    | ..          | OK: opened                                      |
    | /           | OK: opened                                      |
    | /bin        | ERR: No such file or directory (os error 44)    |
    | /boot       | ERR: No such file or directory (os error 44)    |
    | /dev        | ERR: No such file or directory (os error 44)    |
    | /etc        | ERR: No such file or directory (os error 44)    |
    | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /home       | ERR: No such file or directory (os error 44)    |
    | /lib        | ERR: No such file or directory (os error 44)    |
    | /lib64      | ERR: No such file or directory (os error 44)    |
    | /opt        | ERR: No such file or directory (os error 44)    |
    | /proc       | ERR: No such file or directory (os error 44)    |
    | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /root       | ERR: No such file or directory (os error 44)    |
    | /run        | ERR: No such file or directory (os error 44)    |
    | /sbin       | ERR: No such file or directory (os error 44)    |
    | /srv        | ERR: No such file or directory (os error 44)    |
    | /sys        | ERR: No such file or directory (os error 44)    |
    | /tmp        | ERR: No such file or directory (os error 44)    |
    | /usr        | ERR: No such file or directory (os error 44)    |
    | /var        | ERR: No such file or directory (os error 44)    |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | ERR: No such file or directory (os error 44)    |
    +-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_open_create() {
    let udf = udf("open_create").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | OK: opened                                      |
    | .           | OK: opened                                      |
    | ..          | OK: opened                                      |
    | /           | OK: opened                                      |
    | /bin        | ERR: No such file or directory (os error 44)    |
    | /boot       | ERR: No such file or directory (os error 44)    |
    | /dev        | ERR: No such file or directory (os error 44)    |
    | /etc        | ERR: No such file or directory (os error 44)    |
    | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /home       | ERR: No such file or directory (os error 44)    |
    | /lib        | ERR: No such file or directory (os error 44)    |
    | /lib64      | ERR: No such file or directory (os error 44)    |
    | /opt        | ERR: No such file or directory (os error 44)    |
    | /proc       | ERR: No such file or directory (os error 44)    |
    | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /root       | ERR: No such file or directory (os error 44)    |
    | /run        | ERR: No such file or directory (os error 44)    |
    | /sbin       | ERR: No such file or directory (os error 44)    |
    | /srv        | ERR: No such file or directory (os error 44)    |
    | /sys        | ERR: No such file or directory (os error 44)    |
    | /tmp        | ERR: No such file or directory (os error 44)    |
    | /usr        | ERR: No such file or directory (os error 44)    |
    | /var        | ERR: No such file or directory (os error 44)    |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | ERR: No such file or directory (os error 44)    |
    +-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_open_create_new() {
    let udf = udf("open_create_new").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | ERR: File exists (os error 20)                  |
    | .           | ERR: File exists (os error 20)                  |
    | ..          | ERR: File exists (os error 20)                  |
    | /           | ERR: File exists (os error 20)                  |
    | /bin        | OK: opened                                      |
    | /boot       | OK: opened                                      |
    | /dev        | OK: opened                                      |
    | /etc        | OK: opened                                      |
    | /etc/group  | ERR: Not a directory (os error 54)              |
    | /etc/passwd | ERR: Not a directory (os error 54)              |
    | /etc/shadow | ERR: Not a directory (os error 54)              |
    | /home       | OK: opened                                      |
    | /lib        | OK: opened                                      |
    | /lib64      | OK: opened                                      |
    | /opt        | OK: opened                                      |
    | /proc       | OK: opened                                      |
    | /proc/self  | ERR: Not a directory (os error 54)              |
    | /root       | OK: opened                                      |
    | /run        | OK: opened                                      |
    | /sbin       | OK: opened                                      |
    | /srv        | OK: opened                                      |
    | /sys        | OK: opened                                      |
    | /tmp        | OK: opened                                      |
    | /usr        | OK: opened                                      |
    | /var        | OK: opened                                      |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | ERR: Invalid argument (os error 28)             |
    +-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_open_read() {
    let udf = udf("open_read").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | OK: opened                                      |
    | .           | OK: opened                                      |
    | ..          | OK: opened                                      |
    | /           | OK: opened                                      |
    | /bin        | ERR: No such file or directory (os error 44)    |
    | /boot       | ERR: No such file or directory (os error 44)    |
    | /dev        | ERR: No such file or directory (os error 44)    |
    | /etc        | ERR: No such file or directory (os error 44)    |
    | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /home       | ERR: No such file or directory (os error 44)    |
    | /lib        | ERR: No such file or directory (os error 44)    |
    | /lib64      | ERR: No such file or directory (os error 44)    |
    | /opt        | ERR: No such file or directory (os error 44)    |
    | /proc       | ERR: No such file or directory (os error 44)    |
    | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /root       | ERR: No such file or directory (os error 44)    |
    | /run        | ERR: No such file or directory (os error 44)    |
    | /sbin       | ERR: No such file or directory (os error 44)    |
    | /srv        | ERR: No such file or directory (os error 44)    |
    | /sys        | ERR: No such file or directory (os error 44)    |
    | /tmp        | ERR: No such file or directory (os error 44)    |
    | /usr        | ERR: No such file or directory (os error 44)    |
    | /var        | ERR: No such file or directory (os error 44)    |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | ERR: No such file or directory (os error 44)    |
    +-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_open_truncate() {
    let udf = udf("open_truncate").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | ERR: Is a directory (os error 31)               |
    | .           | ERR: Is a directory (os error 31)               |
    | ..          | ERR: Is a directory (os error 31)               |
    | /           | ERR: Is a directory (os error 31)               |
    | /bin        | ERR: No such file or directory (os error 44)    |
    | /boot       | ERR: No such file or directory (os error 44)    |
    | /dev        | ERR: No such file or directory (os error 44)    |
    | /etc        | ERR: No such file or directory (os error 44)    |
    | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /home       | ERR: No such file or directory (os error 44)    |
    | /lib        | ERR: No such file or directory (os error 44)    |
    | /lib64      | ERR: No such file or directory (os error 44)    |
    | /opt        | ERR: No such file or directory (os error 44)    |
    | /proc       | ERR: No such file or directory (os error 44)    |
    | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /root       | ERR: No such file or directory (os error 44)    |
    | /run        | ERR: No such file or directory (os error 44)    |
    | /sbin       | ERR: No such file or directory (os error 44)    |
    | /srv        | ERR: No such file or directory (os error 44)    |
    | /sys        | ERR: No such file or directory (os error 44)    |
    | /tmp        | ERR: No such file or directory (os error 44)    |
    | /usr        | ERR: No such file or directory (os error 44)    |
    | /var        | ERR: No such file or directory (os error 44)    |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | ERR: No such file or directory (os error 44)    |
    +-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_open_write() {
    let udf = udf("open_write").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | OK: opened                                      |
    | .           | OK: opened                                      |
    | ..          | OK: opened                                      |
    | /           | OK: opened                                      |
    | /bin        | ERR: No such file or directory (os error 44)    |
    | /boot       | ERR: No such file or directory (os error 44)    |
    | /dev        | ERR: No such file or directory (os error 44)    |
    | /etc        | ERR: No such file or directory (os error 44)    |
    | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /home       | ERR: No such file or directory (os error 44)    |
    | /lib        | ERR: No such file or directory (os error 44)    |
    | /lib64      | ERR: No such file or directory (os error 44)    |
    | /opt        | ERR: No such file or directory (os error 44)    |
    | /proc       | ERR: No such file or directory (os error 44)    |
    | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /root       | ERR: No such file or directory (os error 44)    |
    | /run        | ERR: No such file or directory (os error 44)    |
    | /sbin       | ERR: No such file or directory (os error 44)    |
    | /srv        | ERR: No such file or directory (os error 44)    |
    | /sys        | ERR: No such file or directory (os error 44)    |
    | /tmp        | ERR: No such file or directory (os error 44)    |
    | /usr        | ERR: No such file or directory (os error 44)    |
    | /var        | ERR: No such file or directory (os error 44)    |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | ERR: No such file or directory (os error 44)    |
    +-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_read_dir() {
    let udf = udf("read_dir").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | OK: <EMPTY>                                     |
    | .           | OK: <EMPTY>                                     |
    | ..          | OK: <EMPTY>                                     |
    | /           | OK: <EMPTY>                                     |
    | /bin        | ERR: No such file or directory (os error 44)    |
    | /boot       | ERR: No such file or directory (os error 44)    |
    | /dev        | ERR: No such file or directory (os error 44)    |
    | /etc        | ERR: No such file or directory (os error 44)    |
    | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /home       | ERR: No such file or directory (os error 44)    |
    | /lib        | ERR: No such file or directory (os error 44)    |
    | /lib64      | ERR: No such file or directory (os error 44)    |
    | /opt        | ERR: No such file or directory (os error 44)    |
    | /proc       | ERR: No such file or directory (os error 44)    |
    | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /root       | ERR: No such file or directory (os error 44)    |
    | /run        | ERR: No such file or directory (os error 44)    |
    | /sbin       | ERR: No such file or directory (os error 44)    |
    | /srv        | ERR: No such file or directory (os error 44)    |
    | /sys        | ERR: No such file or directory (os error 44)    |
    | /tmp        | ERR: No such file or directory (os error 44)    |
    | /usr        | ERR: No such file or directory (os error 44)    |
    | /var        | ERR: No such file or directory (os error 44)    |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | ERR: No such file or directory (os error 44)    |
    +-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_read_link() {
    let udf = udf("read_link").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | ERR: Not supported (os error 58)                |
    | .           | ERR: Not supported (os error 58)                |
    | ..          | ERR: Not supported (os error 58)                |
    | /           | ERR: Not supported (os error 58)                |
    | /bin        | ERR: No such file or directory (os error 44)    |
    | /boot       | ERR: No such file or directory (os error 44)    |
    | /dev        | ERR: No such file or directory (os error 44)    |
    | /etc        | ERR: No such file or directory (os error 44)    |
    | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /home       | ERR: No such file or directory (os error 44)    |
    | /lib        | ERR: No such file or directory (os error 44)    |
    | /lib64      | ERR: No such file or directory (os error 44)    |
    | /opt        | ERR: No such file or directory (os error 44)    |
    | /proc       | ERR: No such file or directory (os error 44)    |
    | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /root       | ERR: No such file or directory (os error 44)    |
    | /run        | ERR: No such file or directory (os error 44)    |
    | /sbin       | ERR: No such file or directory (os error 44)    |
    | /srv        | ERR: No such file or directory (os error 44)    |
    | /sys        | ERR: No such file or directory (os error 44)    |
    | /tmp        | ERR: No such file or directory (os error 44)    |
    | /usr        | ERR: No such file or directory (os error 44)    |
    | /var        | ERR: No such file or directory (os error 44)    |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | ERR: No such file or directory (os error 44)    |
    +-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_remove_dir() {
    let udf = udf("remove_dir").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | ERR: Read-only file system (os error 69)        |
    | .           | ERR: Read-only file system (os error 69)        |
    | ..          | ERR: Read-only file system (os error 69)        |
    | /           | ERR: Read-only file system (os error 69)        |
    | /bin        | ERR: Read-only file system (os error 69)        |
    | /boot       | ERR: Read-only file system (os error 69)        |
    | /dev        | ERR: Read-only file system (os error 69)        |
    | /etc        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /home       | ERR: Read-only file system (os error 69)        |
    | /lib        | ERR: Read-only file system (os error 69)        |
    | /lib64      | ERR: Read-only file system (os error 69)        |
    | /opt        | ERR: Read-only file system (os error 69)        |
    | /proc       | ERR: Read-only file system (os error 69)        |
    | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /root       | ERR: Read-only file system (os error 69)        |
    | /run        | ERR: Read-only file system (os error 69)        |
    | /sbin       | ERR: Read-only file system (os error 69)        |
    | /srv        | ERR: Read-only file system (os error 69)        |
    | /sys        | ERR: Read-only file system (os error 69)        |
    | /tmp        | ERR: Read-only file system (os error 69)        |
    | /usr        | ERR: Read-only file system (os error 69)        |
    | /var        | ERR: Read-only file system (os error 69)        |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | ERR: Read-only file system (os error 69)        |
    +-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_remove_file() {
    let udf = udf("remove_file").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | ERR: Read-only file system (os error 69)        |
    | .           | ERR: Read-only file system (os error 69)        |
    | ..          | ERR: Read-only file system (os error 69)        |
    | /           | ERR: Read-only file system (os error 69)        |
    | /bin        | ERR: Read-only file system (os error 69)        |
    | /boot       | ERR: Read-only file system (os error 69)        |
    | /dev        | ERR: Read-only file system (os error 69)        |
    | /etc        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /home       | ERR: Read-only file system (os error 69)        |
    | /lib        | ERR: Read-only file system (os error 69)        |
    | /lib64      | ERR: Read-only file system (os error 69)        |
    | /opt        | ERR: Read-only file system (os error 69)        |
    | /proc       | ERR: Read-only file system (os error 69)        |
    | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /root       | ERR: Read-only file system (os error 69)        |
    | /run        | ERR: Read-only file system (os error 69)        |
    | /sbin       | ERR: Read-only file system (os error 69)        |
    | /srv        | ERR: Read-only file system (os error 69)        |
    | /sys        | ERR: Read-only file system (os error 69)        |
    | /tmp        | ERR: Read-only file system (os error 69)        |
    | /usr        | ERR: Read-only file system (os error 69)        |
    | /var        | ERR: Read-only file system (os error 69)        |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | ERR: Read-only file system (os error 69)        |
    +-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_rename() {
    let udf = udf("rename").await;

    insta::assert_snapshot!(
        run_2(&udf).await,
        @r"
    +-------------+-------------+-------------------------------------------------+
    | from        | to          | output                                          |
    +-------------+-------------+-------------------------------------------------+
    |             |             | ERR: Read-only file system (os error 69)        |
    |             | .           | ERR: Read-only file system (os error 69)        |
    |             | ..          | ERR: Read-only file system (os error 69)        |
    |             | /           | ERR: Read-only file system (os error 69)        |
    |             | /bin        | ERR: Read-only file system (os error 69)        |
    |             | /boot       | ERR: Read-only file system (os error 69)        |
    |             | /dev        | ERR: Read-only file system (os error 69)        |
    |             | /etc        | ERR: Read-only file system (os error 69)        |
    |             | /etc/group  | ERR: Read-only file system (os error 69)        |
    |             | /etc/passwd | ERR: Read-only file system (os error 69)        |
    |             | /etc/shadow | ERR: Read-only file system (os error 69)        |
    |             | /home       | ERR: Read-only file system (os error 69)        |
    |             | /lib        | ERR: Read-only file system (os error 69)        |
    |             | /lib64      | ERR: Read-only file system (os error 69)        |
    |             | /opt        | ERR: Read-only file system (os error 69)        |
    |             | /proc       | ERR: Read-only file system (os error 69)        |
    |             | /proc/self  | ERR: Read-only file system (os error 69)        |
    |             | /root       | ERR: Read-only file system (os error 69)        |
    |             | /run        | ERR: Read-only file system (os error 69)        |
    |             | /sbin       | ERR: Read-only file system (os error 69)        |
    |             | /srv        | ERR: Read-only file system (os error 69)        |
    |             | /sys        | ERR: Read-only file system (os error 69)        |
    |             | /tmp        | ERR: Read-only file system (os error 69)        |
    |             | /usr        | ERR: Read-only file system (os error 69)        |
    |             | /var        | ERR: Read-only file system (os error 69)        |
    |             | \0          | ERR: file name contained an unexpected NUL byte |
    |             | /x/..       | ERR: Read-only file system (os error 69)        |
    | .           |             | ERR: Read-only file system (os error 69)        |
    | .           | .           | ERR: Read-only file system (os error 69)        |
    | .           | ..          | ERR: Read-only file system (os error 69)        |
    | .           | /           | ERR: Read-only file system (os error 69)        |
    | .           | /bin        | ERR: Read-only file system (os error 69)        |
    | .           | /boot       | ERR: Read-only file system (os error 69)        |
    | .           | /dev        | ERR: Read-only file system (os error 69)        |
    | .           | /etc        | ERR: Read-only file system (os error 69)        |
    | .           | /etc/group  | ERR: Read-only file system (os error 69)        |
    | .           | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | .           | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | .           | /home       | ERR: Read-only file system (os error 69)        |
    | .           | /lib        | ERR: Read-only file system (os error 69)        |
    | .           | /lib64      | ERR: Read-only file system (os error 69)        |
    | .           | /opt        | ERR: Read-only file system (os error 69)        |
    | .           | /proc       | ERR: Read-only file system (os error 69)        |
    | .           | /proc/self  | ERR: Read-only file system (os error 69)        |
    | .           | /root       | ERR: Read-only file system (os error 69)        |
    | .           | /run        | ERR: Read-only file system (os error 69)        |
    | .           | /sbin       | ERR: Read-only file system (os error 69)        |
    | .           | /srv        | ERR: Read-only file system (os error 69)        |
    | .           | /sys        | ERR: Read-only file system (os error 69)        |
    | .           | /tmp        | ERR: Read-only file system (os error 69)        |
    | .           | /usr        | ERR: Read-only file system (os error 69)        |
    | .           | /var        | ERR: Read-only file system (os error 69)        |
    | .           | \0          | ERR: file name contained an unexpected NUL byte |
    | .           | /x/..       | ERR: Read-only file system (os error 69)        |
    | ..          |             | ERR: Read-only file system (os error 69)        |
    | ..          | .           | ERR: Read-only file system (os error 69)        |
    | ..          | ..          | ERR: Read-only file system (os error 69)        |
    | ..          | /           | ERR: Read-only file system (os error 69)        |
    | ..          | /bin        | ERR: Read-only file system (os error 69)        |
    | ..          | /boot       | ERR: Read-only file system (os error 69)        |
    | ..          | /dev        | ERR: Read-only file system (os error 69)        |
    | ..          | /etc        | ERR: Read-only file system (os error 69)        |
    | ..          | /etc/group  | ERR: Read-only file system (os error 69)        |
    | ..          | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | ..          | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | ..          | /home       | ERR: Read-only file system (os error 69)        |
    | ..          | /lib        | ERR: Read-only file system (os error 69)        |
    | ..          | /lib64      | ERR: Read-only file system (os error 69)        |
    | ..          | /opt        | ERR: Read-only file system (os error 69)        |
    | ..          | /proc       | ERR: Read-only file system (os error 69)        |
    | ..          | /proc/self  | ERR: Read-only file system (os error 69)        |
    | ..          | /root       | ERR: Read-only file system (os error 69)        |
    | ..          | /run        | ERR: Read-only file system (os error 69)        |
    | ..          | /sbin       | ERR: Read-only file system (os error 69)        |
    | ..          | /srv        | ERR: Read-only file system (os error 69)        |
    | ..          | /sys        | ERR: Read-only file system (os error 69)        |
    | ..          | /tmp        | ERR: Read-only file system (os error 69)        |
    | ..          | /usr        | ERR: Read-only file system (os error 69)        |
    | ..          | /var        | ERR: Read-only file system (os error 69)        |
    | ..          | \0          | ERR: file name contained an unexpected NUL byte |
    | ..          | /x/..       | ERR: Read-only file system (os error 69)        |
    | /           |             | ERR: Read-only file system (os error 69)        |
    | /           | .           | ERR: Read-only file system (os error 69)        |
    | /           | ..          | ERR: Read-only file system (os error 69)        |
    | /           | /           | ERR: Read-only file system (os error 69)        |
    | /           | /bin        | ERR: Read-only file system (os error 69)        |
    | /           | /boot       | ERR: Read-only file system (os error 69)        |
    | /           | /dev        | ERR: Read-only file system (os error 69)        |
    | /           | /etc        | ERR: Read-only file system (os error 69)        |
    | /           | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /           | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /           | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /           | /home       | ERR: Read-only file system (os error 69)        |
    | /           | /lib        | ERR: Read-only file system (os error 69)        |
    | /           | /lib64      | ERR: Read-only file system (os error 69)        |
    | /           | /opt        | ERR: Read-only file system (os error 69)        |
    | /           | /proc       | ERR: Read-only file system (os error 69)        |
    | /           | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /           | /root       | ERR: Read-only file system (os error 69)        |
    | /           | /run        | ERR: Read-only file system (os error 69)        |
    | /           | /sbin       | ERR: Read-only file system (os error 69)        |
    | /           | /srv        | ERR: Read-only file system (os error 69)        |
    | /           | /sys        | ERR: Read-only file system (os error 69)        |
    | /           | /tmp        | ERR: Read-only file system (os error 69)        |
    | /           | /usr        | ERR: Read-only file system (os error 69)        |
    | /           | /var        | ERR: Read-only file system (os error 69)        |
    | /           | \0          | ERR: file name contained an unexpected NUL byte |
    | /           | /x/..       | ERR: Read-only file system (os error 69)        |
    | /bin        |             | ERR: Read-only file system (os error 69)        |
    | /bin        | .           | ERR: Read-only file system (os error 69)        |
    | /bin        | ..          | ERR: Read-only file system (os error 69)        |
    | /bin        | /           | ERR: Read-only file system (os error 69)        |
    | /bin        | /bin        | ERR: Read-only file system (os error 69)        |
    | /bin        | /boot       | ERR: Read-only file system (os error 69)        |
    | /bin        | /dev        | ERR: Read-only file system (os error 69)        |
    | /bin        | /etc        | ERR: Read-only file system (os error 69)        |
    | /bin        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /bin        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /bin        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /bin        | /home       | ERR: Read-only file system (os error 69)        |
    | /bin        | /lib        | ERR: Read-only file system (os error 69)        |
    | /bin        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /bin        | /opt        | ERR: Read-only file system (os error 69)        |
    | /bin        | /proc       | ERR: Read-only file system (os error 69)        |
    | /bin        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /bin        | /root       | ERR: Read-only file system (os error 69)        |
    | /bin        | /run        | ERR: Read-only file system (os error 69)        |
    | /bin        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /bin        | /srv        | ERR: Read-only file system (os error 69)        |
    | /bin        | /sys        | ERR: Read-only file system (os error 69)        |
    | /bin        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /bin        | /usr        | ERR: Read-only file system (os error 69)        |
    | /bin        | /var        | ERR: Read-only file system (os error 69)        |
    | /bin        | \0          | ERR: file name contained an unexpected NUL byte |
    | /bin        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /boot       |             | ERR: Read-only file system (os error 69)        |
    | /boot       | .           | ERR: Read-only file system (os error 69)        |
    | /boot       | ..          | ERR: Read-only file system (os error 69)        |
    | /boot       | /           | ERR: Read-only file system (os error 69)        |
    | /boot       | /bin        | ERR: Read-only file system (os error 69)        |
    | /boot       | /boot       | ERR: Read-only file system (os error 69)        |
    | /boot       | /dev        | ERR: Read-only file system (os error 69)        |
    | /boot       | /etc        | ERR: Read-only file system (os error 69)        |
    | /boot       | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /boot       | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /boot       | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /boot       | /home       | ERR: Read-only file system (os error 69)        |
    | /boot       | /lib        | ERR: Read-only file system (os error 69)        |
    | /boot       | /lib64      | ERR: Read-only file system (os error 69)        |
    | /boot       | /opt        | ERR: Read-only file system (os error 69)        |
    | /boot       | /proc       | ERR: Read-only file system (os error 69)        |
    | /boot       | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /boot       | /root       | ERR: Read-only file system (os error 69)        |
    | /boot       | /run        | ERR: Read-only file system (os error 69)        |
    | /boot       | /sbin       | ERR: Read-only file system (os error 69)        |
    | /boot       | /srv        | ERR: Read-only file system (os error 69)        |
    | /boot       | /sys        | ERR: Read-only file system (os error 69)        |
    | /boot       | /tmp        | ERR: Read-only file system (os error 69)        |
    | /boot       | /usr        | ERR: Read-only file system (os error 69)        |
    | /boot       | /var        | ERR: Read-only file system (os error 69)        |
    | /boot       | \0          | ERR: file name contained an unexpected NUL byte |
    | /boot       | /x/..       | ERR: Read-only file system (os error 69)        |
    | /dev        |             | ERR: Read-only file system (os error 69)        |
    | /dev        | .           | ERR: Read-only file system (os error 69)        |
    | /dev        | ..          | ERR: Read-only file system (os error 69)        |
    | /dev        | /           | ERR: Read-only file system (os error 69)        |
    | /dev        | /bin        | ERR: Read-only file system (os error 69)        |
    | /dev        | /boot       | ERR: Read-only file system (os error 69)        |
    | /dev        | /dev        | ERR: Read-only file system (os error 69)        |
    | /dev        | /etc        | ERR: Read-only file system (os error 69)        |
    | /dev        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /dev        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /dev        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /dev        | /home       | ERR: Read-only file system (os error 69)        |
    | /dev        | /lib        | ERR: Read-only file system (os error 69)        |
    | /dev        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /dev        | /opt        | ERR: Read-only file system (os error 69)        |
    | /dev        | /proc       | ERR: Read-only file system (os error 69)        |
    | /dev        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /dev        | /root       | ERR: Read-only file system (os error 69)        |
    | /dev        | /run        | ERR: Read-only file system (os error 69)        |
    | /dev        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /dev        | /srv        | ERR: Read-only file system (os error 69)        |
    | /dev        | /sys        | ERR: Read-only file system (os error 69)        |
    | /dev        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /dev        | /usr        | ERR: Read-only file system (os error 69)        |
    | /dev        | /var        | ERR: Read-only file system (os error 69)        |
    | /dev        | \0          | ERR: file name contained an unexpected NUL byte |
    | /dev        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /etc        |             | ERR: Read-only file system (os error 69)        |
    | /etc        | .           | ERR: Read-only file system (os error 69)        |
    | /etc        | ..          | ERR: Read-only file system (os error 69)        |
    | /etc        | /           | ERR: Read-only file system (os error 69)        |
    | /etc        | /bin        | ERR: Read-only file system (os error 69)        |
    | /etc        | /boot       | ERR: Read-only file system (os error 69)        |
    | /etc        | /dev        | ERR: Read-only file system (os error 69)        |
    | /etc        | /etc        | ERR: Read-only file system (os error 69)        |
    | /etc        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /etc        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /etc        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /etc        | /home       | ERR: Read-only file system (os error 69)        |
    | /etc        | /lib        | ERR: Read-only file system (os error 69)        |
    | /etc        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /etc        | /opt        | ERR: Read-only file system (os error 69)        |
    | /etc        | /proc       | ERR: Read-only file system (os error 69)        |
    | /etc        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /etc        | /root       | ERR: Read-only file system (os error 69)        |
    | /etc        | /run        | ERR: Read-only file system (os error 69)        |
    | /etc        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /etc        | /srv        | ERR: Read-only file system (os error 69)        |
    | /etc        | /sys        | ERR: Read-only file system (os error 69)        |
    | /etc        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /etc        | /usr        | ERR: Read-only file system (os error 69)        |
    | /etc        | /var        | ERR: Read-only file system (os error 69)        |
    | /etc        | \0          | ERR: file name contained an unexpected NUL byte |
    | /etc        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /etc/group  |             | ERR: Read-only file system (os error 69)        |
    | /etc/group  | .           | ERR: Read-only file system (os error 69)        |
    | /etc/group  | ..          | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /           | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /bin        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /boot       | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /dev        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /etc        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /home       | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /lib        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /lib64      | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /opt        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /proc       | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /root       | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /run        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /sbin       | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /srv        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /sys        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /tmp        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /usr        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | /var        | ERR: Read-only file system (os error 69)        |
    | /etc/group  | \0          | ERR: file name contained an unexpected NUL byte |
    | /etc/group  | /x/..       | ERR: Read-only file system (os error 69)        |
    | /etc/passwd |             | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | .           | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | ..          | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /           | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /bin        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /boot       | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /dev        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /etc        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /home       | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /lib        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /lib64      | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /opt        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /proc       | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /root       | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /run        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /sbin       | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /srv        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /sys        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /tmp        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /usr        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | /var        | ERR: Read-only file system (os error 69)        |
    | /etc/passwd | \0          | ERR: file name contained an unexpected NUL byte |
    | /etc/passwd | /x/..       | ERR: Read-only file system (os error 69)        |
    | /etc/shadow |             | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | .           | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | ..          | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /           | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /bin        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /boot       | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /dev        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /etc        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /home       | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /lib        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /lib64      | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /opt        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /proc       | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /root       | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /run        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /sbin       | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /srv        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /sys        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /tmp        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /usr        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | /var        | ERR: Read-only file system (os error 69)        |
    | /etc/shadow | \0          | ERR: file name contained an unexpected NUL byte |
    | /etc/shadow | /x/..       | ERR: Read-only file system (os error 69)        |
    | /home       |             | ERR: Read-only file system (os error 69)        |
    | /home       | .           | ERR: Read-only file system (os error 69)        |
    | /home       | ..          | ERR: Read-only file system (os error 69)        |
    | /home       | /           | ERR: Read-only file system (os error 69)        |
    | /home       | /bin        | ERR: Read-only file system (os error 69)        |
    | /home       | /boot       | ERR: Read-only file system (os error 69)        |
    | /home       | /dev        | ERR: Read-only file system (os error 69)        |
    | /home       | /etc        | ERR: Read-only file system (os error 69)        |
    | /home       | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /home       | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /home       | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /home       | /home       | ERR: Read-only file system (os error 69)        |
    | /home       | /lib        | ERR: Read-only file system (os error 69)        |
    | /home       | /lib64      | ERR: Read-only file system (os error 69)        |
    | /home       | /opt        | ERR: Read-only file system (os error 69)        |
    | /home       | /proc       | ERR: Read-only file system (os error 69)        |
    | /home       | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /home       | /root       | ERR: Read-only file system (os error 69)        |
    | /home       | /run        | ERR: Read-only file system (os error 69)        |
    | /home       | /sbin       | ERR: Read-only file system (os error 69)        |
    | /home       | /srv        | ERR: Read-only file system (os error 69)        |
    | /home       | /sys        | ERR: Read-only file system (os error 69)        |
    | /home       | /tmp        | ERR: Read-only file system (os error 69)        |
    | /home       | /usr        | ERR: Read-only file system (os error 69)        |
    | /home       | /var        | ERR: Read-only file system (os error 69)        |
    | /home       | \0          | ERR: file name contained an unexpected NUL byte |
    | /home       | /x/..       | ERR: Read-only file system (os error 69)        |
    | /lib        |             | ERR: Read-only file system (os error 69)        |
    | /lib        | .           | ERR: Read-only file system (os error 69)        |
    | /lib        | ..          | ERR: Read-only file system (os error 69)        |
    | /lib        | /           | ERR: Read-only file system (os error 69)        |
    | /lib        | /bin        | ERR: Read-only file system (os error 69)        |
    | /lib        | /boot       | ERR: Read-only file system (os error 69)        |
    | /lib        | /dev        | ERR: Read-only file system (os error 69)        |
    | /lib        | /etc        | ERR: Read-only file system (os error 69)        |
    | /lib        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /lib        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /lib        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /lib        | /home       | ERR: Read-only file system (os error 69)        |
    | /lib        | /lib        | ERR: Read-only file system (os error 69)        |
    | /lib        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /lib        | /opt        | ERR: Read-only file system (os error 69)        |
    | /lib        | /proc       | ERR: Read-only file system (os error 69)        |
    | /lib        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /lib        | /root       | ERR: Read-only file system (os error 69)        |
    | /lib        | /run        | ERR: Read-only file system (os error 69)        |
    | /lib        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /lib        | /srv        | ERR: Read-only file system (os error 69)        |
    | /lib        | /sys        | ERR: Read-only file system (os error 69)        |
    | /lib        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /lib        | /usr        | ERR: Read-only file system (os error 69)        |
    | /lib        | /var        | ERR: Read-only file system (os error 69)        |
    | /lib        | \0          | ERR: file name contained an unexpected NUL byte |
    | /lib        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /lib64      |             | ERR: Read-only file system (os error 69)        |
    | /lib64      | .           | ERR: Read-only file system (os error 69)        |
    | /lib64      | ..          | ERR: Read-only file system (os error 69)        |
    | /lib64      | /           | ERR: Read-only file system (os error 69)        |
    | /lib64      | /bin        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /boot       | ERR: Read-only file system (os error 69)        |
    | /lib64      | /dev        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /etc        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /lib64      | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /lib64      | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /lib64      | /home       | ERR: Read-only file system (os error 69)        |
    | /lib64      | /lib        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /lib64      | ERR: Read-only file system (os error 69)        |
    | /lib64      | /opt        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /proc       | ERR: Read-only file system (os error 69)        |
    | /lib64      | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /lib64      | /root       | ERR: Read-only file system (os error 69)        |
    | /lib64      | /run        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /sbin       | ERR: Read-only file system (os error 69)        |
    | /lib64      | /srv        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /sys        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /tmp        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /usr        | ERR: Read-only file system (os error 69)        |
    | /lib64      | /var        | ERR: Read-only file system (os error 69)        |
    | /lib64      | \0          | ERR: file name contained an unexpected NUL byte |
    | /lib64      | /x/..       | ERR: Read-only file system (os error 69)        |
    | /opt        |             | ERR: Read-only file system (os error 69)        |
    | /opt        | .           | ERR: Read-only file system (os error 69)        |
    | /opt        | ..          | ERR: Read-only file system (os error 69)        |
    | /opt        | /           | ERR: Read-only file system (os error 69)        |
    | /opt        | /bin        | ERR: Read-only file system (os error 69)        |
    | /opt        | /boot       | ERR: Read-only file system (os error 69)        |
    | /opt        | /dev        | ERR: Read-only file system (os error 69)        |
    | /opt        | /etc        | ERR: Read-only file system (os error 69)        |
    | /opt        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /opt        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /opt        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /opt        | /home       | ERR: Read-only file system (os error 69)        |
    | /opt        | /lib        | ERR: Read-only file system (os error 69)        |
    | /opt        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /opt        | /opt        | ERR: Read-only file system (os error 69)        |
    | /opt        | /proc       | ERR: Read-only file system (os error 69)        |
    | /opt        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /opt        | /root       | ERR: Read-only file system (os error 69)        |
    | /opt        | /run        | ERR: Read-only file system (os error 69)        |
    | /opt        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /opt        | /srv        | ERR: Read-only file system (os error 69)        |
    | /opt        | /sys        | ERR: Read-only file system (os error 69)        |
    | /opt        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /opt        | /usr        | ERR: Read-only file system (os error 69)        |
    | /opt        | /var        | ERR: Read-only file system (os error 69)        |
    | /opt        | \0          | ERR: file name contained an unexpected NUL byte |
    | /opt        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /proc       |             | ERR: Read-only file system (os error 69)        |
    | /proc       | .           | ERR: Read-only file system (os error 69)        |
    | /proc       | ..          | ERR: Read-only file system (os error 69)        |
    | /proc       | /           | ERR: Read-only file system (os error 69)        |
    | /proc       | /bin        | ERR: Read-only file system (os error 69)        |
    | /proc       | /boot       | ERR: Read-only file system (os error 69)        |
    | /proc       | /dev        | ERR: Read-only file system (os error 69)        |
    | /proc       | /etc        | ERR: Read-only file system (os error 69)        |
    | /proc       | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /proc       | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /proc       | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /proc       | /home       | ERR: Read-only file system (os error 69)        |
    | /proc       | /lib        | ERR: Read-only file system (os error 69)        |
    | /proc       | /lib64      | ERR: Read-only file system (os error 69)        |
    | /proc       | /opt        | ERR: Read-only file system (os error 69)        |
    | /proc       | /proc       | ERR: Read-only file system (os error 69)        |
    | /proc       | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /proc       | /root       | ERR: Read-only file system (os error 69)        |
    | /proc       | /run        | ERR: Read-only file system (os error 69)        |
    | /proc       | /sbin       | ERR: Read-only file system (os error 69)        |
    | /proc       | /srv        | ERR: Read-only file system (os error 69)        |
    | /proc       | /sys        | ERR: Read-only file system (os error 69)        |
    | /proc       | /tmp        | ERR: Read-only file system (os error 69)        |
    | /proc       | /usr        | ERR: Read-only file system (os error 69)        |
    | /proc       | /var        | ERR: Read-only file system (os error 69)        |
    | /proc       | \0          | ERR: file name contained an unexpected NUL byte |
    | /proc       | /x/..       | ERR: Read-only file system (os error 69)        |
    | /proc/self  |             | ERR: Read-only file system (os error 69)        |
    | /proc/self  | .           | ERR: Read-only file system (os error 69)        |
    | /proc/self  | ..          | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /           | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /bin        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /boot       | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /dev        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /etc        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /home       | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /lib        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /lib64      | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /opt        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /proc       | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /root       | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /run        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /sbin       | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /srv        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /sys        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /tmp        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /usr        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | /var        | ERR: Read-only file system (os error 69)        |
    | /proc/self  | \0          | ERR: file name contained an unexpected NUL byte |
    | /proc/self  | /x/..       | ERR: Read-only file system (os error 69)        |
    | /root       |             | ERR: Read-only file system (os error 69)        |
    | /root       | .           | ERR: Read-only file system (os error 69)        |
    | /root       | ..          | ERR: Read-only file system (os error 69)        |
    | /root       | /           | ERR: Read-only file system (os error 69)        |
    | /root       | /bin        | ERR: Read-only file system (os error 69)        |
    | /root       | /boot       | ERR: Read-only file system (os error 69)        |
    | /root       | /dev        | ERR: Read-only file system (os error 69)        |
    | /root       | /etc        | ERR: Read-only file system (os error 69)        |
    | /root       | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /root       | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /root       | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /root       | /home       | ERR: Read-only file system (os error 69)        |
    | /root       | /lib        | ERR: Read-only file system (os error 69)        |
    | /root       | /lib64      | ERR: Read-only file system (os error 69)        |
    | /root       | /opt        | ERR: Read-only file system (os error 69)        |
    | /root       | /proc       | ERR: Read-only file system (os error 69)        |
    | /root       | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /root       | /root       | ERR: Read-only file system (os error 69)        |
    | /root       | /run        | ERR: Read-only file system (os error 69)        |
    | /root       | /sbin       | ERR: Read-only file system (os error 69)        |
    | /root       | /srv        | ERR: Read-only file system (os error 69)        |
    | /root       | /sys        | ERR: Read-only file system (os error 69)        |
    | /root       | /tmp        | ERR: Read-only file system (os error 69)        |
    | /root       | /usr        | ERR: Read-only file system (os error 69)        |
    | /root       | /var        | ERR: Read-only file system (os error 69)        |
    | /root       | \0          | ERR: file name contained an unexpected NUL byte |
    | /root       | /x/..       | ERR: Read-only file system (os error 69)        |
    | /run        |             | ERR: Read-only file system (os error 69)        |
    | /run        | .           | ERR: Read-only file system (os error 69)        |
    | /run        | ..          | ERR: Read-only file system (os error 69)        |
    | /run        | /           | ERR: Read-only file system (os error 69)        |
    | /run        | /bin        | ERR: Read-only file system (os error 69)        |
    | /run        | /boot       | ERR: Read-only file system (os error 69)        |
    | /run        | /dev        | ERR: Read-only file system (os error 69)        |
    | /run        | /etc        | ERR: Read-only file system (os error 69)        |
    | /run        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /run        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /run        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /run        | /home       | ERR: Read-only file system (os error 69)        |
    | /run        | /lib        | ERR: Read-only file system (os error 69)        |
    | /run        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /run        | /opt        | ERR: Read-only file system (os error 69)        |
    | /run        | /proc       | ERR: Read-only file system (os error 69)        |
    | /run        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /run        | /root       | ERR: Read-only file system (os error 69)        |
    | /run        | /run        | ERR: Read-only file system (os error 69)        |
    | /run        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /run        | /srv        | ERR: Read-only file system (os error 69)        |
    | /run        | /sys        | ERR: Read-only file system (os error 69)        |
    | /run        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /run        | /usr        | ERR: Read-only file system (os error 69)        |
    | /run        | /var        | ERR: Read-only file system (os error 69)        |
    | /run        | \0          | ERR: file name contained an unexpected NUL byte |
    | /run        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /sbin       |             | ERR: Read-only file system (os error 69)        |
    | /sbin       | .           | ERR: Read-only file system (os error 69)        |
    | /sbin       | ..          | ERR: Read-only file system (os error 69)        |
    | /sbin       | /           | ERR: Read-only file system (os error 69)        |
    | /sbin       | /bin        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /boot       | ERR: Read-only file system (os error 69)        |
    | /sbin       | /dev        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /etc        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /sbin       | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /sbin       | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /sbin       | /home       | ERR: Read-only file system (os error 69)        |
    | /sbin       | /lib        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /lib64      | ERR: Read-only file system (os error 69)        |
    | /sbin       | /opt        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /proc       | ERR: Read-only file system (os error 69)        |
    | /sbin       | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /sbin       | /root       | ERR: Read-only file system (os error 69)        |
    | /sbin       | /run        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /sbin       | ERR: Read-only file system (os error 69)        |
    | /sbin       | /srv        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /sys        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /tmp        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /usr        | ERR: Read-only file system (os error 69)        |
    | /sbin       | /var        | ERR: Read-only file system (os error 69)        |
    | /sbin       | \0          | ERR: file name contained an unexpected NUL byte |
    | /sbin       | /x/..       | ERR: Read-only file system (os error 69)        |
    | /srv        |             | ERR: Read-only file system (os error 69)        |
    | /srv        | .           | ERR: Read-only file system (os error 69)        |
    | /srv        | ..          | ERR: Read-only file system (os error 69)        |
    | /srv        | /           | ERR: Read-only file system (os error 69)        |
    | /srv        | /bin        | ERR: Read-only file system (os error 69)        |
    | /srv        | /boot       | ERR: Read-only file system (os error 69)        |
    | /srv        | /dev        | ERR: Read-only file system (os error 69)        |
    | /srv        | /etc        | ERR: Read-only file system (os error 69)        |
    | /srv        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /srv        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /srv        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /srv        | /home       | ERR: Read-only file system (os error 69)        |
    | /srv        | /lib        | ERR: Read-only file system (os error 69)        |
    | /srv        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /srv        | /opt        | ERR: Read-only file system (os error 69)        |
    | /srv        | /proc       | ERR: Read-only file system (os error 69)        |
    | /srv        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /srv        | /root       | ERR: Read-only file system (os error 69)        |
    | /srv        | /run        | ERR: Read-only file system (os error 69)        |
    | /srv        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /srv        | /srv        | ERR: Read-only file system (os error 69)        |
    | /srv        | /sys        | ERR: Read-only file system (os error 69)        |
    | /srv        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /srv        | /usr        | ERR: Read-only file system (os error 69)        |
    | /srv        | /var        | ERR: Read-only file system (os error 69)        |
    | /srv        | \0          | ERR: file name contained an unexpected NUL byte |
    | /srv        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /sys        |             | ERR: Read-only file system (os error 69)        |
    | /sys        | .           | ERR: Read-only file system (os error 69)        |
    | /sys        | ..          | ERR: Read-only file system (os error 69)        |
    | /sys        | /           | ERR: Read-only file system (os error 69)        |
    | /sys        | /bin        | ERR: Read-only file system (os error 69)        |
    | /sys        | /boot       | ERR: Read-only file system (os error 69)        |
    | /sys        | /dev        | ERR: Read-only file system (os error 69)        |
    | /sys        | /etc        | ERR: Read-only file system (os error 69)        |
    | /sys        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /sys        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /sys        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /sys        | /home       | ERR: Read-only file system (os error 69)        |
    | /sys        | /lib        | ERR: Read-only file system (os error 69)        |
    | /sys        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /sys        | /opt        | ERR: Read-only file system (os error 69)        |
    | /sys        | /proc       | ERR: Read-only file system (os error 69)        |
    | /sys        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /sys        | /root       | ERR: Read-only file system (os error 69)        |
    | /sys        | /run        | ERR: Read-only file system (os error 69)        |
    | /sys        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /sys        | /srv        | ERR: Read-only file system (os error 69)        |
    | /sys        | /sys        | ERR: Read-only file system (os error 69)        |
    | /sys        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /sys        | /usr        | ERR: Read-only file system (os error 69)        |
    | /sys        | /var        | ERR: Read-only file system (os error 69)        |
    | /sys        | \0          | ERR: file name contained an unexpected NUL byte |
    | /sys        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /tmp        |             | ERR: Read-only file system (os error 69)        |
    | /tmp        | .           | ERR: Read-only file system (os error 69)        |
    | /tmp        | ..          | ERR: Read-only file system (os error 69)        |
    | /tmp        | /           | ERR: Read-only file system (os error 69)        |
    | /tmp        | /bin        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /boot       | ERR: Read-only file system (os error 69)        |
    | /tmp        | /dev        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /etc        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /tmp        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /tmp        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /tmp        | /home       | ERR: Read-only file system (os error 69)        |
    | /tmp        | /lib        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /tmp        | /opt        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /proc       | ERR: Read-only file system (os error 69)        |
    | /tmp        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /tmp        | /root       | ERR: Read-only file system (os error 69)        |
    | /tmp        | /run        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /tmp        | /srv        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /sys        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /usr        | ERR: Read-only file system (os error 69)        |
    | /tmp        | /var        | ERR: Read-only file system (os error 69)        |
    | /tmp        | \0          | ERR: file name contained an unexpected NUL byte |
    | /tmp        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /usr        |             | ERR: Read-only file system (os error 69)        |
    | /usr        | .           | ERR: Read-only file system (os error 69)        |
    | /usr        | ..          | ERR: Read-only file system (os error 69)        |
    | /usr        | /           | ERR: Read-only file system (os error 69)        |
    | /usr        | /bin        | ERR: Read-only file system (os error 69)        |
    | /usr        | /boot       | ERR: Read-only file system (os error 69)        |
    | /usr        | /dev        | ERR: Read-only file system (os error 69)        |
    | /usr        | /etc        | ERR: Read-only file system (os error 69)        |
    | /usr        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /usr        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /usr        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /usr        | /home       | ERR: Read-only file system (os error 69)        |
    | /usr        | /lib        | ERR: Read-only file system (os error 69)        |
    | /usr        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /usr        | /opt        | ERR: Read-only file system (os error 69)        |
    | /usr        | /proc       | ERR: Read-only file system (os error 69)        |
    | /usr        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /usr        | /root       | ERR: Read-only file system (os error 69)        |
    | /usr        | /run        | ERR: Read-only file system (os error 69)        |
    | /usr        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /usr        | /srv        | ERR: Read-only file system (os error 69)        |
    | /usr        | /sys        | ERR: Read-only file system (os error 69)        |
    | /usr        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /usr        | /usr        | ERR: Read-only file system (os error 69)        |
    | /usr        | /var        | ERR: Read-only file system (os error 69)        |
    | /usr        | \0          | ERR: file name contained an unexpected NUL byte |
    | /usr        | /x/..       | ERR: Read-only file system (os error 69)        |
    | /var        |             | ERR: Read-only file system (os error 69)        |
    | /var        | .           | ERR: Read-only file system (os error 69)        |
    | /var        | ..          | ERR: Read-only file system (os error 69)        |
    | /var        | /           | ERR: Read-only file system (os error 69)        |
    | /var        | /bin        | ERR: Read-only file system (os error 69)        |
    | /var        | /boot       | ERR: Read-only file system (os error 69)        |
    | /var        | /dev        | ERR: Read-only file system (os error 69)        |
    | /var        | /etc        | ERR: Read-only file system (os error 69)        |
    | /var        | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /var        | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /var        | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /var        | /home       | ERR: Read-only file system (os error 69)        |
    | /var        | /lib        | ERR: Read-only file system (os error 69)        |
    | /var        | /lib64      | ERR: Read-only file system (os error 69)        |
    | /var        | /opt        | ERR: Read-only file system (os error 69)        |
    | /var        | /proc       | ERR: Read-only file system (os error 69)        |
    | /var        | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /var        | /root       | ERR: Read-only file system (os error 69)        |
    | /var        | /run        | ERR: Read-only file system (os error 69)        |
    | /var        | /sbin       | ERR: Read-only file system (os error 69)        |
    | /var        | /srv        | ERR: Read-only file system (os error 69)        |
    | /var        | /sys        | ERR: Read-only file system (os error 69)        |
    | /var        | /tmp        | ERR: Read-only file system (os error 69)        |
    | /var        | /usr        | ERR: Read-only file system (os error 69)        |
    | /var        | /var        | ERR: Read-only file system (os error 69)        |
    | /var        | \0          | ERR: file name contained an unexpected NUL byte |
    | /var        | /x/..       | ERR: Read-only file system (os error 69)        |
    | \0          |             | ERR: file name contained an unexpected NUL byte |
    | \0          | .           | ERR: file name contained an unexpected NUL byte |
    | \0          | ..          | ERR: file name contained an unexpected NUL byte |
    | \0          | /           | ERR: file name contained an unexpected NUL byte |
    | \0          | /bin        | ERR: file name contained an unexpected NUL byte |
    | \0          | /boot       | ERR: file name contained an unexpected NUL byte |
    | \0          | /dev        | ERR: file name contained an unexpected NUL byte |
    | \0          | /etc        | ERR: file name contained an unexpected NUL byte |
    | \0          | /etc/group  | ERR: file name contained an unexpected NUL byte |
    | \0          | /etc/passwd | ERR: file name contained an unexpected NUL byte |
    | \0          | /etc/shadow | ERR: file name contained an unexpected NUL byte |
    | \0          | /home       | ERR: file name contained an unexpected NUL byte |
    | \0          | /lib        | ERR: file name contained an unexpected NUL byte |
    | \0          | /lib64      | ERR: file name contained an unexpected NUL byte |
    | \0          | /opt        | ERR: file name contained an unexpected NUL byte |
    | \0          | /proc       | ERR: file name contained an unexpected NUL byte |
    | \0          | /proc/self  | ERR: file name contained an unexpected NUL byte |
    | \0          | /root       | ERR: file name contained an unexpected NUL byte |
    | \0          | /run        | ERR: file name contained an unexpected NUL byte |
    | \0          | /sbin       | ERR: file name contained an unexpected NUL byte |
    | \0          | /srv        | ERR: file name contained an unexpected NUL byte |
    | \0          | /sys        | ERR: file name contained an unexpected NUL byte |
    | \0          | /tmp        | ERR: file name contained an unexpected NUL byte |
    | \0          | /usr        | ERR: file name contained an unexpected NUL byte |
    | \0          | /var        | ERR: file name contained an unexpected NUL byte |
    | \0          | \0          | ERR: file name contained an unexpected NUL byte |
    | \0          | /x/..       | ERR: file name contained an unexpected NUL byte |
    | /x/..       |             | ERR: Read-only file system (os error 69)        |
    | /x/..       | .           | ERR: Read-only file system (os error 69)        |
    | /x/..       | ..          | ERR: Read-only file system (os error 69)        |
    | /x/..       | /           | ERR: Read-only file system (os error 69)        |
    | /x/..       | /bin        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /boot       | ERR: Read-only file system (os error 69)        |
    | /x/..       | /dev        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /etc        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /etc/group  | ERR: Read-only file system (os error 69)        |
    | /x/..       | /etc/passwd | ERR: Read-only file system (os error 69)        |
    | /x/..       | /etc/shadow | ERR: Read-only file system (os error 69)        |
    | /x/..       | /home       | ERR: Read-only file system (os error 69)        |
    | /x/..       | /lib        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /lib64      | ERR: Read-only file system (os error 69)        |
    | /x/..       | /opt        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /proc       | ERR: Read-only file system (os error 69)        |
    | /x/..       | /proc/self  | ERR: Read-only file system (os error 69)        |
    | /x/..       | /root       | ERR: Read-only file system (os error 69)        |
    | /x/..       | /run        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /sbin       | ERR: Read-only file system (os error 69)        |
    | /x/..       | /srv        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /sys        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /tmp        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /usr        | ERR: Read-only file system (os error 69)        |
    | /x/..       | /var        | ERR: Read-only file system (os error 69)        |
    | /x/..       | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | /x/..       | ERR: Read-only file system (os error 69)        |
    +-------------+-------------+-------------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_set_permissions() {
    let udf = udf("set_permissions").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-----------------------------------------------+
    | path        | result                                        |
    +-------------+-----------------------------------------------+
    |             | ERR: operation not supported on this platform |
    | .           | ERR: operation not supported on this platform |
    | ..          | ERR: operation not supported on this platform |
    | /           | ERR: operation not supported on this platform |
    | /bin        | ERR: operation not supported on this platform |
    | /boot       | ERR: operation not supported on this platform |
    | /dev        | ERR: operation not supported on this platform |
    | /etc        | ERR: operation not supported on this platform |
    | /etc/group  | ERR: operation not supported on this platform |
    | /etc/passwd | ERR: operation not supported on this platform |
    | /etc/shadow | ERR: operation not supported on this platform |
    | /home       | ERR: operation not supported on this platform |
    | /lib        | ERR: operation not supported on this platform |
    | /lib64      | ERR: operation not supported on this platform |
    | /opt        | ERR: operation not supported on this platform |
    | /proc       | ERR: operation not supported on this platform |
    | /proc/self  | ERR: operation not supported on this platform |
    | /root       | ERR: operation not supported on this platform |
    | /run        | ERR: operation not supported on this platform |
    | /sbin       | ERR: operation not supported on this platform |
    | /srv        | ERR: operation not supported on this platform |
    | /sys        | ERR: operation not supported on this platform |
    | /tmp        | ERR: operation not supported on this platform |
    | /usr        | ERR: operation not supported on this platform |
    | /var        | ERR: operation not supported on this platform |
    | \0          | ERR: operation not supported on this platform |
    | /x/..       | ERR: operation not supported on this platform |
    +-------------+-----------------------------------------------+
    ",
    );
}

#[tokio::test]
async fn test_symlink_metadata() {
    let udf = udf("symlink_metadata").await;

    insta::assert_snapshot!(
        run_1(&udf).await,
        @r"
    +-------------+-------------------------------------------------+
    | path        | result                                          |
    +-------------+-------------------------------------------------+
    |             | OK: got data                                    |
    | .           | OK: got data                                    |
    | ..          | OK: got data                                    |
    | /           | OK: got data                                    |
    | /bin        | ERR: No such file or directory (os error 44)    |
    | /boot       | ERR: No such file or directory (os error 44)    |
    | /dev        | ERR: No such file or directory (os error 44)    |
    | /etc        | ERR: No such file or directory (os error 44)    |
    | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /home       | ERR: No such file or directory (os error 44)    |
    | /lib        | ERR: No such file or directory (os error 44)    |
    | /lib64      | ERR: No such file or directory (os error 44)    |
    | /opt        | ERR: No such file or directory (os error 44)    |
    | /proc       | ERR: No such file or directory (os error 44)    |
    | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /root       | ERR: No such file or directory (os error 44)    |
    | /run        | ERR: No such file or directory (os error 44)    |
    | /sbin       | ERR: No such file or directory (os error 44)    |
    | /srv        | ERR: No such file or directory (os error 44)    |
    | /sys        | ERR: No such file or directory (os error 44)    |
    | /tmp        | ERR: No such file or directory (os error 44)    |
    | /usr        | ERR: No such file or directory (os error 44)    |
    | /var        | ERR: No such file or directory (os error 44)    |
    | \0          | ERR: file name contained an unexpected NUL byte |
    | /x/..       | ERR: No such file or directory (os error 44)    |
    +-------------+-------------------------------------------------+
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
fn cross<S>(array: &[S]) -> (Vec<String>, Vec<String>)
where
    S: ToString,
{
    let mut out_a = Vec::with_capacity(array.len() * array.len());
    let mut out_b = Vec::with_capacity(array.len() * array.len());

    for a in array {
        for b in array {
            out_a.push(a.to_string());
            out_b.push(b.to_string());
        }
    }

    (out_a, out_b)
}

/// Make path nicely printable.
fn nice_path(path: &str) -> String {
    path.replace("\0", r#"\0"#)
}

/// Run UDF that expects one string input.
async fn run_1(udf: &WasmScalarUdf) -> String {
    let input = Arc::new(
        PATHS
            .iter()
            .map(|p| Some(p.to_owned()))
            .collect::<StringArray>(),
    );
    let input_nice = Arc::new(
        input
            .iter()
            .map(|s| s.map(nice_path))
            .collect::<StringArray>(),
    ) as _;

    let result = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input as _)],
            arg_fields: vec![Arc::new(Field::new("a", DataType::Utf8, true))],
            number_rows: PATHS.len(),
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();

    batches_to_string(&[
        RecordBatch::try_from_iter([("path", input_nice), ("result", result)]).unwrap(),
    ])
}

/// Run UDF that expects two string inputs.
async fn run_2(udf: &WasmScalarUdf) -> String {
    let (paths_from, paths_to) = cross(PATHS);

    let input_from = Arc::new(paths_from.into_iter().map(Some).collect::<StringArray>());
    let input_to = Arc::new(paths_to.into_iter().map(Some).collect::<StringArray>());

    let input_from_nice = Arc::new(
        input_from
            .iter()
            .map(|s| s.map(nice_path))
            .collect::<StringArray>(),
    ) as _;
    let input_to_nice = Arc::new(
        input_to
            .iter()
            .map(|s| s.map(nice_path))
            .collect::<StringArray>(),
    ) as _;

    let number_rows = input_from.len();

    let result = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(input_from as _),
                ColumnarValue::Array(input_to as _),
            ],
            arg_fields: vec![
                Arc::new(Field::new("from", DataType::Utf8, true)),
                Arc::new(Field::new("to", DataType::Utf8, true)),
            ],
            number_rows,
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();

    batches_to_string(&[RecordBatch::try_from_iter([
        ("from", input_from_nice),
        ("to", input_to_nice),
        ("output", result),
    ])
    .unwrap()])
}
