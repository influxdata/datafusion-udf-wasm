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
        |             | /x/..       | ERR: Invalid seek (os error 70)                 |
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
        | .           | /x/..       | ERR: Invalid seek (os error 70)                 |
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
        | ..          | /tmp        | ERR: Out of memory (os error 48)                |
        | ..          | /usr        | ERR: Out of memory (os error 48)                |
        | ..          | /var        | ERR: Out of memory (os error 48)                |
        | ..          | \0          | ERR: Out of memory (os error 48)                |
        | ..          | /x/..       | ERR: Out of memory (os error 48)                |
        | /           |             | ERR: Out of memory (os error 48)                |
        | /           | .           | ERR: Out of memory (os error 48)                |
        | /           | ..          | ERR: Out of memory (os error 48)                |
        | /           | /           | ERR: Out of memory (os error 48)                |
        | /           | /bin        | ERR: Out of memory (os error 48)                |
        | /           | /boot       | ERR: Out of memory (os error 48)                |
        | /           | /dev        | ERR: Out of memory (os error 48)                |
        | /           | /etc        | ERR: Out of memory (os error 48)                |
        | /           | /etc/group  | ERR: Out of memory (os error 48)                |
        | /           | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /           | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /           | /home       | ERR: Out of memory (os error 48)                |
        | /           | /lib        | ERR: Out of memory (os error 48)                |
        | /           | /lib64      | ERR: Out of memory (os error 48)                |
        | /           | /opt        | ERR: Out of memory (os error 48)                |
        | /           | /proc       | ERR: Out of memory (os error 48)                |
        | /           | /proc/self  | ERR: Out of memory (os error 48)                |
        | /           | /root       | ERR: Out of memory (os error 48)                |
        | /           | /run        | ERR: Out of memory (os error 48)                |
        | /           | /sbin       | ERR: Out of memory (os error 48)                |
        | /           | /srv        | ERR: Out of memory (os error 48)                |
        | /           | /sys        | ERR: Out of memory (os error 48)                |
        | /           | /tmp        | ERR: Out of memory (os error 48)                |
        | /           | /usr        | ERR: Out of memory (os error 48)                |
        | /           | /var        | ERR: Out of memory (os error 48)                |
        | /           | \0          | ERR: Out of memory (os error 48)                |
        | /           | /x/..       | ERR: Out of memory (os error 48)                |
        | /bin        |             | ERR: Out of memory (os error 48)                |
        | /bin        | .           | ERR: Out of memory (os error 48)                |
        | /bin        | ..          | ERR: Out of memory (os error 48)                |
        | /bin        | /           | ERR: Out of memory (os error 48)                |
        | /bin        | /bin        | ERR: Out of memory (os error 48)                |
        | /bin        | /boot       | ERR: Out of memory (os error 48)                |
        | /bin        | /dev        | ERR: Out of memory (os error 48)                |
        | /bin        | /etc        | ERR: Out of memory (os error 48)                |
        | /bin        | /etc/group  | ERR: Out of memory (os error 48)                |
        | /bin        | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /bin        | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /bin        | /home       | ERR: Out of memory (os error 48)                |
        | /bin        | /lib        | ERR: Out of memory (os error 48)                |
        | /bin        | /lib64      | ERR: Out of memory (os error 48)                |
        | /bin        | /opt        | ERR: Out of memory (os error 48)                |
        | /bin        | /proc       | ERR: Out of memory (os error 48)                |
        | /bin        | /proc/self  | ERR: Out of memory (os error 48)                |
        | /bin        | /root       | ERR: Out of memory (os error 48)                |
        | /bin        | /run        | ERR: Out of memory (os error 48)                |
        | /bin        | /sbin       | ERR: Out of memory (os error 48)                |
        | /bin        | /srv        | ERR: Out of memory (os error 48)                |
        | /bin        | /sys        | ERR: Out of memory (os error 48)                |
        | /bin        | /tmp        | ERR: Out of memory (os error 48)                |
        | /bin        | /usr        | ERR: Out of memory (os error 48)                |
        | /bin        | /var        | ERR: Out of memory (os error 48)                |
        | /bin        | \0          | ERR: Out of memory (os error 48)                |
        | /bin        | /x/..       | ERR: Out of memory (os error 48)                |
        | /boot       |             | ERR: Out of memory (os error 48)                |
        | /boot       | .           | ERR: Out of memory (os error 48)                |
        | /boot       | ..          | ERR: Out of memory (os error 48)                |
        | /boot       | /           | ERR: Out of memory (os error 48)                |
        | /boot       | /bin        | ERR: Out of memory (os error 48)                |
        | /boot       | /boot       | ERR: Out of memory (os error 48)                |
        | /boot       | /dev        | ERR: Out of memory (os error 48)                |
        | /boot       | /etc        | ERR: Out of memory (os error 48)                |
        | /boot       | /etc/group  | ERR: Out of memory (os error 48)                |
        | /boot       | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /boot       | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /boot       | /home       | ERR: Out of memory (os error 48)                |
        | /boot       | /lib        | ERR: Out of memory (os error 48)                |
        | /boot       | /lib64      | ERR: Out of memory (os error 48)                |
        | /boot       | /opt        | ERR: Out of memory (os error 48)                |
        | /boot       | /proc       | ERR: Out of memory (os error 48)                |
        | /boot       | /proc/self  | ERR: Out of memory (os error 48)                |
        | /boot       | /root       | ERR: Out of memory (os error 48)                |
        | /boot       | /run        | ERR: Out of memory (os error 48)                |
        | /boot       | /sbin       | ERR: Out of memory (os error 48)                |
        | /boot       | /srv        | ERR: Out of memory (os error 48)                |
        | /boot       | /sys        | ERR: Out of memory (os error 48)                |
        | /boot       | /tmp        | ERR: Out of memory (os error 48)                |
        | /boot       | /usr        | ERR: Out of memory (os error 48)                |
        | /boot       | /var        | ERR: Out of memory (os error 48)                |
        | /boot       | \0          | ERR: Out of memory (os error 48)                |
        | /boot       | /x/..       | ERR: Out of memory (os error 48)                |
        | /dev        |             | ERR: Out of memory (os error 48)                |
        | /dev        | .           | ERR: Out of memory (os error 48)                |
        | /dev        | ..          | ERR: Out of memory (os error 48)                |
        | /dev        | /           | ERR: Out of memory (os error 48)                |
        | /dev        | /bin        | ERR: Out of memory (os error 48)                |
        | /dev        | /boot       | ERR: Out of memory (os error 48)                |
        | /dev        | /dev        | ERR: Out of memory (os error 48)                |
        | /dev        | /etc        | ERR: Out of memory (os error 48)                |
        | /dev        | /etc/group  | ERR: Out of memory (os error 48)                |
        | /dev        | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /dev        | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /dev        | /home       | ERR: Out of memory (os error 48)                |
        | /dev        | /lib        | ERR: Out of memory (os error 48)                |
        | /dev        | /lib64      | ERR: Out of memory (os error 48)                |
        | /dev        | /opt        | ERR: Out of memory (os error 48)                |
        | /dev        | /proc       | ERR: Out of memory (os error 48)                |
        | /dev        | /proc/self  | ERR: Out of memory (os error 48)                |
        | /dev        | /root       | ERR: Out of memory (os error 48)                |
        | /dev        | /run        | ERR: Out of memory (os error 48)                |
        | /dev        | /sbin       | ERR: Out of memory (os error 48)                |
        | /dev        | /srv        | ERR: Out of memory (os error 48)                |
        | /dev        | /sys        | ERR: Out of memory (os error 48)                |
        | /dev        | /tmp        | ERR: Out of memory (os error 48)                |
        | /dev        | /usr        | ERR: Out of memory (os error 48)                |
        | /dev        | /var        | ERR: Out of memory (os error 48)                |
        | /dev        | \0          | ERR: Out of memory (os error 48)                |
        | /dev        | /x/..       | ERR: Out of memory (os error 48)                |
        | /etc        |             | ERR: Out of memory (os error 48)                |
        | /etc        | .           | ERR: Out of memory (os error 48)                |
        | /etc        | ..          | ERR: Out of memory (os error 48)                |
        | /etc        | /           | ERR: Out of memory (os error 48)                |
        | /etc        | /bin        | ERR: Out of memory (os error 48)                |
        | /etc        | /boot       | ERR: Out of memory (os error 48)                |
        | /etc        | /dev        | ERR: Out of memory (os error 48)                |
        | /etc        | /etc        | ERR: Out of memory (os error 48)                |
        | /etc        | /etc/group  | ERR: Out of memory (os error 48)                |
        | /etc        | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /etc        | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /etc        | /home       | ERR: Out of memory (os error 48)                |
        | /etc        | /lib        | ERR: Out of memory (os error 48)                |
        | /etc        | /lib64      | ERR: Out of memory (os error 48)                |
        | /etc        | /opt        | ERR: Out of memory (os error 48)                |
        | /etc        | /proc       | ERR: Out of memory (os error 48)                |
        | /etc        | /proc/self  | ERR: Out of memory (os error 48)                |
        | /etc        | /root       | ERR: Out of memory (os error 48)                |
        | /etc        | /run        | ERR: Out of memory (os error 48)                |
        | /etc        | /sbin       | ERR: Out of memory (os error 48)                |
        | /etc        | /srv        | ERR: Out of memory (os error 48)                |
        | /etc        | /sys        | ERR: Out of memory (os error 48)                |
        | /etc        | /tmp        | ERR: Out of memory (os error 48)                |
        | /etc        | /usr        | ERR: Out of memory (os error 48)                |
        | /etc        | /var        | ERR: Out of memory (os error 48)                |
        | /etc        | \0          | ERR: Out of memory (os error 48)                |
        | /etc        | /x/..       | ERR: Out of memory (os error 48)                |
        | /etc/group  |             | ERR: No such file or directory (os error 44)    |
        | /etc/group  | .           | ERR: No such file or directory (os error 44)    |
        | /etc/group  | ..          | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /           | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /bin        | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /boot       | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /dev        | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /etc        | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /etc/group  | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /etc/passwd | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /etc/shadow | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /home       | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /lib        | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /lib64      | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /opt        | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /proc       | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /proc/self  | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /root       | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /run        | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /sbin       | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /srv        | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /sys        | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /tmp        | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /usr        | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /var        | ERR: No such file or directory (os error 44)    |
        | /etc/group  | \0          | ERR: No such file or directory (os error 44)    |
        | /etc/group  | /x/..       | ERR: No such file or directory (os error 44)    |
        | /etc/passwd |             | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | .           | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | ..          | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /           | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /bin        | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /boot       | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /dev        | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /etc        | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /etc/group  | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /etc/passwd | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /etc/shadow | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /home       | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /lib        | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /lib64      | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /opt        | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /proc       | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /proc/self  | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /root       | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /run        | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /sbin       | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /srv        | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /sys        | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /tmp        | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /usr        | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /var        | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | \0          | ERR: No such file or directory (os error 44)    |
        | /etc/passwd | /x/..       | ERR: No such file or directory (os error 44)    |
        | /etc/shadow |             | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | .           | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | ..          | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /           | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /bin        | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /boot       | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /dev        | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /etc        | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /etc/group  | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /etc/passwd | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /etc/shadow | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /home       | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /lib        | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /lib64      | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /opt        | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /proc       | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /proc/self  | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /root       | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /run        | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /sbin       | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /srv        | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /sys        | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /tmp        | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /usr        | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /var        | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | \0          | ERR: No such file or directory (os error 44)    |
        | /etc/shadow | /x/..       | ERR: No such file or directory (os error 44)    |
        | /home       |             | ERR: Out of memory (os error 48)                |
        | /home       | .           | ERR: Out of memory (os error 48)                |
        | /home       | ..          | ERR: Out of memory (os error 48)                |
        | /home       | /           | ERR: Out of memory (os error 48)                |
        | /home       | /bin        | ERR: Out of memory (os error 48)                |
        | /home       | /boot       | ERR: Out of memory (os error 48)                |
        | /home       | /dev        | ERR: Out of memory (os error 48)                |
        | /home       | /etc        | ERR: Out of memory (os error 48)                |
        | /home       | /etc/group  | ERR: Out of memory (os error 48)                |
        | /home       | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /home       | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /home       | /home       | ERR: Out of memory (os error 48)                |
        | /home       | /lib        | ERR: Out of memory (os error 48)                |
        | /home       | /lib64      | ERR: Out of memory (os error 48)                |
        | /home       | /opt        | ERR: Out of memory (os error 48)                |
        | /home       | /proc       | ERR: Out of memory (os error 48)                |
        | /home       | /proc/self  | ERR: Out of memory (os error 48)                |
        | /home       | /root       | ERR: Out of memory (os error 48)                |
        | /home       | /run        | ERR: Out of memory (os error 48)                |
        | /home       | /sbin       | ERR: Out of memory (os error 48)                |
        | /home       | /srv        | ERR: Out of memory (os error 48)                |
        | /home       | /sys        | ERR: Out of memory (os error 48)                |
        | /home       | /tmp        | ERR: Out of memory (os error 48)                |
        | /home       | /usr        | ERR: Out of memory (os error 48)                |
        | /home       | /var        | ERR: Out of memory (os error 48)                |
        | /home       | \0          | ERR: Out of memory (os error 48)                |
        | /home       | /x/..       | ERR: Out of memory (os error 48)                |
        | /lib        |             | ERR: Out of memory (os error 48)                |
        | /lib        | .           | ERR: Out of memory (os error 48)                |
        | /lib        | ..          | ERR: Out of memory (os error 48)                |
        | /lib        | /           | ERR: Out of memory (os error 48)                |
        | /lib        | /bin        | ERR: Out of memory (os error 48)                |
        | /lib        | /boot       | ERR: Out of memory (os error 48)                |
        | /lib        | /dev        | ERR: Out of memory (os error 48)                |
        | /lib        | /etc        | ERR: Out of memory (os error 48)                |
        | /lib        | /etc/group  | ERR: Out of memory (os error 48)                |
        | /lib        | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /lib        | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /lib        | /home       | ERR: Out of memory (os error 48)                |
        | /lib        | /lib        | ERR: Out of memory (os error 48)                |
        | /lib        | /lib64      | ERR: Out of memory (os error 48)                |
        | /lib        | /opt        | ERR: Out of memory (os error 48)                |
        | /lib        | /proc       | ERR: Out of memory (os error 48)                |
        | /lib        | /proc/self  | ERR: Out of memory (os error 48)                |
        | /lib        | /root       | ERR: Out of memory (os error 48)                |
        | /lib        | /run        | ERR: Out of memory (os error 48)                |
        | /lib        | /sbin       | ERR: Out of memory (os error 48)                |
        | /lib        | /srv        | ERR: Out of memory (os error 48)                |
        | /lib        | /sys        | ERR: Out of memory (os error 48)                |
        | /lib        | /tmp        | ERR: Out of memory (os error 48)                |
        | /lib        | /usr        | ERR: Out of memory (os error 48)                |
        | /lib        | /var        | ERR: Out of memory (os error 48)                |
        | /lib        | \0          | ERR: Out of memory (os error 48)                |
        | /lib        | /x/..       | ERR: Out of memory (os error 48)                |
        | /lib64      |             | ERR: Out of memory (os error 48)                |
        | /lib64      | .           | ERR: Out of memory (os error 48)                |
        | /lib64      | ..          | ERR: Out of memory (os error 48)                |
        | /lib64      | /           | ERR: Out of memory (os error 48)                |
        | /lib64      | /bin        | ERR: Out of memory (os error 48)                |
        | /lib64      | /boot       | ERR: Out of memory (os error 48)                |
        | /lib64      | /dev        | ERR: Out of memory (os error 48)                |
        | /lib64      | /etc        | ERR: Out of memory (os error 48)                |
        | /lib64      | /etc/group  | ERR: Out of memory (os error 48)                |
        | /lib64      | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /lib64      | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /lib64      | /home       | ERR: Out of memory (os error 48)                |
        | /lib64      | /lib        | ERR: Out of memory (os error 48)                |
        | /lib64      | /lib64      | ERR: Out of memory (os error 48)                |
        | /lib64      | /opt        | ERR: Out of memory (os error 48)                |
        | /lib64      | /proc       | ERR: Out of memory (os error 48)                |
        | /lib64      | /proc/self  | ERR: Out of memory (os error 48)                |
        | /lib64      | /root       | ERR: Out of memory (os error 48)                |
        | /lib64      | /run        | ERR: Out of memory (os error 48)                |
        | /lib64      | /sbin       | ERR: Out of memory (os error 48)                |
        | /lib64      | /srv        | ERR: Out of memory (os error 48)                |
        | /lib64      | /sys        | ERR: Out of memory (os error 48)                |
        | /lib64      | /tmp        | ERR: Out of memory (os error 48)                |
        | /lib64      | /usr        | ERR: Out of memory (os error 48)                |
        | /lib64      | /var        | ERR: Out of memory (os error 48)                |
        | /lib64      | \0          | ERR: Out of memory (os error 48)                |
        | /lib64      | /x/..       | ERR: Out of memory (os error 48)                |
        | /opt        |             | ERR: Out of memory (os error 48)                |
        | /opt        | .           | ERR: Out of memory (os error 48)                |
        | /opt        | ..          | ERR: Out of memory (os error 48)                |
        | /opt        | /           | ERR: Out of memory (os error 48)                |
        | /opt        | /bin        | ERR: Out of memory (os error 48)                |
        | /opt        | /boot       | ERR: Out of memory (os error 48)                |
        | /opt        | /dev        | ERR: Out of memory (os error 48)                |
        | /opt        | /etc        | ERR: Out of memory (os error 48)                |
        | /opt        | /etc/group  | ERR: Out of memory (os error 48)                |
        | /opt        | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /opt        | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /opt        | /home       | ERR: Out of memory (os error 48)                |
        | /opt        | /lib        | ERR: Out of memory (os error 48)                |
        | /opt        | /lib64      | ERR: Out of memory (os error 48)                |
        | /opt        | /opt        | ERR: Out of memory (os error 48)                |
        | /opt        | /proc       | ERR: Out of memory (os error 48)                |
        | /opt        | /proc/self  | ERR: Out of memory (os error 48)                |
        | /opt        | /root       | ERR: Out of memory (os error 48)                |
        | /opt        | /run        | ERR: Out of memory (os error 48)                |
        | /opt        | /sbin       | ERR: Out of memory (os error 48)                |
        | /opt        | /srv        | ERR: Out of memory (os error 48)                |
        | /opt        | /sys        | ERR: Out of memory (os error 48)                |
        | /opt        | /tmp        | ERR: Out of memory (os error 48)                |
        | /opt        | /usr        | ERR: Out of memory (os error 48)                |
        | /opt        | /var        | ERR: Out of memory (os error 48)                |
        | /opt        | \0          | ERR: Out of memory (os error 48)                |
        | /opt        | /x/..       | ERR: Out of memory (os error 48)                |
        | /proc       |             | ERR: Out of memory (os error 48)                |
        | /proc       | .           | ERR: Out of memory (os error 48)                |
        | /proc       | ..          | ERR: Out of memory (os error 48)                |
        | /proc       | /           | ERR: Out of memory (os error 48)                |
        | /proc       | /bin        | ERR: Out of memory (os error 48)                |
        | /proc       | /boot       | ERR: Out of memory (os error 48)                |
        | /proc       | /dev        | ERR: Out of memory (os error 48)                |
        | /proc       | /etc        | ERR: Out of memory (os error 48)                |
        | /proc       | /etc/group  | ERR: Out of memory (os error 48)                |
        | /proc       | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /proc       | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /proc       | /home       | ERR: Out of memory (os error 48)                |
        | /proc       | /lib        | ERR: Out of memory (os error 48)                |
        | /proc       | /lib64      | ERR: Out of memory (os error 48)                |
        | /proc       | /opt        | ERR: Out of memory (os error 48)                |
        | /proc       | /proc       | ERR: Out of memory (os error 48)                |
        | /proc       | /proc/self  | ERR: Out of memory (os error 48)                |
        | /proc       | /root       | ERR: Out of memory (os error 48)                |
        | /proc       | /run        | ERR: Out of memory (os error 48)                |
        | /proc       | /sbin       | ERR: Out of memory (os error 48)                |
        | /proc       | /srv        | ERR: Out of memory (os error 48)                |
        | /proc       | /sys        | ERR: Out of memory (os error 48)                |
        | /proc       | /tmp        | ERR: Out of memory (os error 48)                |
        | /proc       | /usr        | ERR: Out of memory (os error 48)                |
        | /proc       | /var        | ERR: Out of memory (os error 48)                |
        | /proc       | \0          | ERR: Out of memory (os error 48)                |
        | /proc       | /x/..       | ERR: Out of memory (os error 48)                |
        | /proc/self  |             | ERR: No such file or directory (os error 44)    |
        | /proc/self  | .           | ERR: No such file or directory (os error 44)    |
        | /proc/self  | ..          | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /           | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /bin        | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /boot       | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /dev        | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /etc        | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /etc/group  | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /etc/passwd | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /etc/shadow | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /home       | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /lib        | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /lib64      | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /opt        | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /proc       | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /proc/self  | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /root       | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /run        | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /sbin       | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /srv        | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /sys        | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /tmp        | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /usr        | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /var        | ERR: No such file or directory (os error 44)    |
        | /proc/self  | \0          | ERR: No such file or directory (os error 44)    |
        | /proc/self  | /x/..       | ERR: No such file or directory (os error 44)    |
        | /root       |             | ERR: Out of memory (os error 48)                |
        | /root       | .           | ERR: Out of memory (os error 48)                |
        | /root       | ..          | ERR: Out of memory (os error 48)                |
        | /root       | /           | ERR: Out of memory (os error 48)                |
        | /root       | /bin        | ERR: Out of memory (os error 48)                |
        | /root       | /boot       | ERR: Out of memory (os error 48)                |
        | /root       | /dev        | ERR: Out of memory (os error 48)                |
        | /root       | /etc        | ERR: Out of memory (os error 48)                |
        | /root       | /etc/group  | ERR: Out of memory (os error 48)                |
        | /root       | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /root       | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /root       | /home       | ERR: Out of memory (os error 48)                |
        | /root       | /lib        | ERR: Out of memory (os error 48)                |
        | /root       | /lib64      | ERR: Out of memory (os error 48)                |
        | /root       | /opt        | ERR: Out of memory (os error 48)                |
        | /root       | /proc       | ERR: Out of memory (os error 48)                |
        | /root       | /proc/self  | ERR: Out of memory (os error 48)                |
        | /root       | /root       | ERR: Out of memory (os error 48)                |
        | /root       | /run        | ERR: Out of memory (os error 48)                |
        | /root       | /sbin       | ERR: Out of memory (os error 48)                |
        | /root       | /srv        | ERR: Out of memory (os error 48)                |
        | /root       | /sys        | ERR: Out of memory (os error 48)                |
        | /root       | /tmp        | ERR: Out of memory (os error 48)                |
        | /root       | /usr        | ERR: Out of memory (os error 48)                |
        | /root       | /var        | ERR: Out of memory (os error 48)                |
        | /root       | \0          | ERR: Out of memory (os error 48)                |
        | /root       | /x/..       | ERR: Out of memory (os error 48)                |
        | /run        |             | ERR: Out of memory (os error 48)                |
        | /run        | .           | ERR: Out of memory (os error 48)                |
        | /run        | ..          | ERR: Out of memory (os error 48)                |
        | /run        | /           | ERR: Out of memory (os error 48)                |
        | /run        | /bin        | ERR: Out of memory (os error 48)                |
        | /run        | /boot       | ERR: Out of memory (os error 48)                |
        | /run        | /dev        | ERR: Out of memory (os error 48)                |
        | /run        | /etc        | ERR: Out of memory (os error 48)                |
        | /run        | /etc/group  | ERR: Out of memory (os error 48)                |
        | /run        | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /run        | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /run        | /home       | ERR: Out of memory (os error 48)                |
        | /run        | /lib        | ERR: Out of memory (os error 48)                |
        | /run        | /lib64      | ERR: Out of memory (os error 48)                |
        | /run        | /opt        | ERR: Out of memory (os error 48)                |
        | /run        | /proc       | ERR: Out of memory (os error 48)                |
        | /run        | /proc/self  | ERR: Out of memory (os error 48)                |
        | /run        | /root       | ERR: Out of memory (os error 48)                |
        | /run        | /run        | ERR: Out of memory (os error 48)                |
        | /run        | /sbin       | ERR: Out of memory (os error 48)                |
        | /run        | /srv        | ERR: Out of memory (os error 48)                |
        | /run        | /sys        | ERR: Out of memory (os error 48)                |
        | /run        | /tmp        | ERR: Out of memory (os error 48)                |
        | /run        | /usr        | ERR: Out of memory (os error 48)                |
        | /run        | /var        | ERR: Out of memory (os error 48)                |
        | /run        | \0          | ERR: Out of memory (os error 48)                |
        | /run        | /x/..       | ERR: Out of memory (os error 48)                |
        | /sbin       |             | ERR: Out of memory (os error 48)                |
        | /sbin       | .           | ERR: Out of memory (os error 48)                |
        | /sbin       | ..          | ERR: Out of memory (os error 48)                |
        | /sbin       | /           | ERR: Out of memory (os error 48)                |
        | /sbin       | /bin        | ERR: Out of memory (os error 48)                |
        | /sbin       | /boot       | ERR: Out of memory (os error 48)                |
        | /sbin       | /dev        | ERR: Out of memory (os error 48)                |
        | /sbin       | /etc        | ERR: Out of memory (os error 48)                |
        | /sbin       | /etc/group  | ERR: Out of memory (os error 48)                |
        | /sbin       | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /sbin       | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /sbin       | /home       | ERR: Out of memory (os error 48)                |
        | /sbin       | /lib        | ERR: Out of memory (os error 48)                |
        | /sbin       | /lib64      | ERR: Out of memory (os error 48)                |
        | /sbin       | /opt        | ERR: Out of memory (os error 48)                |
        | /sbin       | /proc       | ERR: Out of memory (os error 48)                |
        | /sbin       | /proc/self  | ERR: Out of memory (os error 48)                |
        | /sbin       | /root       | ERR: Out of memory (os error 48)                |
        | /sbin       | /run        | ERR: Out of memory (os error 48)                |
        | /sbin       | /sbin       | ERR: Out of memory (os error 48)                |
        | /sbin       | /srv        | ERR: Out of memory (os error 48)                |
        | /sbin       | /sys        | ERR: Out of memory (os error 48)                |
        | /sbin       | /tmp        | ERR: Out of memory (os error 48)                |
        | /sbin       | /usr        | ERR: Out of memory (os error 48)                |
        | /sbin       | /var        | ERR: Out of memory (os error 48)                |
        | /sbin       | \0          | ERR: Out of memory (os error 48)                |
        | /sbin       | /x/..       | ERR: Out of memory (os error 48)                |
        | /srv        |             | ERR: Out of memory (os error 48)                |
        | /srv        | .           | ERR: Out of memory (os error 48)                |
        | /srv        | ..          | ERR: Out of memory (os error 48)                |
        | /srv        | /           | ERR: Out of memory (os error 48)                |
        | /srv        | /bin        | ERR: Out of memory (os error 48)                |
        | /srv        | /boot       | ERR: Out of memory (os error 48)                |
        | /srv        | /dev        | ERR: Out of memory (os error 48)                |
        | /srv        | /etc        | ERR: Out of memory (os error 48)                |
        | /srv        | /etc/group  | ERR: Out of memory (os error 48)                |
        | /srv        | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /srv        | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /srv        | /home       | ERR: Out of memory (os error 48)                |
        | /srv        | /lib        | ERR: Out of memory (os error 48)                |
        | /srv        | /lib64      | ERR: Out of memory (os error 48)                |
        | /srv        | /opt        | ERR: Out of memory (os error 48)                |
        | /srv        | /proc       | ERR: Out of memory (os error 48)                |
        | /srv        | /proc/self  | ERR: Out of memory (os error 48)                |
        | /srv        | /root       | ERR: Out of memory (os error 48)                |
        | /srv        | /run        | ERR: Out of memory (os error 48)                |
        | /srv        | /sbin       | ERR: Out of memory (os error 48)                |
        | /srv        | /srv        | ERR: Out of memory (os error 48)                |
        | /srv        | /sys        | ERR: Out of memory (os error 48)                |
        | /srv        | /tmp        | ERR: Out of memory (os error 48)                |
        | /srv        | /usr        | ERR: Out of memory (os error 48)                |
        | /srv        | /var        | ERR: Out of memory (os error 48)                |
        | /srv        | \0          | ERR: Out of memory (os error 48)                |
        | /srv        | /x/..       | ERR: Out of memory (os error 48)                |
        | /sys        |             | ERR: Out of memory (os error 48)                |
        | /sys        | .           | ERR: Out of memory (os error 48)                |
        | /sys        | ..          | ERR: Out of memory (os error 48)                |
        | /sys        | /           | ERR: Out of memory (os error 48)                |
        | /sys        | /bin        | ERR: Out of memory (os error 48)                |
        | /sys        | /boot       | ERR: Out of memory (os error 48)                |
        | /sys        | /dev        | ERR: Out of memory (os error 48)                |
        | /sys        | /etc        | ERR: Out of memory (os error 48)                |
        | /sys        | /etc/group  | ERR: Out of memory (os error 48)                |
        | /sys        | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /sys        | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /sys        | /home       | ERR: Out of memory (os error 48)                |
        | /sys        | /lib        | ERR: Out of memory (os error 48)                |
        | /sys        | /lib64      | ERR: Out of memory (os error 48)                |
        | /sys        | /opt        | ERR: Out of memory (os error 48)                |
        | /sys        | /proc       | ERR: Out of memory (os error 48)                |
        | /sys        | /proc/self  | ERR: Out of memory (os error 48)                |
        | /sys        | /root       | ERR: Out of memory (os error 48)                |
        | /sys        | /run        | ERR: Out of memory (os error 48)                |
        | /sys        | /sbin       | ERR: Out of memory (os error 48)                |
        | /sys        | /srv        | ERR: Out of memory (os error 48)                |
        | /sys        | /sys        | ERR: Out of memory (os error 48)                |
        | /sys        | /tmp        | ERR: Out of memory (os error 48)                |
        | /sys        | /usr        | ERR: Out of memory (os error 48)                |
        | /sys        | /var        | ERR: Out of memory (os error 48)                |
        | /sys        | \0          | ERR: Out of memory (os error 48)                |
        | /sys        | /x/..       | ERR: Out of memory (os error 48)                |
        | /tmp        |             | ERR: Out of memory (os error 48)                |
        | /tmp        | .           | ERR: Out of memory (os error 48)                |
        | /tmp        | ..          | ERR: Out of memory (os error 48)                |
        | /tmp        | /           | ERR: Out of memory (os error 48)                |
        | /tmp        | /bin        | ERR: Out of memory (os error 48)                |
        | /tmp        | /boot       | ERR: Out of memory (os error 48)                |
        | /tmp        | /dev        | ERR: Out of memory (os error 48)                |
        | /tmp        | /etc        | ERR: Out of memory (os error 48)                |
        | /tmp        | /etc/group  | ERR: Out of memory (os error 48)                |
        | /tmp        | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /tmp        | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /tmp        | /home       | ERR: Out of memory (os error 48)                |
        | /tmp        | /lib        | ERR: Out of memory (os error 48)                |
        | /tmp        | /lib64      | ERR: Out of memory (os error 48)                |
        | /tmp        | /opt        | ERR: Out of memory (os error 48)                |
        | /tmp        | /proc       | ERR: Out of memory (os error 48)                |
        | /tmp        | /proc/self  | ERR: Out of memory (os error 48)                |
        | /tmp        | /root       | ERR: Out of memory (os error 48)                |
        | /tmp        | /run        | ERR: Out of memory (os error 48)                |
        | /tmp        | /sbin       | ERR: Out of memory (os error 48)                |
        | /tmp        | /srv        | ERR: Out of memory (os error 48)                |
        | /tmp        | /sys        | ERR: Out of memory (os error 48)                |
        | /tmp        | /tmp        | ERR: Out of memory (os error 48)                |
        | /tmp        | /usr        | ERR: Out of memory (os error 48)                |
        | /tmp        | /var        | ERR: Out of memory (os error 48)                |
        | /tmp        | \0          | ERR: Out of memory (os error 48)                |
        | /tmp        | /x/..       | ERR: Out of memory (os error 48)                |
        | /usr        |             | ERR: Out of memory (os error 48)                |
        | /usr        | .           | ERR: Out of memory (os error 48)                |
        | /usr        | ..          | ERR: Out of memory (os error 48)                |
        | /usr        | /           | ERR: Out of memory (os error 48)                |
        | /usr        | /bin        | ERR: Out of memory (os error 48)                |
        | /usr        | /boot       | ERR: Out of memory (os error 48)                |
        | /usr        | /dev        | ERR: Out of memory (os error 48)                |
        | /usr        | /etc        | ERR: Out of memory (os error 48)                |
        | /usr        | /etc/group  | ERR: Out of memory (os error 48)                |
        | /usr        | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /usr        | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /usr        | /home       | ERR: Out of memory (os error 48)                |
        | /usr        | /lib        | ERR: Out of memory (os error 48)                |
        | /usr        | /lib64      | ERR: Out of memory (os error 48)                |
        | /usr        | /opt        | ERR: Out of memory (os error 48)                |
        | /usr        | /proc       | ERR: Out of memory (os error 48)                |
        | /usr        | /proc/self  | ERR: Out of memory (os error 48)                |
        | /usr        | /root       | ERR: Out of memory (os error 48)                |
        | /usr        | /run        | ERR: Out of memory (os error 48)                |
        | /usr        | /sbin       | ERR: Out of memory (os error 48)                |
        | /usr        | /srv        | ERR: Out of memory (os error 48)                |
        | /usr        | /sys        | ERR: Out of memory (os error 48)                |
        | /usr        | /tmp        | ERR: Out of memory (os error 48)                |
        | /usr        | /usr        | ERR: Out of memory (os error 48)                |
        | /usr        | /var        | ERR: Out of memory (os error 48)                |
        | /usr        | \0          | ERR: Out of memory (os error 48)                |
        | /usr        | /x/..       | ERR: Out of memory (os error 48)                |
        | /var        |             | ERR: Out of memory (os error 48)                |
        | /var        | .           | ERR: Out of memory (os error 48)                |
        | /var        | ..          | ERR: Out of memory (os error 48)                |
        | /var        | /           | ERR: Out of memory (os error 48)                |
        | /var        | /bin        | ERR: Out of memory (os error 48)                |
        | /var        | /boot       | ERR: Out of memory (os error 48)                |
        | /var        | /dev        | ERR: Out of memory (os error 48)                |
        | /var        | /etc        | ERR: Out of memory (os error 48)                |
        | /var        | /etc/group  | ERR: Out of memory (os error 48)                |
        | /var        | /etc/passwd | ERR: Out of memory (os error 48)                |
        | /var        | /etc/shadow | ERR: Out of memory (os error 48)                |
        | /var        | /home       | ERR: Out of memory (os error 48)                |
        | /var        | /lib        | ERR: Out of memory (os error 48)                |
        | /var        | /lib64      | ERR: Out of memory (os error 48)                |
        | /var        | /opt        | ERR: Out of memory (os error 48)                |
        | /var        | /proc       | ERR: Out of memory (os error 48)                |
        | /var        | /proc/self  | ERR: Out of memory (os error 48)                |
        | /var        | /root       | ERR: Out of memory (os error 48)                |
        | /var        | /run        | ERR: Out of memory (os error 48)                |
        | /var        | /sbin       | ERR: Out of memory (os error 48)                |
        | /var        | /srv        | ERR: Out of memory (os error 48)                |
        | /var        | /sys        | ERR: Out of memory (os error 48)                |
        | /var        | /tmp        | ERR: Out of memory (os error 48)                |
        | /var        | /usr        | ERR: Out of memory (os error 48)                |
        | /var        | /var        | ERR: Out of memory (os error 48)                |
        | /var        | \0          | ERR: Out of memory (os error 48)                |
        | /var        | /x/..       | ERR: Out of memory (os error 48)                |
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
    |             | ERR: Invalid seek (os error 70)                 |
    | .           | ERR: Invalid seek (os error 70)                 |
    | ..          | ERR: Invalid seek (os error 70)                 |
    | /           | ERR: Invalid seek (os error 70)                 |
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
    | /x/..       | ERR: Invalid seek (os error 70)                 |
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
    | /x/..       | ERR: Invalid seek (os error 70)                 |
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
