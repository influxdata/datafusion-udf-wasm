use arrow::{
    array::{RecordBatch, StringArray},
    compute::concat,
    datatypes::{DataType, Field},
};
use datafusion_common::{ScalarValue, config::ConfigOptions, test_util::batches_to_string};
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
    insta::assert_snapshot!(
        run_1("canonicalize").await,
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
    insta::assert_snapshot!(
        run_2("copy").await,
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
    | /bin        |             | ERR: No such file or directory (os error 44)    |
    | /bin        | .           | ERR: No such file or directory (os error 44)    |
    | /bin        | ..          | ERR: No such file or directory (os error 44)    |
    | /bin        | /           | ERR: No such file or directory (os error 44)    |
    | /bin        | /bin        | ERR: No such file or directory (os error 44)    |
    | /bin        | /boot       | ERR: No such file or directory (os error 44)    |
    | /bin        | /dev        | ERR: No such file or directory (os error 44)    |
    | /bin        | /etc        | ERR: No such file or directory (os error 44)    |
    | /bin        | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /bin        | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /bin        | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /bin        | /home       | ERR: No such file or directory (os error 44)    |
    | /bin        | /lib        | ERR: No such file or directory (os error 44)    |
    | /bin        | /lib64      | ERR: No such file or directory (os error 44)    |
    | /bin        | /opt        | ERR: No such file or directory (os error 44)    |
    | /bin        | /proc       | ERR: No such file or directory (os error 44)    |
    | /bin        | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /bin        | /root       | ERR: No such file or directory (os error 44)    |
    | /bin        | /run        | ERR: No such file or directory (os error 44)    |
    | /bin        | /sbin       | ERR: No such file or directory (os error 44)    |
    | /bin        | /srv        | ERR: No such file or directory (os error 44)    |
    | /bin        | /sys        | ERR: No such file or directory (os error 44)    |
    | /bin        | /tmp        | ERR: No such file or directory (os error 44)    |
    | /bin        | /usr        | ERR: No such file or directory (os error 44)    |
    | /bin        | /var        | ERR: No such file or directory (os error 44)    |
    | /bin        | \0          | ERR: No such file or directory (os error 44)    |
    | /bin        | /x/..       | ERR: No such file or directory (os error 44)    |
    | /boot       |             | ERR: No such file or directory (os error 44)    |
    | /boot       | .           | ERR: No such file or directory (os error 44)    |
    | /boot       | ..          | ERR: No such file or directory (os error 44)    |
    | /boot       | /           | ERR: No such file or directory (os error 44)    |
    | /boot       | /bin        | ERR: No such file or directory (os error 44)    |
    | /boot       | /boot       | ERR: No such file or directory (os error 44)    |
    | /boot       | /dev        | ERR: No such file or directory (os error 44)    |
    | /boot       | /etc        | ERR: No such file or directory (os error 44)    |
    | /boot       | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /boot       | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /boot       | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /boot       | /home       | ERR: No such file or directory (os error 44)    |
    | /boot       | /lib        | ERR: No such file or directory (os error 44)    |
    | /boot       | /lib64      | ERR: No such file or directory (os error 44)    |
    | /boot       | /opt        | ERR: No such file or directory (os error 44)    |
    | /boot       | /proc       | ERR: No such file or directory (os error 44)    |
    | /boot       | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /boot       | /root       | ERR: No such file or directory (os error 44)    |
    | /boot       | /run        | ERR: No such file or directory (os error 44)    |
    | /boot       | /sbin       | ERR: No such file or directory (os error 44)    |
    | /boot       | /srv        | ERR: No such file or directory (os error 44)    |
    | /boot       | /sys        | ERR: No such file or directory (os error 44)    |
    | /boot       | /tmp        | ERR: No such file or directory (os error 44)    |
    | /boot       | /usr        | ERR: No such file or directory (os error 44)    |
    | /boot       | /var        | ERR: No such file or directory (os error 44)    |
    | /boot       | \0          | ERR: No such file or directory (os error 44)    |
    | /boot       | /x/..       | ERR: No such file or directory (os error 44)    |
    | /dev        |             | ERR: No such file or directory (os error 44)    |
    | /dev        | .           | ERR: No such file or directory (os error 44)    |
    | /dev        | ..          | ERR: No such file or directory (os error 44)    |
    | /dev        | /           | ERR: No such file or directory (os error 44)    |
    | /dev        | /bin        | ERR: No such file or directory (os error 44)    |
    | /dev        | /boot       | ERR: No such file or directory (os error 44)    |
    | /dev        | /dev        | ERR: No such file or directory (os error 44)    |
    | /dev        | /etc        | ERR: No such file or directory (os error 44)    |
    | /dev        | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /dev        | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /dev        | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /dev        | /home       | ERR: No such file or directory (os error 44)    |
    | /dev        | /lib        | ERR: No such file or directory (os error 44)    |
    | /dev        | /lib64      | ERR: No such file or directory (os error 44)    |
    | /dev        | /opt        | ERR: No such file or directory (os error 44)    |
    | /dev        | /proc       | ERR: No such file or directory (os error 44)    |
    | /dev        | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /dev        | /root       | ERR: No such file or directory (os error 44)    |
    | /dev        | /run        | ERR: No such file or directory (os error 44)    |
    | /dev        | /sbin       | ERR: No such file or directory (os error 44)    |
    | /dev        | /srv        | ERR: No such file or directory (os error 44)    |
    | /dev        | /sys        | ERR: No such file or directory (os error 44)    |
    | /dev        | /tmp        | ERR: No such file or directory (os error 44)    |
    | /dev        | /usr        | ERR: No such file or directory (os error 44)    |
    | /dev        | /var        | ERR: No such file or directory (os error 44)    |
    | /dev        | \0          | ERR: No such file or directory (os error 44)    |
    | /dev        | /x/..       | ERR: No such file or directory (os error 44)    |
    | /etc        |             | ERR: No such file or directory (os error 44)    |
    | /etc        | .           | ERR: No such file or directory (os error 44)    |
    | /etc        | ..          | ERR: No such file or directory (os error 44)    |
    | /etc        | /           | ERR: No such file or directory (os error 44)    |
    | /etc        | /bin        | ERR: No such file or directory (os error 44)    |
    | /etc        | /boot       | ERR: No such file or directory (os error 44)    |
    | /etc        | /dev        | ERR: No such file or directory (os error 44)    |
    | /etc        | /etc        | ERR: No such file or directory (os error 44)    |
    | /etc        | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /etc        | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /etc        | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /etc        | /home       | ERR: No such file or directory (os error 44)    |
    | /etc        | /lib        | ERR: No such file or directory (os error 44)    |
    | /etc        | /lib64      | ERR: No such file or directory (os error 44)    |
    | /etc        | /opt        | ERR: No such file or directory (os error 44)    |
    | /etc        | /proc       | ERR: No such file or directory (os error 44)    |
    | /etc        | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /etc        | /root       | ERR: No such file or directory (os error 44)    |
    | /etc        | /run        | ERR: No such file or directory (os error 44)    |
    | /etc        | /sbin       | ERR: No such file or directory (os error 44)    |
    | /etc        | /srv        | ERR: No such file or directory (os error 44)    |
    | /etc        | /sys        | ERR: No such file or directory (os error 44)    |
    | /etc        | /tmp        | ERR: No such file or directory (os error 44)    |
    | /etc        | /usr        | ERR: No such file or directory (os error 44)    |
    | /etc        | /var        | ERR: No such file or directory (os error 44)    |
    | /etc        | \0          | ERR: No such file or directory (os error 44)    |
    | /etc        | /x/..       | ERR: No such file or directory (os error 44)    |
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
    | /home       |             | ERR: No such file or directory (os error 44)    |
    | /home       | .           | ERR: No such file or directory (os error 44)    |
    | /home       | ..          | ERR: No such file or directory (os error 44)    |
    | /home       | /           | ERR: No such file or directory (os error 44)    |
    | /home       | /bin        | ERR: No such file or directory (os error 44)    |
    | /home       | /boot       | ERR: No such file or directory (os error 44)    |
    | /home       | /dev        | ERR: No such file or directory (os error 44)    |
    | /home       | /etc        | ERR: No such file or directory (os error 44)    |
    | /home       | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /home       | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /home       | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /home       | /home       | ERR: No such file or directory (os error 44)    |
    | /home       | /lib        | ERR: No such file or directory (os error 44)    |
    | /home       | /lib64      | ERR: No such file or directory (os error 44)    |
    | /home       | /opt        | ERR: No such file or directory (os error 44)    |
    | /home       | /proc       | ERR: No such file or directory (os error 44)    |
    | /home       | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /home       | /root       | ERR: No such file or directory (os error 44)    |
    | /home       | /run        | ERR: No such file or directory (os error 44)    |
    | /home       | /sbin       | ERR: No such file or directory (os error 44)    |
    | /home       | /srv        | ERR: No such file or directory (os error 44)    |
    | /home       | /sys        | ERR: No such file or directory (os error 44)    |
    | /home       | /tmp        | ERR: No such file or directory (os error 44)    |
    | /home       | /usr        | ERR: No such file or directory (os error 44)    |
    | /home       | /var        | ERR: No such file or directory (os error 44)    |
    | /home       | \0          | ERR: No such file or directory (os error 44)    |
    | /home       | /x/..       | ERR: No such file or directory (os error 44)    |
    | /lib        |             | ERR: No such file or directory (os error 44)    |
    | /lib        | .           | ERR: No such file or directory (os error 44)    |
    | /lib        | ..          | ERR: No such file or directory (os error 44)    |
    | /lib        | /           | ERR: No such file or directory (os error 44)    |
    | /lib        | /bin        | ERR: No such file or directory (os error 44)    |
    | /lib        | /boot       | ERR: No such file or directory (os error 44)    |
    | /lib        | /dev        | ERR: No such file or directory (os error 44)    |
    | /lib        | /etc        | ERR: No such file or directory (os error 44)    |
    | /lib        | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /lib        | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /lib        | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /lib        | /home       | ERR: No such file or directory (os error 44)    |
    | /lib        | /lib        | ERR: No such file or directory (os error 44)    |
    | /lib        | /lib64      | ERR: No such file or directory (os error 44)    |
    | /lib        | /opt        | ERR: No such file or directory (os error 44)    |
    | /lib        | /proc       | ERR: No such file or directory (os error 44)    |
    | /lib        | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /lib        | /root       | ERR: No such file or directory (os error 44)    |
    | /lib        | /run        | ERR: No such file or directory (os error 44)    |
    | /lib        | /sbin       | ERR: No such file or directory (os error 44)    |
    | /lib        | /srv        | ERR: No such file or directory (os error 44)    |
    | /lib        | /sys        | ERR: No such file or directory (os error 44)    |
    | /lib        | /tmp        | ERR: No such file or directory (os error 44)    |
    | /lib        | /usr        | ERR: No such file or directory (os error 44)    |
    | /lib        | /var        | ERR: No such file or directory (os error 44)    |
    | /lib        | \0          | ERR: No such file or directory (os error 44)    |
    | /lib        | /x/..       | ERR: No such file or directory (os error 44)    |
    | /lib64      |             | ERR: No such file or directory (os error 44)    |
    | /lib64      | .           | ERR: No such file or directory (os error 44)    |
    | /lib64      | ..          | ERR: No such file or directory (os error 44)    |
    | /lib64      | /           | ERR: No such file or directory (os error 44)    |
    | /lib64      | /bin        | ERR: No such file or directory (os error 44)    |
    | /lib64      | /boot       | ERR: No such file or directory (os error 44)    |
    | /lib64      | /dev        | ERR: No such file or directory (os error 44)    |
    | /lib64      | /etc        | ERR: No such file or directory (os error 44)    |
    | /lib64      | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /lib64      | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /lib64      | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /lib64      | /home       | ERR: No such file or directory (os error 44)    |
    | /lib64      | /lib        | ERR: No such file or directory (os error 44)    |
    | /lib64      | /lib64      | ERR: No such file or directory (os error 44)    |
    | /lib64      | /opt        | ERR: No such file or directory (os error 44)    |
    | /lib64      | /proc       | ERR: No such file or directory (os error 44)    |
    | /lib64      | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /lib64      | /root       | ERR: No such file or directory (os error 44)    |
    | /lib64      | /run        | ERR: No such file or directory (os error 44)    |
    | /lib64      | /sbin       | ERR: No such file or directory (os error 44)    |
    | /lib64      | /srv        | ERR: No such file or directory (os error 44)    |
    | /lib64      | /sys        | ERR: No such file or directory (os error 44)    |
    | /lib64      | /tmp        | ERR: No such file or directory (os error 44)    |
    | /lib64      | /usr        | ERR: No such file or directory (os error 44)    |
    | /lib64      | /var        | ERR: No such file or directory (os error 44)    |
    | /lib64      | \0          | ERR: No such file or directory (os error 44)    |
    | /lib64      | /x/..       | ERR: No such file or directory (os error 44)    |
    | /opt        |             | ERR: No such file or directory (os error 44)    |
    | /opt        | .           | ERR: No such file or directory (os error 44)    |
    | /opt        | ..          | ERR: No such file or directory (os error 44)    |
    | /opt        | /           | ERR: No such file or directory (os error 44)    |
    | /opt        | /bin        | ERR: No such file or directory (os error 44)    |
    | /opt        | /boot       | ERR: No such file or directory (os error 44)    |
    | /opt        | /dev        | ERR: No such file or directory (os error 44)    |
    | /opt        | /etc        | ERR: No such file or directory (os error 44)    |
    | /opt        | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /opt        | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /opt        | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /opt        | /home       | ERR: No such file or directory (os error 44)    |
    | /opt        | /lib        | ERR: No such file or directory (os error 44)    |
    | /opt        | /lib64      | ERR: No such file or directory (os error 44)    |
    | /opt        | /opt        | ERR: No such file or directory (os error 44)    |
    | /opt        | /proc       | ERR: No such file or directory (os error 44)    |
    | /opt        | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /opt        | /root       | ERR: No such file or directory (os error 44)    |
    | /opt        | /run        | ERR: No such file or directory (os error 44)    |
    | /opt        | /sbin       | ERR: No such file or directory (os error 44)    |
    | /opt        | /srv        | ERR: No such file or directory (os error 44)    |
    | /opt        | /sys        | ERR: No such file or directory (os error 44)    |
    | /opt        | /tmp        | ERR: No such file or directory (os error 44)    |
    | /opt        | /usr        | ERR: No such file or directory (os error 44)    |
    | /opt        | /var        | ERR: No such file or directory (os error 44)    |
    | /opt        | \0          | ERR: No such file or directory (os error 44)    |
    | /opt        | /x/..       | ERR: No such file or directory (os error 44)    |
    | /proc       |             | ERR: No such file or directory (os error 44)    |
    | /proc       | .           | ERR: No such file or directory (os error 44)    |
    | /proc       | ..          | ERR: No such file or directory (os error 44)    |
    | /proc       | /           | ERR: No such file or directory (os error 44)    |
    | /proc       | /bin        | ERR: No such file or directory (os error 44)    |
    | /proc       | /boot       | ERR: No such file or directory (os error 44)    |
    | /proc       | /dev        | ERR: No such file or directory (os error 44)    |
    | /proc       | /etc        | ERR: No such file or directory (os error 44)    |
    | /proc       | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /proc       | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /proc       | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /proc       | /home       | ERR: No such file or directory (os error 44)    |
    | /proc       | /lib        | ERR: No such file or directory (os error 44)    |
    | /proc       | /lib64      | ERR: No such file or directory (os error 44)    |
    | /proc       | /opt        | ERR: No such file or directory (os error 44)    |
    | /proc       | /proc       | ERR: No such file or directory (os error 44)    |
    | /proc       | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /proc       | /root       | ERR: No such file or directory (os error 44)    |
    | /proc       | /run        | ERR: No such file or directory (os error 44)    |
    | /proc       | /sbin       | ERR: No such file or directory (os error 44)    |
    | /proc       | /srv        | ERR: No such file or directory (os error 44)    |
    | /proc       | /sys        | ERR: No such file or directory (os error 44)    |
    | /proc       | /tmp        | ERR: No such file or directory (os error 44)    |
    | /proc       | /usr        | ERR: No such file or directory (os error 44)    |
    | /proc       | /var        | ERR: No such file or directory (os error 44)    |
    | /proc       | \0          | ERR: No such file or directory (os error 44)    |
    | /proc       | /x/..       | ERR: No such file or directory (os error 44)    |
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
    | /root       |             | ERR: No such file or directory (os error 44)    |
    | /root       | .           | ERR: No such file or directory (os error 44)    |
    | /root       | ..          | ERR: No such file or directory (os error 44)    |
    | /root       | /           | ERR: No such file or directory (os error 44)    |
    | /root       | /bin        | ERR: No such file or directory (os error 44)    |
    | /root       | /boot       | ERR: No such file or directory (os error 44)    |
    | /root       | /dev        | ERR: No such file or directory (os error 44)    |
    | /root       | /etc        | ERR: No such file or directory (os error 44)    |
    | /root       | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /root       | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /root       | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /root       | /home       | ERR: No such file or directory (os error 44)    |
    | /root       | /lib        | ERR: No such file or directory (os error 44)    |
    | /root       | /lib64      | ERR: No such file or directory (os error 44)    |
    | /root       | /opt        | ERR: No such file or directory (os error 44)    |
    | /root       | /proc       | ERR: No such file or directory (os error 44)    |
    | /root       | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /root       | /root       | ERR: No such file or directory (os error 44)    |
    | /root       | /run        | ERR: No such file or directory (os error 44)    |
    | /root       | /sbin       | ERR: No such file or directory (os error 44)    |
    | /root       | /srv        | ERR: No such file or directory (os error 44)    |
    | /root       | /sys        | ERR: No such file or directory (os error 44)    |
    | /root       | /tmp        | ERR: No such file or directory (os error 44)    |
    | /root       | /usr        | ERR: No such file or directory (os error 44)    |
    | /root       | /var        | ERR: No such file or directory (os error 44)    |
    | /root       | \0          | ERR: No such file or directory (os error 44)    |
    | /root       | /x/..       | ERR: No such file or directory (os error 44)    |
    | /run        |             | ERR: No such file or directory (os error 44)    |
    | /run        | .           | ERR: No such file or directory (os error 44)    |
    | /run        | ..          | ERR: No such file or directory (os error 44)    |
    | /run        | /           | ERR: No such file or directory (os error 44)    |
    | /run        | /bin        | ERR: No such file or directory (os error 44)    |
    | /run        | /boot       | ERR: No such file or directory (os error 44)    |
    | /run        | /dev        | ERR: No such file or directory (os error 44)    |
    | /run        | /etc        | ERR: No such file or directory (os error 44)    |
    | /run        | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /run        | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /run        | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /run        | /home       | ERR: No such file or directory (os error 44)    |
    | /run        | /lib        | ERR: No such file or directory (os error 44)    |
    | /run        | /lib64      | ERR: No such file or directory (os error 44)    |
    | /run        | /opt        | ERR: No such file or directory (os error 44)    |
    | /run        | /proc       | ERR: No such file or directory (os error 44)    |
    | /run        | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /run        | /root       | ERR: No such file or directory (os error 44)    |
    | /run        | /run        | ERR: No such file or directory (os error 44)    |
    | /run        | /sbin       | ERR: No such file or directory (os error 44)    |
    | /run        | /srv        | ERR: No such file or directory (os error 44)    |
    | /run        | /sys        | ERR: No such file or directory (os error 44)    |
    | /run        | /tmp        | ERR: No such file or directory (os error 44)    |
    | /run        | /usr        | ERR: No such file or directory (os error 44)    |
    | /run        | /var        | ERR: No such file or directory (os error 44)    |
    | /run        | \0          | ERR: No such file or directory (os error 44)    |
    | /run        | /x/..       | ERR: No such file or directory (os error 44)    |
    | /sbin       |             | ERR: No such file or directory (os error 44)    |
    | /sbin       | .           | ERR: No such file or directory (os error 44)    |
    | /sbin       | ..          | ERR: No such file or directory (os error 44)    |
    | /sbin       | /           | ERR: No such file or directory (os error 44)    |
    | /sbin       | /bin        | ERR: No such file or directory (os error 44)    |
    | /sbin       | /boot       | ERR: No such file or directory (os error 44)    |
    | /sbin       | /dev        | ERR: No such file or directory (os error 44)    |
    | /sbin       | /etc        | ERR: No such file or directory (os error 44)    |
    | /sbin       | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /sbin       | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /sbin       | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /sbin       | /home       | ERR: No such file or directory (os error 44)    |
    | /sbin       | /lib        | ERR: No such file or directory (os error 44)    |
    | /sbin       | /lib64      | ERR: No such file or directory (os error 44)    |
    | /sbin       | /opt        | ERR: No such file or directory (os error 44)    |
    | /sbin       | /proc       | ERR: No such file or directory (os error 44)    |
    | /sbin       | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /sbin       | /root       | ERR: No such file or directory (os error 44)    |
    | /sbin       | /run        | ERR: No such file or directory (os error 44)    |
    | /sbin       | /sbin       | ERR: No such file or directory (os error 44)    |
    | /sbin       | /srv        | ERR: No such file or directory (os error 44)    |
    | /sbin       | /sys        | ERR: No such file or directory (os error 44)    |
    | /sbin       | /tmp        | ERR: No such file or directory (os error 44)    |
    | /sbin       | /usr        | ERR: No such file or directory (os error 44)    |
    | /sbin       | /var        | ERR: No such file or directory (os error 44)    |
    | /sbin       | \0          | ERR: No such file or directory (os error 44)    |
    | /sbin       | /x/..       | ERR: No such file or directory (os error 44)    |
    | /srv        |             | ERR: No such file or directory (os error 44)    |
    | /srv        | .           | ERR: No such file or directory (os error 44)    |
    | /srv        | ..          | ERR: No such file or directory (os error 44)    |
    | /srv        | /           | ERR: No such file or directory (os error 44)    |
    | /srv        | /bin        | ERR: No such file or directory (os error 44)    |
    | /srv        | /boot       | ERR: No such file or directory (os error 44)    |
    | /srv        | /dev        | ERR: No such file or directory (os error 44)    |
    | /srv        | /etc        | ERR: No such file or directory (os error 44)    |
    | /srv        | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /srv        | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /srv        | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /srv        | /home       | ERR: No such file or directory (os error 44)    |
    | /srv        | /lib        | ERR: No such file or directory (os error 44)    |
    | /srv        | /lib64      | ERR: No such file or directory (os error 44)    |
    | /srv        | /opt        | ERR: No such file or directory (os error 44)    |
    | /srv        | /proc       | ERR: No such file or directory (os error 44)    |
    | /srv        | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /srv        | /root       | ERR: No such file or directory (os error 44)    |
    | /srv        | /run        | ERR: No such file or directory (os error 44)    |
    | /srv        | /sbin       | ERR: No such file or directory (os error 44)    |
    | /srv        | /srv        | ERR: No such file or directory (os error 44)    |
    | /srv        | /sys        | ERR: No such file or directory (os error 44)    |
    | /srv        | /tmp        | ERR: No such file or directory (os error 44)    |
    | /srv        | /usr        | ERR: No such file or directory (os error 44)    |
    | /srv        | /var        | ERR: No such file or directory (os error 44)    |
    | /srv        | \0          | ERR: No such file or directory (os error 44)    |
    | /srv        | /x/..       | ERR: No such file or directory (os error 44)    |
    | /sys        |             | ERR: No such file or directory (os error 44)    |
    | /sys        | .           | ERR: No such file or directory (os error 44)    |
    | /sys        | ..          | ERR: No such file or directory (os error 44)    |
    | /sys        | /           | ERR: No such file or directory (os error 44)    |
    | /sys        | /bin        | ERR: No such file or directory (os error 44)    |
    | /sys        | /boot       | ERR: No such file or directory (os error 44)    |
    | /sys        | /dev        | ERR: No such file or directory (os error 44)    |
    | /sys        | /etc        | ERR: No such file or directory (os error 44)    |
    | /sys        | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /sys        | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /sys        | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /sys        | /home       | ERR: No such file or directory (os error 44)    |
    | /sys        | /lib        | ERR: No such file or directory (os error 44)    |
    | /sys        | /lib64      | ERR: No such file or directory (os error 44)    |
    | /sys        | /opt        | ERR: No such file or directory (os error 44)    |
    | /sys        | /proc       | ERR: No such file or directory (os error 44)    |
    | /sys        | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /sys        | /root       | ERR: No such file or directory (os error 44)    |
    | /sys        | /run        | ERR: No such file or directory (os error 44)    |
    | /sys        | /sbin       | ERR: No such file or directory (os error 44)    |
    | /sys        | /srv        | ERR: No such file or directory (os error 44)    |
    | /sys        | /sys        | ERR: No such file or directory (os error 44)    |
    | /sys        | /tmp        | ERR: No such file or directory (os error 44)    |
    | /sys        | /usr        | ERR: No such file or directory (os error 44)    |
    | /sys        | /var        | ERR: No such file or directory (os error 44)    |
    | /sys        | \0          | ERR: No such file or directory (os error 44)    |
    | /sys        | /x/..       | ERR: No such file or directory (os error 44)    |
    | /tmp        |             | ERR: No such file or directory (os error 44)    |
    | /tmp        | .           | ERR: No such file or directory (os error 44)    |
    | /tmp        | ..          | ERR: No such file or directory (os error 44)    |
    | /tmp        | /           | ERR: No such file or directory (os error 44)    |
    | /tmp        | /bin        | ERR: No such file or directory (os error 44)    |
    | /tmp        | /boot       | ERR: No such file or directory (os error 44)    |
    | /tmp        | /dev        | ERR: No such file or directory (os error 44)    |
    | /tmp        | /etc        | ERR: No such file or directory (os error 44)    |
    | /tmp        | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /tmp        | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /tmp        | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /tmp        | /home       | ERR: No such file or directory (os error 44)    |
    | /tmp        | /lib        | ERR: No such file or directory (os error 44)    |
    | /tmp        | /lib64      | ERR: No such file or directory (os error 44)    |
    | /tmp        | /opt        | ERR: No such file or directory (os error 44)    |
    | /tmp        | /proc       | ERR: No such file or directory (os error 44)    |
    | /tmp        | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /tmp        | /root       | ERR: No such file or directory (os error 44)    |
    | /tmp        | /run        | ERR: No such file or directory (os error 44)    |
    | /tmp        | /sbin       | ERR: No such file or directory (os error 44)    |
    | /tmp        | /srv        | ERR: No such file or directory (os error 44)    |
    | /tmp        | /sys        | ERR: No such file or directory (os error 44)    |
    | /tmp        | /tmp        | ERR: No such file or directory (os error 44)    |
    | /tmp        | /usr        | ERR: No such file or directory (os error 44)    |
    | /tmp        | /var        | ERR: No such file or directory (os error 44)    |
    | /tmp        | \0          | ERR: No such file or directory (os error 44)    |
    | /tmp        | /x/..       | ERR: No such file or directory (os error 44)    |
    | /usr        |             | ERR: No such file or directory (os error 44)    |
    | /usr        | .           | ERR: No such file or directory (os error 44)    |
    | /usr        | ..          | ERR: No such file or directory (os error 44)    |
    | /usr        | /           | ERR: No such file or directory (os error 44)    |
    | /usr        | /bin        | ERR: No such file or directory (os error 44)    |
    | /usr        | /boot       | ERR: No such file or directory (os error 44)    |
    | /usr        | /dev        | ERR: No such file or directory (os error 44)    |
    | /usr        | /etc        | ERR: No such file or directory (os error 44)    |
    | /usr        | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /usr        | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /usr        | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /usr        | /home       | ERR: No such file or directory (os error 44)    |
    | /usr        | /lib        | ERR: No such file or directory (os error 44)    |
    | /usr        | /lib64      | ERR: No such file or directory (os error 44)    |
    | /usr        | /opt        | ERR: No such file or directory (os error 44)    |
    | /usr        | /proc       | ERR: No such file or directory (os error 44)    |
    | /usr        | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /usr        | /root       | ERR: No such file or directory (os error 44)    |
    | /usr        | /run        | ERR: No such file or directory (os error 44)    |
    | /usr        | /sbin       | ERR: No such file or directory (os error 44)    |
    | /usr        | /srv        | ERR: No such file or directory (os error 44)    |
    | /usr        | /sys        | ERR: No such file or directory (os error 44)    |
    | /usr        | /tmp        | ERR: No such file or directory (os error 44)    |
    | /usr        | /usr        | ERR: No such file or directory (os error 44)    |
    | /usr        | /var        | ERR: No such file or directory (os error 44)    |
    | /usr        | \0          | ERR: No such file or directory (os error 44)    |
    | /usr        | /x/..       | ERR: No such file or directory (os error 44)    |
    | /var        |             | ERR: No such file or directory (os error 44)    |
    | /var        | .           | ERR: No such file or directory (os error 44)    |
    | /var        | ..          | ERR: No such file or directory (os error 44)    |
    | /var        | /           | ERR: No such file or directory (os error 44)    |
    | /var        | /bin        | ERR: No such file or directory (os error 44)    |
    | /var        | /boot       | ERR: No such file or directory (os error 44)    |
    | /var        | /dev        | ERR: No such file or directory (os error 44)    |
    | /var        | /etc        | ERR: No such file or directory (os error 44)    |
    | /var        | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /var        | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /var        | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /var        | /home       | ERR: No such file or directory (os error 44)    |
    | /var        | /lib        | ERR: No such file or directory (os error 44)    |
    | /var        | /lib64      | ERR: No such file or directory (os error 44)    |
    | /var        | /opt        | ERR: No such file or directory (os error 44)    |
    | /var        | /proc       | ERR: No such file or directory (os error 44)    |
    | /var        | /proc/self  | ERR: No such file or directory (os error 44)    |
    | /var        | /root       | ERR: No such file or directory (os error 44)    |
    | /var        | /run        | ERR: No such file or directory (os error 44)    |
    | /var        | /sbin       | ERR: No such file or directory (os error 44)    |
    | /var        | /srv        | ERR: No such file or directory (os error 44)    |
    | /var        | /sys        | ERR: No such file or directory (os error 44)    |
    | /var        | /tmp        | ERR: No such file or directory (os error 44)    |
    | /var        | /usr        | ERR: No such file or directory (os error 44)    |
    | /var        | /var        | ERR: No such file or directory (os error 44)    |
    | /var        | \0          | ERR: No such file or directory (os error 44)    |
    | /var        | /x/..       | ERR: No such file or directory (os error 44)    |
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
    insta::assert_snapshot!(
        run_1("create_dir").await,
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
    | /etc/group  | ERR: No such file or directory (os error 44)    |
    | /etc/passwd | ERR: No such file or directory (os error 44)    |
    | /etc/shadow | ERR: No such file or directory (os error 44)    |
    | /home       | OK: created                                     |
    | /lib        | OK: created                                     |
    | /lib64      | OK: created                                     |
    | /opt        | OK: created                                     |
    | /proc       | OK: created                                     |
    | /proc/self  | ERR: No such file or directory (os error 44)    |
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
    insta::assert_snapshot!(
        run_1("exists").await,
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
    insta::assert_snapshot!(
        run_2("hard_link").await,
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
    insta::assert_snapshot!(
        run_1("metadata").await,
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
    insta::assert_snapshot!(
        run_1("open_append").await,
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
async fn test_open_create() {
    insta::assert_snapshot!(
        run_1("open_create").await,
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
async fn test_open_create_new() {
    insta::assert_snapshot!(
        run_1("open_create_new").await,
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
async fn test_open_read() {
    insta::assert_snapshot!(
        run_1("open_read").await,
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
    insta::assert_snapshot!(
        run_1("open_truncate").await,
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
async fn test_open_write() {
    insta::assert_snapshot!(
        run_1("open_write").await,
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
async fn test_read_dir() {
    insta::assert_snapshot!(
        run_1("read_dir").await,
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
    insta::assert_snapshot!(
        run_1("read_link").await,
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
    insta::assert_snapshot!(
        run_1("remove_dir").await,
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
    insta::assert_snapshot!(
        run_1("remove_file").await,
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
    insta::assert_snapshot!(
        run_2("rename").await,
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
    insta::assert_snapshot!(
        run_1("set_permissions").await,
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
    insta::assert_snapshot!(
        run_1("symlink_metadata").await,
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
async fn run_1(name: &'static str) -> String {
    let mut input_nice = Vec::with_capacity(PATHS.len());
    let mut results = Vec::with_capacity(PATHS.len());
    for input in PATHS {
        // tests are stateful, hence get a fresh UDF for every input
        let udf = udf(name).await;

        let result = udf
            .invoke_async_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    (*input).to_owned(),
                )))],
                arg_fields: vec![Arc::new(Field::new("a", DataType::Utf8, true))],
                number_rows: 1,
                return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .await
            .unwrap()
            .unwrap_array();

        results.push(result);
        input_nice.push(nice_path(input));
    }

    let results_ref = results.iter().map(|a| a.as_ref()).collect::<Vec<_>>();

    batches_to_string(&[RecordBatch::try_from_iter([
        (
            "path",
            Arc::new(StringArray::from_iter_values(input_nice)) as _,
        ),
        ("result", concat(&results_ref).unwrap()),
    ])
    .unwrap()])
}

/// Run UDF that expects two string inputs.
async fn run_2(name: &'static str) -> String {
    let (paths_from, paths_to) = cross(PATHS);
    let mut input_from_nice = Vec::with_capacity(paths_from.len());
    let mut input_to_nice = Vec::with_capacity(paths_to.len());
    let mut results = Vec::with_capacity(paths_from.len());
    for (input_from, input_to) in paths_from.into_iter().zip(paths_to) {
        // tests are stateful, hence get a fresh UDF for every input
        let udf = udf(name).await;

        let result = udf
            .invoke_async_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(input_from.clone()))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(input_to.clone()))),
                ],
                arg_fields: vec![
                    Arc::new(Field::new("from", DataType::Utf8, true)),
                    Arc::new(Field::new("to", DataType::Utf8, true)),
                ],
                number_rows: 1,
                return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .await
            .unwrap()
            .unwrap_array();

        results.push(result);
        input_from_nice.push(nice_path(&input_from));
        input_to_nice.push(nice_path(&input_to));
    }

    let results_ref = results.iter().map(|a| a.as_ref()).collect::<Vec<_>>();

    batches_to_string(&[RecordBatch::try_from_iter([
        (
            "from",
            Arc::new(StringArray::from_iter_values(input_from_nice)) as _,
        ),
        (
            "to",
            Arc::new(StringArray::from_iter_values(input_to_nice)) as _,
        ),
        ("output", concat(&results_ref).unwrap()),
    ])
    .unwrap()])
}
