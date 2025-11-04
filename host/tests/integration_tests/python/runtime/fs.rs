use std::sync::Arc;

use arrow::{
    array::{Array, StringArray},
    datatypes::{DataType, Field},
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_udf_wasm_host::{WasmPermissions, WasmScalarUdf, vfs::VfsLimits};

use crate::integration_tests::{
    python::test_utils::{python_component, python_scalar_udf},
    test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_listdir() {
    const CODE: &str = r#"
import os

def listdir(cwd: str | None, dir: str) -> str:
    if cwd:
        os.chdir(cwd)

    return ", ".join(os.listdir(dir))
"#;

    let udf = python_scalar_udf(CODE).await.unwrap();

    struct TestCase {
        cwd: Option<&'static str>,
        dir: &'static str,
        results: &'static [&'static str],
    }
    const CASES: &[TestCase] = &[
        TestCase {
            cwd: None,
            dir: "/",
            results: &["lib"],
        },
        TestCase {
            cwd: None,
            dir: "/lib",
            results: &["python3.14"],
        },
        TestCase {
            cwd: None,
            dir: "/lib/python3.14/compression",
            results: &[
                "__init__.py",
                "_common",
                "bz2.py",
                "gzip.py",
                "lzma.py",
                "zlib.py",
                "zstd",
            ],
        },
        TestCase {
            cwd: None,
            dir: "/lib/../../lib",
            results: &["python3.14"],
        },
        TestCase {
            cwd: None,
            dir: "lib",
            results: &["python3.14"],
        },
        TestCase {
            cwd: None,
            dir: "./lib",
            results: &["python3.14"],
        },
        TestCase {
            cwd: Some("/lib"),
            dir: ".",
            results: &["python3.14"],
        },
        TestCase {
            cwd: Some("/lib"),
            dir: "..",
            results: &["lib"],
        },
    ];

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from_iter(
                    CASES.iter().map(|c| c.cwd),
                ))),
                ColumnarValue::Array(Arc::new(StringArray::from_iter(
                    CASES.iter().map(|c| Some(c.dir)),
                ))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("cwd", DataType::Utf8, true)),
                Arc::new(Field::new("dir", DataType::Utf8, true)),
            ],
            number_rows: CASES.len(),
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
        })
        .unwrap()
        .unwrap_array();

    assert_eq!(
        array.as_ref(),
        &StringArray::from_iter(CASES.iter().map(|c| Some(c.results.join(", ")))) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read() {
    const CODE: &str = r#"
def read(path: str) -> str:
    try:
        with open(path, "r") as fp:
            data = fp.read()
            return f"OK: {data}"
    except Exception as e:
        return f"ERR: {e}"
"#;

    let udf = python_scalar_udf(CODE).await.unwrap();

    struct TestCase {
        path: &'static str,
        result: Result<&'static str, &'static str>,
    }
    const CASES: &[TestCase] = &[
        TestCase {
            path: "/",
            result: Err("[Errno 31] Is a directory: '/'"),
        },
        TestCase {
            path: "/lib",
            result: Err("[Errno 31] Is a directory: '/lib'"),
        },
        TestCase {
            path: "/test",
            result: Err("[Errno 44] No such file or directory: '/test'"),
        },
        TestCase {
            path: "/lib/python3.14/__phello__/__init__.py",
            result: Ok(r#"initialized = True

def main():
    print("Hello world!")

if __name__ == '__main__':
    main()
"#),
        },
    ];

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from_iter(
                CASES.iter().map(|c| Some(c.path)),
            )))],
            arg_fields: vec![Arc::new(Field::new("path", DataType::Utf8, true))],
            number_rows: CASES.len(),
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
        })
        .unwrap()
        .unwrap_array();

    assert_eq!(
        array.as_ref(),
        &StringArray::from_iter(CASES.iter().map(|c| {
            let out = match c.result {
                Ok(s) => format!("OK: {s}"),
                Err(s) => format!("ERR: {s}"),
            };
            Some(out)
        })) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write() {
    const CODE: &str = r#"
def write(path: str) -> str:
    try:
        open(path, "w")
    except Exception as e:
        return f"ERR: {e}"

    raise Exception("unreachable")
"#;

    let udf = python_scalar_udf(CODE).await.unwrap();

    struct TestCase {
        path: &'static str,
        err: &'static str,
    }
    const CASES: &[TestCase] = &[
        TestCase {
            path: "/",
            err: "[Errno 69] Read-only file system: '/'",
        },
        TestCase {
            path: "/lib",
            err: "[Errno 69] Read-only file system: '/lib'",
        },
        TestCase {
            path: "/test",
            err: "[Errno 69] Read-only file system: '/test'",
        },
    ];

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from_iter(
                CASES.iter().map(|c| Some(c.path)),
            )))],
            arg_fields: vec![Arc::new(Field::new("path", DataType::Utf8, true))],
            number_rows: CASES.len(),
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
        })
        .unwrap()
        .unwrap_array();

    assert_eq!(
        array.as_ref(),
        &StringArray::from_iter(CASES.iter().map(|c| Some(format!("ERR: {}", c.err))))
            as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_limit_inodes() {
    let component = python_component().await;

    // since the VFS is immutable, we have to use the limit that is too small for the root FS
    let err = WasmScalarUdf::new(
        component,
        &WasmPermissions::new().with_vfs_limits(VfsLimits {
            inodes: 42,
            ..Default::default()
        }),
        "".to_owned(),
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"IO error: inodes limit reached: limit<=42 current==42 requested+=1");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_limit_bytes() {
    let component = python_component().await;

    // since the VFS is immutable, we have to use the limit that is too small for the root FS
    let err = WasmScalarUdf::new(
        component,
        &WasmPermissions::new().with_vfs_limits(VfsLimits {
            bytes: 1337,
            ..Default::default()
        }),
        "".to_owned(),
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"IO error: bytes limit reached: limit<=1337 current==1021 requested+=12355");
}
