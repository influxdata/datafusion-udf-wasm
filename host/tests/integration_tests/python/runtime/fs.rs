use std::sync::Arc;

use arrow::{
    array::{Array, StringArray},
    datatypes::{DataType, Field},
};
use datafusion_common::config::ConfigOptions;
use datafusion_execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, async_udf::AsyncScalarUDFImpl};
use datafusion_udf_wasm_host::{VfsLimits, WasmPermissions, WasmScalarUdf};
use regex::Regex;
use tokio::runtime::Handle;

use crate::integration_tests::{
    python::test_utils::{python_component, python_scalar_udf},
    test_utils::ColumnarValueExt,
};

#[tokio::test]
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
        .invoke_async_with_args(ScalarFunctionArgs {
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
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();

    assert_eq!(
        array.as_ref(),
        &StringArray::from_iter(CASES.iter().map(|c| Some(c.results.join(", ")))) as &dyn Array,
    );
}

#[tokio::test]
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
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from_iter(
                CASES.iter().map(|c| Some(c.path)),
            )))],
            arg_fields: vec![Arc::new(Field::new("path", DataType::Utf8, true))],
            number_rows: CASES.len(),
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
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

#[tokio::test]
async fn test_write() {
    const CODE: &str = r#"
def write(path: str) -> str:
    try:
        with open(path, "w") as fp:
            fp.write("data")
            data = fp.read()
            return "OK: {data}"
    except Exception as e:
        return f"ERR: {e}"
"#;

    let udf = python_scalar_udf(CODE).await.unwrap();

    struct TestCase {
        path: &'static str,
        err: &'static str,
    }
    const CASES: &[TestCase] = &[
        TestCase {
            path: "/",
            err: "ERR: [Errno 31] Is a directory: '/'",
        },
        TestCase {
            path: "/lib",
            err: "ERR: [Errno 31] Is a directory: '/lib'",
        },
        TestCase {
            path: "/test",
            err: "ERR: [Errno 69] Read-only file system",
        },
    ];

    let array = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from_iter(
                CASES.iter().map(|c| Some(c.path)),
            )))],
            arg_fields: vec![Arc::new(Field::new("path", DataType::Utf8, true))],
            number_rows: CASES.len(),
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();

    assert_eq!(
        array.as_ref(),
        &StringArray::from_iter(CASES.iter().map(|c| Some(c.err))) as &dyn Array,
    );
}

#[tokio::test]
async fn test_limit_inodes() {
    let component = python_component().await;

    // since the VFS is immutable, we have to use the limit that is too small for the root FS
    let err = WasmScalarUdf::new(
        component,
        &WasmPermissions::new().with_vfs_limits(VfsLimits {
            inodes: 42,
            ..Default::default()
        }),
        Handle::current(),
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
    IO error: inodes limit reached: limit<=42 current==42 requested+=1
    ");
}

#[tokio::test]
async fn test_limit_bytes() {
    let component = python_component().await;

    // Since the VFS is immutable, we have to use the limit that is too small for the root FS. At the same time, the
    // memory pool must be large enough to hold the component code itself. So the memory limit should be
    // `code_size < limit < code_size + FS`. Instead of hard-coding this balance here, we try to automatically find it.
    const START: usize = 100_000_000; // 100MB
    const STEP: usize = 10_000_000; // 10MB
    let try_create = async |limit| {
        WasmScalarUdf::new(
            component,
            &WasmPermissions::default(),
            Handle::current(),
            &(Arc::new(GreedyMemoryPool::new(limit)) as _),
            "".to_owned(),
        )
        .await
    };
    let mut current = START;
    try_create(current)
        .await
        .expect("START limit should be large enough to succeed");
    let err = loop {
        current = current.checked_sub(STEP).unwrap();
        match try_create(current).await {
            Ok(_) => {}
            Err(e) => {
                break e;
            }
        }
    };

    // normalize sizes
    let err = err.to_string();
    let err = Regex::new(r#"[0-9.]+ [KM]B"#)
        .unwrap()
        .replace_all(&err, "<SIZE>");

    insta::assert_snapshot!(
        err,
        @r"
    populate root FS from TAR
    caused by
    IO error: Resources exhausted: Failed to allocate additional <SIZE> for WASM UDF resources with <SIZE> already allocated for this reservation - <SIZE> remain available for the total pool
    ");
}
