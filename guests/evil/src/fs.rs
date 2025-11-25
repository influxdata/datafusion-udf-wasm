//! Payload that interact with the (virtual) file system.
use std::{hash::Hash, sync::Arc};

use arrow::{array::StringArray, datatypes::DataType};
use datafusion_common::{Result as DataFusionResult, cast::as_string_array};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::common::{DynBox, String1Udf};

/// UDF that produces a string from two inputs.
#[derive(Debug, PartialEq, Eq, Hash)]
struct String2Udf {
    /// Name.
    name: &'static str,

    /// String producer.
    effect: DynBox<dyn Fn(String, String) -> Result<String, String> + Send + Sync>,

    /// Signature of the UDF.
    ///
    /// We store this here because [`ScalarUDFImpl::signature`] requires us to return a reference.
    signature: Signature,
}

impl String2Udf {
    /// Create new UDF.
    fn new<F>(name: &'static str, effect: F) -> Self
    where
        F: Fn(String, String) -> Result<String, String> + Send + Sync + 'static,
    {
        Self {
            name,
            effect: DynBox(Box::new(effect)),
            signature: Signature::uniform(2, vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for String2Udf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let mut it = args.args.into_iter();
        let array_1 = it.next().unwrap().into_array(args.number_rows).unwrap();
        let array_2 = it.next().unwrap().into_array(args.number_rows).unwrap();
        let array_1 = as_string_array(&array_1).unwrap();
        let array_2 = as_string_array(&array_2).unwrap();
        let array = array_1
            .iter()
            .zip(array_2)
            .map(|(a, b)| {
                if let (Some(a), Some(b)) = (a, b) {
                    Some(match (self.effect)(a.to_owned(), b.to_owned()) {
                        Ok(s) => format!("OK: {s}"),
                        Err(s) => format!("ERR: {s}"),
                    })
                } else {
                    None
                }
            })
            .collect::<StringArray>();
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

/// Returns our evil UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![
        Arc::new(String1Udf::new("canonicalize", |path| {
            std::fs::canonicalize(path)
                .map(|p| p.display().to_string())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String2Udf::new("copy", |from, to| {
            std::fs::copy(from, to)
                .map(|n_bytes| n_bytes.to_string())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("create_dir", |path| {
            std::fs::create_dir(path)
                .map(|()| "created".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("exists", |path| {
            std::fs::exists(path)
                .map(|b| b.to_string())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String2Udf::new("hard_link", |from, to| {
            std::fs::hard_link(from, to)
                .map(|_| "linked".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("metadata", |path| {
            std::fs::metadata(path)
                .map(|_| "got data".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("open_append", |path| {
            std::fs::File::options()
                .append(true)
                .create(false)
                .create_new(false)
                .read(false)
                .truncate(false)
                .write(false)
                .open(path)
                .map(|_| "opened".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("open_create", |path| {
            std::fs::File::options()
                .append(false)
                .create(true)
                .create_new(false)
                .read(false)
                .truncate(false)
                .write(true)
                .open(path)
                .map(|_| "opened".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("open_create_new", |path| {
            std::fs::File::options()
                .append(false)
                .create(false)
                .create_new(true)
                .read(false)
                .truncate(false)
                .write(true)
                .open(path)
                .map(|_| "opened".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("open_read", |path| {
            std::fs::File::options()
                .append(false)
                .create(false)
                .create_new(false)
                .read(true)
                .truncate(false)
                .write(false)
                .open(path)
                .map(|_| "opened".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("open_truncate", |path| {
            std::fs::File::options()
                .append(false)
                .create(false)
                .create_new(false)
                .read(false)
                .truncate(true)
                .write(true)
                .open(path)
                .map(|_| "opened".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("open_write", |path| {
            std::fs::File::options()
                .append(false)
                .create(false)
                .create_new(false)
                .read(false)
                .truncate(false)
                .write(true)
                .open(path)
                .map(|_| "opened".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("read_dir", |path| {
            std::fs::read_dir(path)
                .map(|it| {
                    let entries = it
                        .map(|res| match res {
                            Ok(entry) => format!("OK: {}", entry.path().display()),
                            Err(e) => format!("ERR: {e}"),
                        })
                        .collect::<Vec<_>>();
                    if entries.is_empty() {
                        "<EMPTY>".to_owned()
                    } else {
                        entries.join(",")
                    }
                })
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("read_link", |path| {
            std::fs::read_link(path)
                .map(|p| p.display().to_string())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("remove_dir", |path| {
            std::fs::remove_dir(path)
                .map(|()| "removed".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("remove_file", |path| {
            std::fs::remove_file(path)
                .map(|()| "removed".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String2Udf::new("rename", |from, to| {
            std::fs::rename(from, to)
                .map(|()| "renamed".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("set_permissions", |path| {
            // We need some pre-existing file to get the opaque permissions object first. The root of the filesystem
            // should always be available.
            let perm = std::fs::metadata("/").unwrap().permissions();
            std::fs::set_permissions(path, perm)
                .map(|()| "set".to_owned())
                .map_err(|e| e.to_string())
        })),
        Arc::new(String1Udf::new("symlink_metadata", |path| {
            std::fs::symlink_metadata(path)
                .map(|_| "got data".to_owned())
                .map_err(|e| e.to_string())
        })),
    ])
}
