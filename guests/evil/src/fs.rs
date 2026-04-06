//! Payload that interact with the (virtual) file system.
use std::{hash::Hash, io::Write, sync::Arc};

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

/// Write mode for our write-and-read-back UDFs. This is just a convenient way
/// to bundle the various options for opening a file for writing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct WriteMode {
    /// Whether writes append to the file.
    append: bool,
    /// Whether the open may create a missing file.
    create: bool,
    /// Whether the open must create a new file.
    create_new: bool,
    /// Whether the open truncates an existing file first.
    truncate: bool,
    /// Whether the open requests write access.
    write: bool,
}

impl WriteMode {
    /// Open an existing file and overwrite bytes in place.
    const OVERWRITE: Self = Self {
        append: false,
        create: false,
        create_new: false,
        truncate: false,
        write: true,
    };

    /// Open an existing file and truncate it before writing.
    const TRUNCATE: Self = Self {
        append: false,
        create: false,
        create_new: false,
        truncate: true,
        write: true,
    };

    /// Create the file if it does not exist before writing.
    const CREATE: Self = Self {
        append: false,
        create: true,
        create_new: false,
        truncate: false,
        write: true,
    };

    /// Require the file to not exist before writing.
    const CREATE_NEW: Self = Self {
        append: false,
        create: false,
        create_new: true,
        truncate: false,
        write: true,
    };
}

/// Return a small pre-seeded root filesystem for malicious write tests.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn root() -> Option<Vec<u8>> {
    let mut ar = tar::Builder::new(Vec::new());

    append_dir(&mut ar, "dir");
    append_dir(&mut ar, "nested");
    append_file(&mut ar, "seed.txt", b"seed data");
    append_file(&mut ar, "nested/child.txt", b"nested data");

    Some(ar.into_inner().unwrap())
}

/// Append a directory entry to the seeded TAR archive.
fn append_dir(ar: &mut tar::Builder<Vec<u8>>, path: &str) {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Directory);
    header.set_mode(0o755);
    header.set_path(path).unwrap();
    header.set_size(0);
    header.set_cksum();
    ar.append(&header, b"".as_slice()).unwrap();
}

/// Append a regular file entry to the seeded TAR archive.
fn append_file(ar: &mut tar::Builder<Vec<u8>>, path: &str, content: &[u8]) {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_mode(0o644);
    header.set_path(path).unwrap();
    header.set_size(content.len() as u64);
    header.set_cksum();
    ar.append(&header, content).unwrap();
}

/// Write content to a path using the configured open mode and then read it back.
fn write_and_read_back(path: String, content: String, mode: WriteMode) -> Result<String, String> {
    let mut file = std::fs::File::options()
        .append(mode.append)
        .create(mode.create)
        .create_new(mode.create_new)
        .read(false)
        .truncate(mode.truncate)
        .write(mode.write)
        .open(&path)
        .map_err(|e| e.to_string())?;

    file.write_all(content.as_bytes())
        .map_err(|e| e.to_string())?;
    drop(file);

    std::fs::read_to_string(path).map_err(|e| e.to_string())
}

/// Write a large payload in chunks so resource exhaustion happens inside the guest write path.
fn write_chunked(path: String, size: String) -> Result<String, String> {
    let mut file = std::fs::File::options()
        .append(false)
        .create(true)
        .create_new(false)
        .read(false)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|e| e.to_string())?;

    let mut remaining = size.parse::<usize>().map_err(|e| e.to_string())?;
    let mut written = 0usize;
    let chunk = vec![b'x'; 64 * 1024];

    while remaining > 0 {
        let n = remaining.min(chunk.len());
        if let Err(e) = file.write_all(&chunk[..n]) {
            return Err(format!("write failed after {written} bytes: {e}"));
        }
        written += n;
        remaining -= n;
    }

    Ok(written.to_string())
}

/// Returns UDFs that perform writes to the filesystem and read back the
/// results.
fn seeded_write_udfs() -> Vec<Arc<dyn ScalarUDFImpl>> {
    vec![
        Arc::new(String2Udf::new("write_read_back", |path, content| {
            write_and_read_back(path, content, WriteMode::OVERWRITE)
        })),
        Arc::new(String2Udf::new(
            "truncate_write_read_back",
            |path, content| write_and_read_back(path, content, WriteMode::TRUNCATE),
        )),
        Arc::new(String2Udf::new(
            "create_write_read_back",
            |path, content| write_and_read_back(path, content, WriteMode::CREATE),
        )),
        Arc::new(String2Udf::new(
            "create_new_write_read_back",
            |path, content| write_and_read_back(path, content, WriteMode::CREATE_NEW),
        )),
        Arc::new(String2Udf::new("write_chunked", |path, size| {
            write_chunked(path, size)
        })),
    ]
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

/// Returns UDFs that perform writes to the filesystem and read back the
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn seeded_udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(seeded_write_udfs())
}
