//! Implements the Rust guest glue code for [DataFusion] UDFs.
//!
//!
//! [DataFusion]: https://datafusion.apache.org/
pub mod bindings;
pub mod conversion;
pub mod wrapper;

use std::{
    fs::{self, File},
    io::{Cursor, ErrorKind},
    path::{Component, Path, PathBuf},
    sync::OnceLock,
};

use datafusion_common::{DataFusionError, Result as DataFusionResult};

/// Export UDFs to WebAssembly.
///
/// # Example
/// Make sure your crate has the correct type:
///
/// ```toml
/// [lib]
/// crate-type = ["cdylib"]
/// ```
///
/// Then implement one function:
/// - **scalar UDFs:** A method that takes a string -- which it may use it or not -- and returns a list of
///   [`ScalarUDFImpl`]s. If the guest needs additional files, it can use the provided WASI
///   filesystem directly; Rust dependencies themselves should continue to be declared in the
///   guest crate's `Cargo.toml`.
///
/// ```rust
/// # use std::sync::Arc;
/// #
/// # use datafusion_common::error::DataFusionError;
/// # use datafusion_expr::ScalarUDFImpl;
/// #
/// # use datafusion_udf_wasm_guest::export;
/// #
/// fn udfs(source: String) -> Result<Vec<Arc<dyn ScalarUDFImpl>>, DataFusionError> {
///     // You may use the provided source code to generate UDFs on-the-fly.
///     todo!()
/// }
///
/// export! {
///     scalar_udfs: udfs,
/// }
/// ```
///
///
/// [`ScalarUDFImpl`]: datafusion_expr::ScalarUDFImpl
#[macro_export]
macro_rules! export {
    {
        scalar_udfs: $scalar_udfs:ident$(,)?
    } => {

        #[derive(Debug)]
        struct Implementation;

        impl $crate::bindings::exports::datafusion_udf_wasm::udf::types::Guest for Implementation {
            type ConfigOptions = $crate::wrapper::ConfigOptionsWrapper;
            type Field = $crate::wrapper::FieldWrapper;
            type ScalarUdf = $crate::wrapper::ScalarUdfWrapper;

            fn scalar_udfs(
                source: String,
            ) -> Result<
                Vec<$crate::bindings::exports::datafusion_udf_wasm::udf::types::ScalarUdf>,
                $crate::bindings::exports::datafusion_udf_wasm::udf::types::DataFusionError,
            > {
                let udfs = $scalar_udfs(source)?;

                Ok(
                    udfs.into_iter()
                    .map(|udf| $crate::bindings::exports::datafusion_udf_wasm::udf::types::ScalarUdf::new(
                        $crate::wrapper::ScalarUdfWrapper::new(udf)
                    ))
                    .collect()
                )
            }
        }

        // only export on WASI, because otherwise the linker is going to be sad
        #[cfg(target_os = "wasi")]
        $crate::bindings::_export!(Implementation with_types_in $crate::bindings);

        // create dummy function for other targets to suppress "unused" warnings
        #[cfg(not(target_os = "wasi"))]
        #[doc(hidden)]
        pub fn _export_stuff() -> impl $crate::bindings::exports::datafusion_udf_wasm::udf::types::Guest {
            Implementation
        }
    };
}

/// Extract a tar archive into the guest's WASI filesystem root.
pub fn extract_tar_to_root(tar_data: &[u8]) -> DataFusionResult<()> {
    let mut archive = tar::Archive::new(Cursor::new(tar_data));
    let entries = archive.entries().map_err(DataFusionError::IoError)?;

    for entry in entries {
        let mut entry = entry.map_err(DataFusionError::IoError)?;
        let path = entry.path().map_err(DataFusionError::IoError)?;
        let path = sanitize_tar_path(path.as_ref())?;
        let entry_type = entry.header().entry_type();
        let target = Path::new("/").join(&path);

        if entry_type.is_dir() {
            fs::create_dir_all(&target).map_err(DataFusionError::IoError)?;
        } else if entry_type.is_file() || entry_type.is_contiguous() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(DataFusionError::IoError)?;
            }

            let mut file = File::create(&target).map_err(DataFusionError::IoError)?;
            std::io::copy(&mut entry, &mut file).map_err(DataFusionError::IoError)?;
        } else {
            return Err(DataFusionError::IoError(std::io::Error::new(
                ErrorKind::Unsupported,
                format!(
                    "Unsupported TAR content: {entry_type:?} @ {}",
                    path.display()
                ),
            )));
        }
    }

    Ok(())
}

/// Sanitize a TAR path to prevent directory traversal and other unsafe constructs.
fn sanitize_tar_path(path: &Path) -> DataFusionResult<PathBuf> {
    let mut sanitized = PathBuf::new();

    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => sanitized.push(part),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(DataFusionError::IoError(std::io::Error::new(
                    ErrorKind::InvalidData,
                    format!("Unsupported TAR path: {}", path.display()),
                )));
            }
        }
    }

    Ok(sanitized)
}

/// Run guest initialization once per component instance.
pub fn init_once<F>(state: &'static OnceLock<Result<(), String>>, init: F) -> DataFusionResult<()>
where
    F: FnOnce() -> DataFusionResult<()>,
{
    state
        .get_or_init(|| init().map_err(|e| e.to_string()))
        .clone()
        .map_err(DataFusionError::Execution)
}
