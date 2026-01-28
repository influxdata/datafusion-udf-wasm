//! Implements the Rust guest glue code for [DataFusion] UDFs.
//!
//!
//! [DataFusion]: https://datafusion.apache.org/
pub mod bindings;
pub mod conversion;
pub mod wrapper;

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
/// The implement two functions:
/// - **root file system:** Return the bytes of a [tar] file that contains the root filesystem for your WebAssembly
///   payload. The file-system will be provides as read-only.
/// - **scalar UDFs:** A method that takes a string -- which it may use it or not -- and returns a list of
///   [`ScalarUDFImpl`]s.
///
/// ```rust
/// # use std::sync::Arc;
/// #
/// # use datafusion_common::error::DataFusionError;
/// # use datafusion_expr::ScalarUDFImpl;
/// #
/// # use datafusion_udf_wasm_guest::export;
/// #
/// fn root() -> Option<Vec<u8>> {
///     // root file system is optional
///     None
/// }
///
/// fn udfs(source: String) -> Result<Vec<Arc<dyn ScalarUDFImpl>>, DataFusionError> {
///     // You may use the provided source code to generate UDFs on-the-fly.
///     todo!()
/// }
///
/// export! {
///     root_fs_tar: root,
///     scalar_udfs: udfs,
/// }
/// ```
///
///
/// [`ScalarUDFImpl`]: datafusion_expr::ScalarUDFImpl
/// [tar]: https://en.wikipedia.org/wiki/Tar_(computing)
#[macro_export]
macro_rules! export {
    {
        root_fs_tar: $root_fs_tar:ident,
        scalar_udfs: $scalar_udfs:ident$(,)?
    } => {

        #[derive(Debug)]
        struct Implementation;

        impl $crate::bindings::exports::datafusion_udf_wasm::udf::types::Guest for Implementation {
            type ConfigOptions = $crate::wrapper::ConfigOptionsWrapper;
            type Field = $crate::wrapper::FieldWrapper;
            type ScalarUdf = $crate::wrapper::ScalarUdfWrapper;

            fn root_fs_tar() -> Option<Vec<u8>> {
                $root_fs_tar()
            }

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
