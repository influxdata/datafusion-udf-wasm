pub mod bindings;
pub mod conversion;
pub mod wrapper;

#[macro_export]
macro_rules! export {
    {
        root_fs_tar: $root_fs_tar:ident,
        scalar_udfs: $scalar_udfs:ident,
    } => {

        #[derive(Debug)]
        struct Implementation;

        impl $crate::bindings::exports::datafusion_udf_wasm::udf::types::Guest for Implementation {
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
        pub fn _export_stuff() -> impl $crate::bindings::exports::datafusion_udf_wasm::udf::types::Guest {
            Implementation
        }
    };
}
