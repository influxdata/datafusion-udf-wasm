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

            fn scalar_udfs() -> Vec<$crate::bindings::exports::datafusion_udf_wasm::udf::types::ScalarUdf> {
                $scalar_udfs()
                    .into_iter()
                    .map(|udf| $crate::bindings::exports::datafusion_udf_wasm::udf::types::ScalarUdf::new(
                        $crate::wrapper::ScalarUdfWrapper::new(udf)
                    ))
                    .collect()
            }
        }

        $crate::bindings::_export!(Implementation with_types_in $crate::bindings);
    };
}
