pub mod bindings;
pub mod conversion;
pub mod wrapper;

#[macro_export]
macro_rules! export {
    ($fn:ident) => {

        #[derive(Debug)]
        struct Implementation;

        impl $crate::bindings::exports::datafusion_udf_wasm::udf::types::Guest for Implementation {
            type ScalarUdf = $crate::wrapper::ScalarUdfWrapper;

            fn scalar_udfs() -> Vec<$crate::bindings::exports::datafusion_udf_wasm::udf::types::ScalarUdf> {
                $fn()
                    .into_iter()
                    .map(|udf| $crate::bindings::exports::datafusion_udf_wasm::udf::types::ScalarUdf::new(
                        $crate::wrapper::ScalarUdfWrapper::new(udf)
                    ))
                    .collect()
            }
        }

        $crate::bindings::export!(Implementation with_types_in $crate::bindings);
    };
}
