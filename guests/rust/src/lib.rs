pub mod bindings;
pub mod wrapper;

#[macro_export]
macro_rules! export {
    ($fn:ident) => {

        #[derive(Debug)]
        struct Implementation;

        impl $crate::bindings::exports::datafusion_udf_wasm::udf::types::Guest for Implementation {
            type Udf = $crate::wrapper::UdfWrapper;

            fn udfs() -> Vec<$crate::bindings::exports::datafusion_udf_wasm::udf::types::Udf> {
                $fn()
                    .into_iter()
                    .map(|udf| $crate::bindings::exports::datafusion_udf_wasm::udf::types::Udf::new($crate::wrapper::UdfWrapper::new(udf)))
                    .collect()
            }
        }

        $crate::bindings::export!(Implementation with_types_in $crate::bindings);
    };
}
