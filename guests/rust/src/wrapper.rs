use std::sync::Arc;

use datafusion::logical_expr::ScalarUDFImpl;

use crate::bindings::exports::datafusion_udf_wasm::udf::types::{Guest, GuestUdf, Udf};

#[derive(Debug)]
pub(crate) struct UdfWrapper(Arc<dyn ScalarUDFImpl>);

impl UdfWrapper {
    pub(crate) fn new(udf: Arc<dyn ScalarUDFImpl>) -> Self {
        Self(udf)
    }
}

impl GuestUdf for UdfWrapper {
    fn name(&self) -> String {
        self.0.name().to_owned()
    }
}

#[derive(Debug)]
pub(crate) struct Implementation;

impl Guest for Implementation {
    type Udf = UdfWrapper;

    fn udfs() -> Vec<Udf> {
        crate::example::udfs()
            .into_iter()
            .map(|udf| Udf::new(UdfWrapper::new(udf)))
            .collect()
    }
}

crate::bindings::export!(Implementation with_types_in crate::bindings);
