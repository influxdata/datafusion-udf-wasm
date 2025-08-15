use std::sync::Arc;

use crate::bindings::exports::datafusion_udf_wasm::udf::types::GuestUdf;
use datafusion::logical_expr::ScalarUDFImpl;

#[derive(Debug)]
pub struct UdfWrapper(Arc<dyn ScalarUDFImpl>);

impl UdfWrapper {
    pub fn new(udf: Arc<dyn ScalarUDFImpl>) -> Self {
        Self(udf)
    }
}

impl GuestUdf for UdfWrapper {
    fn name(&self) -> String {
        self.0.name().to_owned()
    }
}
