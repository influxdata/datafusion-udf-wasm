use std::sync::Arc;

use crate::bindings::exports::datafusion_udf_wasm::udf::types::GuestScalarUdf;
use datafusion::logical_expr::ScalarUDFImpl;

#[derive(Debug)]
pub struct ScalarUdfWrapper(Arc<dyn ScalarUDFImpl>);

impl ScalarUdfWrapper {
    pub fn new(udf: Arc<dyn ScalarUDFImpl>) -> Self {
        Self(udf)
    }
}

impl GuestScalarUdf for ScalarUdfWrapper {
    fn name(&self) -> String {
        self.0.name().to_owned()
    }
}
