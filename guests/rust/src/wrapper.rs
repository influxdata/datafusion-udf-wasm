use std::sync::Arc;

use crate::bindings::exports::datafusion_udf_wasm::udf::types as wit_types;
use datafusion::{arrow::datatypes::DataType, logical_expr::ScalarUDFImpl};

#[derive(Debug)]
pub struct ScalarUdfWrapper(Arc<dyn ScalarUDFImpl>);

impl ScalarUdfWrapper {
    pub fn new(udf: Arc<dyn ScalarUDFImpl>) -> Self {
        Self(udf)
    }
}

impl wit_types::GuestScalarUdf for ScalarUdfWrapper {
    fn name(&self) -> String {
        self.0.name().to_owned()
    }

    fn signature(&self) -> wit_types::Signature {
        self.0
            .signature()
            .clone()
            .try_into()
            .expect("signature conversion")
    }

    fn return_type(
        &self,
        arg_types: Vec<wit_types::DataType>,
    ) -> Result<wit_types::DataType, wit_types::DataFusionError> {
        let arg_types = arg_types
            .into_iter()
            .map(DataType::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let data_type = self.0.return_type(&arg_types)?;
        Ok(data_type.into())
    }
}
