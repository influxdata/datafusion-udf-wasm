//! Wrapper for [DataFusion] types so that the implement the respective [WIT interfaces](crate::bindings).
//!
//!
//! [DataFusion]: https://datafusion.apache.org/
use std::sync::Arc;

use crate::bindings::exports::datafusion_udf_wasm::udf::types as wit_types;
use arrow::datatypes::DataType;
use datafusion_expr::ScalarUDFImpl;

/// Wraps a [`ScalarUDFImpl`] so that it implements the [WIT definition].
///
///
/// [WIT definition]: wit_types::GuestScalarUdf
#[derive(Debug)]
pub struct ScalarUdfWrapper(Arc<dyn ScalarUDFImpl>);

impl ScalarUdfWrapper {
    /// Create new wrapper from [`ScalarUDFImpl`].
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

    fn invoke_with_args(
        &self,
        args: wit_types::ScalarFunctionArgs,
    ) -> Result<wit_types::ColumnarValue, wit_types::DataFusionError> {
        let args = args.try_into()?;
        let cval = self.0.invoke_with_args(args)?;
        let cval = cval.try_into()?;
        Ok(cval)
    }
}
