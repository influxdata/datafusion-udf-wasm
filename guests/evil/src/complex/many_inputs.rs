//! UDF with many inputs.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// UDF with specific signature.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SignatureUDF {
    /// Name.
    name: &'static str,

    /// The signature.
    signature: Signature,
}

impl SignatureUDF {
    /// Create new UDF.
    fn new(name: &'static str, signature: Signature) -> Self {
        Self { name, signature }
    }
}

impl ScalarUDFImpl for SignatureUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Null)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Err(DataFusionError::NotImplemented(
            "invoke_with_args".to_owned(),
        ))
    }
}

/// Returns our evil UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    let limit: usize = std::env::var("limit").unwrap().parse().unwrap();

    Ok(vec![Arc::new(SignatureUDF::new(
        "input_count",
        Signature::exact(vec![DataType::Null; limit + 1], Volatility::Immutable),
    ))])
}
