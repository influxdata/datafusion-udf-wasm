//! Overly complex data emitted by the payload.

use arrow::datatypes::DataType;
use datafusion_common::{Result as DataFusionResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

pub(crate) mod error;
pub(crate) mod many_inputs;
pub(crate) mod params_long_name;
pub(crate) mod params_many;
pub(crate) mod return_type;
pub(crate) mod return_value;
pub(crate) mod udf_long_name;
pub(crate) mod udfs_duplicate_names;
pub(crate) mod udfs_many;

/// Test UDF.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TestUdf {
    /// Name.
    name: String,

    /// Signature.
    signature: Signature,
}

impl Default for TestUdf {
    fn default() -> Self {
        Self {
            name: "udf".to_owned(),
            signature: Signature {
                type_signature: TypeSignature::Uniform(0, vec![]),
                volatility: Volatility::Immutable,
                parameter_names: None,
            },
        }
    }
}

impl ScalarUDFImpl for TestUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Null)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Null))
    }
}
