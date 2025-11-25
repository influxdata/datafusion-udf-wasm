//! Overly complex data emitted by the payload.

use arrow::datatypes::DataType;
use datafusion_common::{Result as DataFusionResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

pub(crate) mod udf_long_name;
pub(crate) mod udfs_duplicate_names;

/// UDF with a name
#[derive(Debug, PartialEq, Eq, Hash)]
struct NamedUdf(String);

impl ScalarUDFImpl for NamedUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.0
    }

    fn signature(&self) -> &Signature {
        static S: Signature = Signature {
            type_signature: TypeSignature::Uniform(0, vec![]),
            volatility: Volatility::Immutable,
        };

        &S
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Null)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Null))
    }
}
