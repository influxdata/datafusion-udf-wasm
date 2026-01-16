//! Payload that returns invalid data.

use std::sync::Arc;

use arrow::{array::StringArray, datatypes::DataType};
use datafusion_common::error::Result as DataFusionResult;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

/// UDF that return the wrong number of rows.
#[derive(Debug, PartialEq, Eq, Hash)]
struct WrongNumberOfRows;

impl ScalarUDFImpl for WrongNumberOfRows {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "wrong-number-of-rows"
    }

    fn signature(&self) -> &Signature {
        static S: Signature = Signature {
            type_signature: TypeSignature::Uniform(0, vec![]),
            volatility: Volatility::Immutable,
            parameter_names: None,
        };

        &S
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Array(Arc::new(
            (0..=args.number_rows)
                .map(|idx| Some(idx.to_string()))
                .collect::<StringArray>(),
        )))
    }
}

/// Returns our evil UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![Arc::new(WrongNumberOfRows)])
}
