use std::sync::Arc;

use datafusion::{
    arrow::datatypes::DataType,
    common::plan_err,
    error::Result as DataFusionResult,
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
};

#[derive(Debug)]
struct AddOne {
    signature: Signature,
}

impl AddOne {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Int32], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AddOne {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "add_one"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        if !matches!(arg_types.get(0), Some(&DataType::Int32)) {
            return plan_err!("add_one only accepts Int32 arguments");
        }
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        todo!()
    }
}

pub(crate) fn udfs() -> Vec<Arc<dyn ScalarUDFImpl>> {
    vec![Arc::new(AddOne::new())]
}
