// unused-crate-dependencies false positives
#![expect(unused_crate_dependencies)]

use std::sync::Arc;

use arrow::{array::Int32Array, datatypes::DataType};
use datafusion_common::{
    Result as DataFusionResult, ScalarValue, exec_datafusion_err, exec_err, plan_err,
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_udf_wasm_guest::export;

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
        if arg_types.len() != 1 {
            return plan_err!("add_one expects exactly one argument");
        }
        if !matches!(arg_types.first(), Some(&DataType::Int32)) {
            return plan_err!("add_one only accepts Int32 arguments");
        }
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            arg_fields: _,
            number_rows: _,
            return_field: _,
        } = args;

        // extract inputs
        if args.len() != 1 {
            return exec_err!("add_one expects exactly one argument");
        }
        match &args[0] {
            ColumnarValue::Array(array) => {
                let array = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| exec_datafusion_err!("invalid array type"))?;

                // perform calculation
                let array = array
                    .iter()
                    .map(|x| x.and_then(|x| x.checked_add(1)))
                    .collect::<Int32Array>();

                // create output
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let ScalarValue::Int32(x) = scalar else {
                    return exec_err!("add_one only accepts Int32 arguments");
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    x.and_then(|x| x.checked_add(1)),
                )))
            }
        }
    }
}

fn root() -> Option<Vec<u8>> {
    None
}

fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![Arc::new(AddOne::new())])
}

export! {
    root_fs_tar: root,
    scalar_udfs: udfs,
}
