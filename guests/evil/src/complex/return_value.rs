//! Payloads that return complex values when being invoked.

use std::sync::Arc;

use arrow::{
    array::{Int64Array, StructArray},
    datatypes::{DataType, Field},
};
use datafusion_common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use crate::common::DynBox;

/// UDF that returns a specific data type.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ReturnValueUDF {
    /// Name.
    name: &'static str,

    /// The return value
    return_value: DynBox<ColumnarValue>,
}

impl ReturnValueUDF {
    /// Create new UDF.
    fn new(name: &'static str, return_value: ColumnarValue) -> Self {
        Self {
            name,
            return_value: DynBox(Box::new(return_value)),
        }
    }
}

impl ScalarUDFImpl for ReturnValueUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name
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
        Err(DataFusionError::NotImplemented("return_type".to_owned()))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(self.return_value.clone())
    }
}

/// Returns our evil UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    let max_depth: usize = std::env::var("max_depth").unwrap().parse().unwrap();

    Ok(vec![
        Arc::new(ReturnValueUDF::new(
            "dt_depth_array",
            ColumnarValue::Array((0..=max_depth).fold(
                Arc::new(Int64Array::new(vec![1].into(), None)),
                |a, _| {
                    Arc::new(StructArray::from(vec![(
                        Arc::new(Field::new("a", a.data_type().clone(), true)),
                        a,
                    )]))
                },
            )),
        )),
        Arc::new(ReturnValueUDF::new(
            "dt_depth_scalar",
            ColumnarValue::Scalar((0..=max_depth).fold(ScalarValue::Int64(Some(1)), |v, _| {
                ScalarValue::Struct(Arc::new(StructArray::from(vec![(
                    Arc::new(Field::new("a", v.data_type(), true)),
                    v.to_array().unwrap(),
                )])))
            })),
        )),
    ])
}
