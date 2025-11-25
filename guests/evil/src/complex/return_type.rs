//! Badness related to return data types.

use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field};
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

/// UDF that returns a specific data type.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ReturnTypeUDF {
    /// Name.
    name: &'static str,

    /// The return type
    return_type: DataType,
}

impl ReturnTypeUDF {
    /// Create new UDF.
    fn new(name: &'static str, return_type: DataType) -> Self {
        Self { name, return_type }
    }
}

impl ScalarUDFImpl for ReturnTypeUDF {
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
        };

        &S
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(self.return_type.clone())
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
    let max_identifier_length: usize = std::env::var("max_identifier_length")
        .unwrap()
        .parse()
        .unwrap();
    let max_aux_string_length: usize = std::env::var("max_aux_string_length")
        .unwrap()
        .parse()
        .unwrap();
    let max_depth: usize = std::env::var("max_depth").unwrap().parse().unwrap();
    let max_complexity: usize = std::env::var("max_complexity").unwrap().parse().unwrap();

    Ok(vec![
        Arc::new(ReturnTypeUDF::new(
            "dt_depth",
            (0..=max_depth).fold(DataType::Int64, |dt, _| {
                DataType::List(Arc::new(Field::new("f", dt, true)))
            }),
        )),
        Arc::new(ReturnTypeUDF::new(
            "field_name",
            DataType::List(Arc::new(Field::new(
                std::iter::repeat_n('x', max_identifier_length + 1).collect::<String>(),
                DataType::Int64,
                true,
            ))),
        )),
        Arc::new(ReturnTypeUDF::new(
            "field_md_key",
            DataType::List(Arc::new(
                Field::new("f", DataType::Int64, true).with_metadata(HashMap::from([(
                    std::iter::repeat_n('x', max_identifier_length + 1).collect::<String>(),
                    "value".to_owned(),
                )])),
            )),
        )),
        Arc::new(ReturnTypeUDF::new(
            "field_md_value",
            DataType::List(Arc::new(
                Field::new("f", DataType::Int64, true).with_metadata(HashMap::from([(
                    "key".to_owned(),
                    std::iter::repeat_n('x', max_aux_string_length + 1).collect::<String>(),
                )])),
            )),
        )),
        Arc::new(ReturnTypeUDF::new(
            "field_md_items",
            DataType::List(Arc::new(
                Field::new("f", DataType::Int64, true).with_metadata(
                    (0..=max_complexity)
                        .map(|idx| (idx.to_string(), "value".to_owned()))
                        .collect(),
                ),
            )),
        )),
    ])
}
