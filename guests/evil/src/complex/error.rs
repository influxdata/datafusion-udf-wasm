//! Badness related to errors returned by the UDF.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use crate::common::DynBox;

/// UDF that returns an error when invoked.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ErrorUDF {
    /// Name.
    name: &'static str,

    /// The error generator.
    err: DynBox<dyn Fn() -> DataFusionError + Send + Sync>,
}

impl ErrorUDF {
    /// Create new UDF.
    fn new<F>(name: &'static str, f: F) -> Self
    where
        F: Fn() -> DataFusionError + Send + Sync + 'static,
    {
        Self {
            name,
            err: DynBox(Box::new(f)),
        }
    }
}

impl ScalarUDFImpl for ErrorUDF {
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
        Ok(DataType::Null)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Err((self.err)())
    }
}

/// Returns our evil UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![
        Arc::new(ErrorUDF::new("long_ctx", || {
            let limit: usize = std::env::var("limit").unwrap().parse().unwrap();
            DataFusionError::Execution("foo".to_owned())
                .context(std::iter::repeat_n('x', limit + 1).collect::<String>())
        })),
        Arc::new(ErrorUDF::new("long_msg", || {
            let limit: usize = std::env::var("limit").unwrap().parse().unwrap();
            DataFusionError::Execution(std::iter::repeat_n('x', limit + 1).collect())
        })),
        Arc::new(ErrorUDF::new("nested_ctx", || {
            let limit: usize = std::env::var("limit").unwrap().parse().unwrap();
            (0..=limit).fold(DataFusionError::Execution("foo".to_owned()), |err, _| {
                err.context("bar")
            })
        })),
    ])
}
