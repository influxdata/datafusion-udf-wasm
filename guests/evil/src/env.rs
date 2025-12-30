//! Payload that tries to read environment variables.
use std::{hash::Hash, io::Read, sync::Arc};

use arrow::datatypes::DataType;
use datafusion_common::{Result as DataFusionResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use crate::common::DynBox;

/// UDF that produces a string.
#[derive(Debug, PartialEq, Eq, Hash)]
struct StringUdf {
    /// Name.
    name: &'static str,

    /// String producer.
    effect: DynBox<dyn Fn() -> Option<String> + Send + Sync>,
}

impl StringUdf {
    /// Create new UDF.
    fn new<F>(name: &'static str, effect: F) -> Self
    where
        F: Fn() -> Option<String> + Send + Sync + 'static,
    {
        Self {
            name,
            effect: DynBox(Box::new(effect)),
        }
    }
}

impl ScalarUDFImpl for StringUdf {
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
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8((self.effect)())))
    }
}

/// Returns our evil UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![
        Arc::new(StringUdf::new("args", || {
            let vars = std::env::args().collect::<Vec<_>>();
            if vars.is_empty() {
                None
            } else {
                Some(vars.join(","))
            }
        })),
        Arc::new(StringUdf::new("current_dir", || {
            let d = std::env::current_dir().unwrap();
            Some(d.display().to_string())
        })),
        Arc::new(StringUdf::new("current_exe", || {
            let e = std::env::current_exe().unwrap_err();
            Some(e.to_string())
        })),
        Arc::new(StringUdf::new("env", || {
            let vars = std::env::vars()
                .map(|(k, v)| format!("{k}:{v}"))
                .collect::<Vec<_>>();
            if vars.is_empty() {
                None
            } else {
                Some(vars.join(","))
            }
        })),
        Arc::new(StringUdf::new("process_id", || {
            let id = std::process::id();
            Some(id.to_string())
        })),
        Arc::new(StringUdf::new("stdin", || {
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf).unwrap();
            Some(buf)
        })),
        Arc::new(StringUdf::new("thread_id", || {
            let id = std::thread::current().id();
            Some(format!("{id:?}"))
        })),
    ])
}
