//! Methods used by multiple payloads.

use std::{hash::Hash, sync::Arc};

use arrow::{array::StringArray, datatypes::DataType};
use datafusion_common::{Result as DataFusionResult, cast::as_string_array};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// Return empty root file system.
///
/// This always returns [`None`] because the example does not need any files.
pub(crate) fn root_empty() -> Option<Vec<u8>> {
    None
}

/// Returns empty list of UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs_empty(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![])
}

/// UDF that produces a string from one input.
pub(crate) struct String1Udf {
    /// Name.
    name: &'static str,

    /// String producer.
    effect: Box<dyn Fn(String) -> Result<String, String> + Send + Sync>,

    /// Signature of the UDF.
    ///
    /// We store this here because [`ScalarUDFImpl::signature`] requires us to return a reference.
    signature: Signature,
}

impl String1Udf {
    /// Create new UDF.
    pub(crate) fn new<F>(name: &'static str, effect: F) -> Self
    where
        F: Fn(String) -> Result<String, String> + Send + Sync + 'static,
    {
        Self {
            name,
            effect: Box::new(effect),
            signature: Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl std::fmt::Debug for String1Udf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            name,
            effect: _,
            signature,
        } = self;

        f.debug_struct("StringUdf")
            .field("name", name)
            .field("effect", &"<EFFECT>")
            .field("signature", signature)
            .finish()
    }
}

impl PartialEq<Self> for String1Udf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for String1Udf {}

impl Hash for String1Udf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for String1Udf {
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
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let array = args
            .args
            .into_iter()
            .next()
            .unwrap()
            .into_array(args.number_rows)
            .unwrap();
        let array = as_string_array(&array).unwrap();
        let array = array
            .iter()
            .map(|s| {
                s.map(|s| match (self.effect)(s.to_owned()) {
                    Ok(s) => format!("OK: {s}"),
                    Err(s) => format!("ERR: {s}"),
                })
            })
            .collect::<StringArray>();
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}
