//! Methods used by multiple payloads.

use std::{fmt::Debug, hash::Hash, ops::Deref, sync::Arc};

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

/// A container that shadows its inner dynamic value.
///
/// This is mostly used to simplify the handling of `Box<dyn Fn(...) -> ...>`.
///
/// The actual value is ignored for [`Debug`], [`PartialEq`], [`Eq`], and [`Hash`].
pub(crate) struct DynBox<T>(pub(crate) Box<T>)
where
    T: ?Sized;

impl<T> Debug for DynBox<T>
where
    T: ?Sized,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DynBox").finish_non_exhaustive()
    }
}

impl<T> PartialEq<Self> for DynBox<T>
where
    T: ?Sized,
{
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<T> Eq for DynBox<T> where T: ?Sized {}

impl<T> Hash for DynBox<T>
where
    T: ?Sized,
{
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}

impl<T> Deref for DynBox<T>
where
    T: ?Sized,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// UDF that produces a string from one input.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct String1Udf {
    /// Name.
    name: &'static str,

    /// String producer.
    effect: DynBox<dyn Fn(String) -> Result<String, String> + Send + Sync>,

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
            effect: DynBox(Box::new(effect)),
            signature: Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable),
        }
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
