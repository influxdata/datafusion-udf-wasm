//! Example Scalar UDF that just implements "sub str".

// unused-crate-dependencies false positives
#![expect(unused_crate_dependencies)]

use std::sync::Arc;

use arrow::{array::StringArray, datatypes::DataType};
use datafusion_common::{
    Result as DataFusionResult, ScalarValue, exec_datafusion_err, exec_err, plan_err,
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_udf_wasm_guest::export;

/// UDF that implements "sub str".
#[derive(Debug, PartialEq, Eq, Hash)]
struct SubStr {
    /// Signature of the UDF.
    ///
    /// We store this here because [`ScalarUDFImpl::signature`] requires us to return a reference.
    signature: Signature,
}

impl Default for SubStr {
    fn default() -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SubStr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "sub_str"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        if arg_types.len() != 1 {
            return plan_err!("sub_str expects exactly one argument");
        }
        if !matches!(arg_types.first(), Some(&DataType::Utf8)) {
            return plan_err!("sub_str only accepts Utf8 arguments");
        }
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            arg_fields: _,
            number_rows: _,
            return_field: _,
            config_options: _,
        } = args;

        // extract inputs
        if args.len() != 1 {
            return exec_err!("sub_str expects exactly one argument");
        }
        match &args[0] {
            ColumnarValue::Array(array) => {
                let array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| exec_datafusion_err!("invalid array type"))?;

                // perform calculation
                let array = array
                    .iter()
                    .map(|x| x.and_then(sub_str))
                    .collect::<StringArray>();

                // create output
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let ScalarValue::Utf8(s) = scalar else {
                    return exec_err!("sub_str only accepts Utf8 arguments");
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    s.as_deref().and_then(sub_str),
                )))
            }
        }
    }
}

/// Split string by `.` and get the 2nd substring.
///
/// # Examples
///
/// | Input           | Output        |
/// | --------------- | ------------- |
/// | `""`            | `None`        |
/// | `"foo"`         | `None`        |
/// | `"foo."`        | `Some("")`    |
/// | `"foo.bar"`     | `Some("bar")` |
/// | `".bar"`        | `Some("bar")` |
/// | `"foo.bar."`    | `Some("bar")` |
/// | `"foo.bar.baz"` | `Some("bar")` |
fn sub_str(s: &str) -> Option<String> {
    s.split(".").nth(1).map(|s| s.to_owned())
}

/// Return root file system.
///
/// This always returns [`None`] because the example does not need any files.
fn root() -> Option<Vec<u8>> {
    None
}

/// Returns our one example UDF.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![Arc::new(SubStr::default())])
}

export! {
    root_fs_tar: root,
    scalar_udfs: udfs,
}
