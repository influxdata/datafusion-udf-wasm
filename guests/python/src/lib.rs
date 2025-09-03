use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::{
    DataFusionError, Result as DataFusionResult, ScalarValue, exec_err, plan_err,
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_udf_wasm_guest::export;
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;

#[derive(Debug)]
struct Test {
    signature: Signature,
}

impl Test {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(0, vec![], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Test {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "test"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        if !arg_types.is_empty() {
            return plan_err!("test expects no arguments");
        }
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            arg_fields: _,
            number_rows: _,
            return_field: _,
        } = args;

        if !args.is_empty() {
            return exec_err!("test expects no arguments");
        }

        let s = Python::with_gil(|py| {
            let sys = py.import("sys")?;
            let version: String = sys.getattr("version")?.extract()?;

            let locals = [("os", py.import("os")?)].into_py_dict(py)?;
            let code = c_str!("os.getenv('USER') or os.getenv('USERNAME') or 'Unknown'");
            let user: String = py.eval(code, None, Some(&locals))?.extract()?;

            let s = format!("Hello {user}, I'm Python {version}");
            PyResult::Ok(s)
        })
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))))
    }
}

#[allow(clippy::allow_attributes, clippy::const_is_empty)]
fn root() -> Option<Vec<u8>> {
    // The build script will ALWAYS set this environment variable, but if we don't bundle the standard lib the file
    // will simply be empty.
    const ROOT_TAR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/python-lib.tar"));
    (!ROOT_TAR.is_empty()).then(|| ROOT_TAR.to_vec())
}

fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    pyo3::prepare_freethreaded_python();

    Ok(vec![Arc::new(Test::new())])
}

export! {
    root_fs_tar: root,
    scalar_udfs: udfs,
}
