//! [CPython]+[`pyo3`]-based UDFs.
//!
//!
//! [CPython]: https://www.python.org/
//! [`pyo3`]: https://pyo3.rs/
use std::any::Any;
use std::ops::ControlFlow;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::{
    DataFusionError, Result as DataFusionResult, exec_datafusion_err, exec_err,
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_udf_wasm_guest::export;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::error::py_err_to_string;
use crate::inspect::inspect_python_code;
use crate::signature::PythonFn;

mod conversion;
mod error;
mod inspect;
mod signature;

/// A test UDF that demonstrate that we can call Python.
#[derive(Debug)]
struct PythonScalarUDF {
    /// Handle of the wrapped python function.
    python_function: PythonFn,

    /// Signature of the UDF.
    ///
    /// We store this here because [`ScalarUDFImpl::signature`] requires us to return a reference.
    signature: Signature,
}

impl PythonScalarUDF {
    /// Create new UDF.
    fn new(python_function: PythonFn) -> Self {
        let signature = Signature::exact(
            python_function
                .signature
                .parameters
                .iter()
                .map(|t| t.t.data_type())
                .collect(),
            Volatility::Volatile,
        );

        Self {
            python_function,
            signature,
        }
    }

    /// This is [`ScalarUDFImpl::return_type`] but with a more flexible interface, so it can be used in [`ScalarUDFImpl::invoke_with_args`] as well.
    fn return_type_impl<'a, I>(&self, arg_types: I) -> Result<DataType, String>
    where
        I: ExactSizeIterator<Item = &'a DataType>,
    {
        if arg_types.len() != self.python_function.signature.parameters.len() {
            return Err(format!(
                "`{}` expects {} parameters but got {}",
                self.name(),
                self.python_function.signature.parameters.len(),
                arg_types.len(),
            ));
        }

        for (pos, (actual, expected)) in arg_types
            .zip(&self.python_function.signature.parameters)
            .enumerate()
        {
            let expected = expected.t.data_type();
            if actual != &expected {
                return Err(format!(
                    "argument {} of `{}` should be {}, got {}",
                    pos + 1,
                    self.name(),
                    expected,
                    actual
                ));
            }
        }

        Ok(self.python_function.signature.return_type.t.data_type())
    }
}

impl ScalarUDFImpl for PythonScalarUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.python_function.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        self.return_type_impl(arg_types.iter())
            .map_err(DataFusionError::Plan)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
        } = args;

        let return_dt = self.python_function.signature.return_type.t.data_type();
        if return_field.data_type() != &return_dt {
            return exec_err!(
                "`{}` returns {} but was asked to produce {}",
                self.name(),
                return_dt,
                return_field.data_type()
            );
        }
        if !return_field.is_nullable() {
            return exec_err!(
                "`{}` returns nullable data but was asked not to do so",
                self.name()
            );
        }

        // check arg fields by re-using our `return_type` code
        self.return_type_impl(arg_fields.iter().map(|arg_field| arg_field.data_type()))
            .map_err(|msg| {
                DataFusionError::Execution(format!("checking argument fields: {msg}"))
            })?;

        if args.len() != self.python_function.signature.parameters.len() {
            return exec_err!(
                "`{}` expects {} parameters (passed as args) but got {}",
                self.name(),
                self.python_function.signature.parameters.len(),
                args.len()
            );
        }

        let arrays = args
            .into_iter()
            .enumerate()
            .map(|(i, column_value)| {
                let array = column_value.to_array(number_rows)?;
                if array.len() != number_rows {
                    return exec_err!(
                        "array passed for argument {} should have {number_rows} rows but has {}",
                        i + 1,
                        array.len()
                    );
                }
                Ok(array)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Python::with_gil(|py| {
            let mut parameter_iters = arrays
                .iter()
                .zip(&self.python_function.signature.parameters)
                .map(|(array, t)| t.arrow_to_python(array, py))
                .collect::<Result<Vec<_>, _>>()?;

            let handle = self.python_function.handle.bind(py);
            let mut output_row_builder = self
                .python_function
                .signature
                .return_type
                .python_to_arrow(py, number_rows);

            for _ in 0..number_rows {
                // poll ALL iterators before evaluating the controlflow
                let params = parameter_iters
                    .iter_mut()
                    .map(|it| it.next().expect("all iterators have n_rows"))
                    .collect::<Result<Vec<_>, _>>()?;

                // determine if we shall call the Python method or not
                let maybe_params = params.into_iter().try_fold(
                    Vec::with_capacity(parameter_iters.len()),
                    |mut accu, next| {
                        accu.push(next?);
                        ControlFlow::Continue(accu)
                    },
                );

                match maybe_params {
                    ControlFlow::Continue(params) => {
                        let params = PyTuple::new(py, params).map_err(|e| {
                            exec_datafusion_err!(
                                "cannot create parameter tuple: {}",
                                py_err_to_string(e, py)
                            )
                        })?;
                        let rval = handle.call1(params).map_err(|e| {
                            exec_datafusion_err!(
                                "cannot call function: {}",
                                py_err_to_string(e, py)
                            )
                        })?;
                        output_row_builder.push(rval)?;
                    }
                    ControlFlow::Break(()) => {
                        output_row_builder.skip();
                    }
                }
            }

            // check invariants
            for (i, mut it) in parameter_iters.into_iter().enumerate() {
                let next = it.next();
                assert!(
                    next.is_none(),
                    "iterator {} should be done but produced {next:?}",
                    i + 1,
                );
            }

            let output_array = output_row_builder.finish();
            // check invariants
            assert_eq!(output_array.len(), number_rows);

            Ok(ColumnarValue::Array(output_array))
        })
    }
}

/// Return root file system.
///
/// This will be [`Some`] if built for WASM, but [`None`] if build for non-WASM host (e.g. during `cargo check`).
#[allow(clippy::allow_attributes, clippy::const_is_empty)]
fn root() -> Option<Vec<u8>> {
    // The build script will ALWAYS set this environment variable, but if we don't bundle the standard lib the file
    // will simply be empty.
    const ROOT_TAR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/python-lib.tar"));
    (!ROOT_TAR.is_empty()).then(|| ROOT_TAR.to_vec())
}

/// Initialize Python interpreter.
///
/// This should always be called before performing any Python interaction. Calling the method more than once is fine
/// and will result in a "no-op"
///
/// # Panic
/// This assumes that the root file system contains the [Python Standard Library] or that the Python interpeter can
/// find it somewhere else (e.g. when executed outside of a WASM context).
///
/// Otherwise this will likely panic and you find the following printed to stderr:
///
/// ```text
/// Could not find platform independent libraries <prefix>
/// Could not find platform dependent libraries <exec_prefix>
/// Fatal Python error: Failed to import encodings module
/// Python runtime state: core initialized
/// ModuleNotFoundError: No module named 'encodings'
///
/// Current thread 0x012bd368 (most recent call first):
///   <no Python frame>
/// ```
///
///
/// [Python Standard Library]: https://docs.python.org/3/library/index.html
fn init_python() {
    pyo3::prepare_freethreaded_python();
}

/// Generate UDFs from given Python string.
fn udfs(source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    init_python();

    let udfs = inspect_python_code(&source)?;
    Ok(udfs
        .into_iter()
        .map(|f| Arc::new(PythonScalarUDF::new(f)) as _)
        .collect())
}

export! {
    root_fs_tar: root,
    scalar_udfs: udfs,
}
