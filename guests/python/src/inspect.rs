//! Inspection of Python code to extract [signature](crate::signature) information.
use std::ffi::CString;

use datafusion_common::{DataFusionError, error::Result as DataFusionResult};
use pyo3::{
    Bound, FromPyObject, PyAny, PyErr, PyResult, Python,
    exceptions::PyTypeError,
    intern,
    types::{PyAnyMethods, PyDictMethods, PyModuleMethods, PyStringMethods, PyTypeMethods},
};

use crate::{
    error::{PyErrExt, py_err_to_string},
    signature::{PythonFn, PythonFnSignature, PythonNullableType, PythonType},
};

impl<'py> FromPyObject<'py> for PythonType {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();

        // https://docs.python.org/3/library/builtins.html
        let mod_builtins = py.import(intern!(py, "builtins"))?;
        let type_bool = mod_builtins.getattr(intern!(py, "bool"))?;
        let type_int = mod_builtins.getattr(intern!(py, "int"))?;

        if ob.is(type_bool) {
            Ok(Self::Bool)
        } else if ob.is(type_int) {
            Ok(Self::Int)
        } else {
            Err(PyErr::new::<PyTypeError, _>(format!(
                "unknown annotation type: {}",
                py_representation(ob)
            )))
        }
    }
}

impl<'py> FromPyObject<'py> for PythonNullableType {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();

        // https://docs.python.org/3/library/inspect.html
        let mod_inspect = py.import(intern!(py, "inspect"))?;
        // https://docs.python.org/3/library/inspect.html#inspect.Signature
        let type_signature = mod_inspect.getattr(intern!(py, "Signature"))?;
        // https://docs.python.org/3/library/inspect.html#inspect.Signature.empty
        let type_signature_empty = type_signature.getattr(intern!(py, "empty"))?;
        // https://docs.python.org/3/library/inspect.html#inspect.Parameter
        let type_parameter = mod_inspect.getattr(intern!(py, "Parameter"))?;
        // https://docs.python.org/3/library/inspect.html#inspect.Parameter.empty
        let type_parameter_empty = type_parameter.getattr(intern!(py, "empty"))?;
        if ob.is(&type_signature_empty) || ob.is(&type_parameter_empty) {
            return Err(PyErr::new::<PyTypeError, _>("type missing".to_owned()));
        }

        // https://docs.python.org/3/library/types.html
        let mod_types = py.import(intern!(py, "types"))?;
        let type_union = mod_types.getattr(intern!(py, "UnionType"))?;
        let type_none = mod_types.getattr(intern!(py, "NoneType"))?;

        if ob.is_instance(&type_union)? {
            let args = ob.getattr(intern!(py, "__args__"))?;

            let n_args = args.len()?;
            if n_args != 2 {
                return Err(PyErr::new::<PyTypeError, _>(format!(
                    "only unions of length 2 are supported, got {n_args}"
                )));
            }
            let (arg1, arg2): (Bound<'py, PyAny>, Bound<'py, PyAny>) = args.extract()?;

            let inner_type = if arg1.is(&type_none) {
                arg2
            } else if arg2.is(&type_none) {
                arg1
            } else {
                return Err(PyErr::new::<PyTypeError, _>(
                    "only unions with None are supported",
                ));
            };

            Ok(Self {
                t: inner_type.extract()?,
                nullable: true,
            })
        } else {
            Ok(Self {
                t: ob.extract()?,
                nullable: false,
            })
        }
    }
}

impl<'py> FromPyObject<'py> for PythonFnSignature {
    /// Convert [`inspect.Signature`] to [`PythonFnSignature`].
    ///
    /// [`inspect.Signature`]: https://docs.python.org/3/library/inspect.html#inspect.Signature
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();

        let parameters = ob.getattr(intern!(py, "parameters"))?;
        let parameters_values = parameters.getattr(intern!(py, "values"))?;
        let parameters = parameters_values
            .call0()?
            .try_iter()?
            .enumerate()
            .map(|(i, param)| {
                let param = param?;
                // param is now https://docs.python.org/3/library/inspect.html#inspect.Parameter

                let annotation = param.getattr(intern!(py, "annotation"))?;
                let param: PythonNullableType = annotation
                    .extract()
                    .context::<PyTypeError>(format!("inspect parameter {}", i + 1), py)?;
                PyResult::Ok(param)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let return_annotation = ob.getattr(intern!(py, "return_annotation"))?;
        let return_type: PythonNullableType = return_annotation
            .extract()
            .context::<PyTypeError>("inspect return type".to_owned(), py)?;

        Ok(Self {
            parameters,
            return_type,
        })
    }
}

/// Execute python code and retrieve the list of defined functions.
pub(crate) fn inspect_python_code(code: &str) -> DataFusionResult<Vec<PythonFn>> {
    Python::attach(|py| {
        inspect_python_code_inner(code, py)
            .map_err(|e| DataFusionError::Plan(py_err_to_string(e, py)))
    })
}

/// Inner implementation of [`inspect_python_code`] which is meant to wrapped into a Python execution context.
fn inspect_python_code_inner(code: &str, py: Python<'_>) -> PyResult<Vec<PythonFn>> {
    let code = CString::new(code).map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))?;

    // https://docs.python.org/3/library/inspect.html
    let mod_inspect = py.import(intern!(py, "inspect"))?;
    // https://docs.python.org/3/library/inspect.html#inspect.signature
    let fn_signature = mod_inspect.getattr(intern!(py, "signature"))?;

    // https://docs.python.org/3/library/builtins.html
    let mod_builtins = py.import(intern!(py, "builtins"))?;
    let ty_type = mod_builtins.getattr(intern!(py, "type"))?;

    py.run(&code, None, None)?;

    let mod_main = py.import(intern!(py, "__main__"))?;
    let main_content = mod_main.dict();

    let mut fns = vec![];
    for (name, val) in main_content.iter() {
        let Ok(name) = name.str() else {
            continue;
        };
        let Ok(name) = name.to_str() else {
            continue;
        };
        if name.starts_with("_") {
            continue;
        }
        let name = name.to_owned();

        if !val.is_callable() {
            continue;
        }

        // skip imports
        let Ok(val_module) = val.getattr(intern!(py, "__module__")) else {
            continue;
        };
        let Ok(val_module) = val_module.extract::<String>() else {
            continue;
        };
        if val_module != "__main__" {
            continue;
        }

        // skip type definitions like classes
        if val.is_instance(&ty_type)? {
            continue;
        }

        let signature = fn_signature.call((&val,), None)?;
        let signature: PythonFnSignature = signature
            .extract()
            .context::<PyTypeError>(format!("inspect type of `{name}`"), py)?;

        let handle = val.unbind();

        fns.push(PythonFn {
            name,
            signature,
            handle,
        });
    }

    Ok(fns)
}

/// Receives of human-readable representation of a given Python variable.
pub(crate) fn py_representation(ob: &Bound<'_, PyAny>) -> String {
    let s = ob
        .str()
        .and_then(|name| name.to_str().map(|s| s.to_owned()))
        .unwrap_or_else(|_| "<unknown>".to_owned());

    let ty = ob
        .get_type()
        .name()
        .and_then(|name| name.to_str().map(|s| s.to_owned()))
        .unwrap_or_else(|_| "<unknown>".to_owned());

    format!("`{s}` of type `{ty}`")
}
