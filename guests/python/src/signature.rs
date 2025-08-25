use pyo3::{Py, PyAny};

#[derive(Debug)]
pub(crate) enum PythonType {
    Int,
}
#[derive(Debug)]
pub(crate) struct PythonNullableType {
    pub(crate) t: PythonType,
    pub(crate) nullable: bool,
}

#[derive(Debug)]
pub(crate) struct PythonFnSignature {
    pub(crate) parameters: Vec<PythonNullableType>,
    pub(crate) return_type: PythonNullableType,
}

#[derive(Debug)]
pub(crate) struct PythonFn {
    pub(crate) name: String,
    pub(crate) signature: PythonFnSignature,
    pub(crate) handle: Py<PyAny>,
}
