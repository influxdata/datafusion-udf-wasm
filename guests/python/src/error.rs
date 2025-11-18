//! Helper for error handling.
use pyo3::{
    PyErr, PyResult, PyTypeInfo, Python, intern,
    types::{PyAnyMethods, PyDict},
};

/// Convert a Python error to a string.
///
/// The resulted string can easily be passed into a [`DataFusionError`].
///
///
/// [`DataFusionError`]: datafusion_common::error::DataFusionError
pub(crate) fn py_err_to_string(e: PyErr, py: Python<'_>) -> String {
    try_py_err_to_string(e, py).unwrap_or_else(|e| format!("cannot format exception: {e}"))
}

/// Fallible implementation of [`py_err_to_string`].
fn try_py_err_to_string(e: PyErr, py: Python<'_>) -> PyResult<String> {
    // Instead of hand-rolling our own exception + traceback printing mechanism, we just call
    // `traceback.print_exception`. For that we need a `StringIO` object as an output buffer.

    // https://docs.python.org/3/library/io.html
    let mod_io = py.import(intern!(py, "io"))?;
    let type_string_io = mod_io.getattr(intern!(py, "StringIO"))?;
    let string_io = type_string_io.call0()?;

    // https://docs.python.org/3/library/traceback.html
    let mod_traceback = py.import(intern!(py, "traceback"))?;
    let fun_print_exception = mod_traceback.getattr(intern!(py, "print_exception"))?;

    // call `print_exception(e, file=string_io)`
    let kwargs = PyDict::new(py);
    kwargs.set_item("file", &string_io)?;
    fun_print_exception.call((e,), Some(&kwargs))?;

    // call `string_io.getvalue()`
    let s = string_io.call_method("getvalue", (), None)?;
    let s: String = s.extract()?;

    Ok(s)
}

/// Extension trait for [`PyErr`].
pub(crate) trait PyErrExt {
    /// Add context to error by adding a [cause].
    ///
    /// So this Rust code:
    ///
    /// ```rust,no-compile
    /// my_fn
    ///     .call(...)
    ///     .context("foo")
    /// ```
    ///
    /// is roughly equivalent to:
    ///
    /// ```python
    /// try:
    ///     my_fn(...)
    /// except Exception as e:
    ///     raise Exception("foo") from e
    /// ```
    ///
    ///
    /// [cause]: https://docs.python.org/3/library/exceptions.html#BaseException.__cause__
    fn context<T>(self, ctx: String, py: Python<'_>) -> Self
    where
        T: PyTypeInfo;
}

impl PyErrExt for PyErr {
    fn context<T>(self, ctx: String, py: Python<'_>) -> Self
    where
        T: PyTypeInfo,
    {
        let e2 = Self::new::<T, _>(ctx);
        e2.set_cause(py, Some(self));
        e2
    }
}

impl<X> PyErrExt for Result<X, PyErr> {
    fn context<T>(self, ctx: String, py: Python<'_>) -> Self
    where
        T: PyTypeInfo,
    {
        self.map_err(|e| e.context::<T>(ctx, py))
    }
}
