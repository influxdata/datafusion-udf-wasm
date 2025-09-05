//! Helper for error handling.
use pyo3::{PyErr, PyTypeInfo, Python};

/// Convert a Python error to a string.
///
/// The resulted string can easily be passed into a [`DataFusionError`].
///
///
/// [`DataFusionError`]: datafusion_common::error::DataFusionError
pub(crate) fn py_err_to_string(mut e: PyErr, py: Python<'_>) -> String {
    let mut s = e.to_string();
    while let Some(cause) = e.cause(py) {
        s.push_str(&format!("\n\nCaused by:\n{cause}"));
        e = cause;
    }
    s
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
    ///     raise Excpetion("foo") from e
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
