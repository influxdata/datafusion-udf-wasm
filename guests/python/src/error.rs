use pyo3::{PyErr, PyTypeInfo, Python};

pub(crate) fn py_err_to_string(mut e: PyErr, py: Python<'_>) -> String {
    let mut s = e.to_string();
    while let Some(cause) = e.cause(py) {
        s.push_str(&format!("\n\nCaused by:\n{cause}"));
        e = cause;
    }
    s
}

pub(crate) trait PyErrExt {
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
