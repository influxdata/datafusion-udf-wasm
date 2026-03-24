use arrow::array::ArrayRef;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;

/// Extension trait for [`ColumnarValue`] for easier testing.
pub(crate) trait ColumnarValueExt {
    /// Extracts [`ColumnarValue::Array`] variant.
    ///
    /// # Panic
    /// Panics if this is not an array.
    #[track_caller]
    fn unwrap_array(self) -> ArrayRef;

    /// Extracts [`ColumnarValue::Scalar`] variant.
    ///
    /// # Panic
    /// Panics if this is not an scalar.
    #[track_caller]
    fn unwrap_scalar(self) -> ScalarValue;
}

impl ColumnarValueExt for ColumnarValue {
    #[track_caller]
    fn unwrap_array(self) -> ArrayRef {
        match self {
            Self::Array(array) => array,
            Self::Scalar(_) => panic!("expected an array but got a scalar"),
        }
    }

    #[track_caller]
    fn unwrap_scalar(self) -> ScalarValue {
        match self {
            Self::Array(_) => panic!("expected a scalar but got an array"),
            Self::Scalar(scalar_value) => scalar_value,
        }
    }
}

/// Error that shows entire error chain.
///
/// This makes panic messages a la `Err(e).unwrap()` a bit easier to digest.
pub(crate) struct FullError {
    inner: Box<dyn std::error::Error + Send + Sync>,
}

impl FullError {
    /// Create new error wrapper.
    pub(crate) fn new<E>(e: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self { inner: Box::new(e) }
    }
}

impl std::fmt::Debug for FullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

impl std::fmt::Display for FullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // the final error message
        let mut out = String::new();

        // traverse error chain
        let mut maybe_next_err: Option<&(dyn std::error::Error + 'static)> =
            Some(self.inner.as_ref());
        while let Some(err) = maybe_next_err {
            let err_msg = err.to_string();

            // look for biggest overlap with existing parts
            let overlap = (1..=(err_msg.len().min(out.len())))
                .rev()
                .find(|overlap| out[out.len() - overlap..] == err_msg[..*overlap])
                .unwrap_or(0);
            let new_part = &err_msg[overlap..];

            // make sure that there's a separation between old and new part
            const SEP: &str = ": ";
            if !out.is_empty()
                && !new_part.is_empty()
                && !out.ends_with(SEP)
                && !new_part.starts_with(SEP)
            {
                out.push_str(SEP);
            }

            out.push_str(new_part);

            maybe_next_err = err.source();
        }

        write!(f, "{out}")
    }
}

impl std::error::Error for FullError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.inner.as_ref())
    }
}
