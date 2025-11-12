use arrow::array::ArrayRef;
use datafusion_expr::ColumnarValue;

/// Extension trait for [`ColumnarValue`] for easier testing.
pub(crate) trait ColumnarValueExt {
    /// Extracts [`ColumnarValue::Array`] variant.
    ///
    /// # Panic
    /// Panics if this is not an array.
    #[track_caller]
    fn unwrap_array(self) -> ArrayRef;
}

impl ColumnarValueExt for ColumnarValue {
    #[track_caller]
    fn unwrap_array(self) -> ArrayRef {
        match self {
            Self::Array(array) => array,
            Self::Scalar(_) => panic!("expected an array but got a scalar"),
        }
    }
}
