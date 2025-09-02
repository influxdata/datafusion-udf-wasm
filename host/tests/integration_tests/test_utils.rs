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
