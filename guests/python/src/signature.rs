//! Types that represent Python function signatures and handles.
use pyo3::{Py, PyAny};

/// Python types that we support.
///
/// Note that this type does NOT reason about nullability. See [`PythonNullableType`] for that.
#[derive(Debug)]
pub(crate) enum PythonType {
    /// Boolean.
    ///
    /// # Python
    /// The type is called `bool`, documentation can be found here:
    ///
    /// - <https://docs.python.org/3/library/stdtypes.html#boolean-type-bool>
    /// - <https://docs.python.org/3/library/functions.html#bool>
    ///
    /// # Arrow
    /// We map this to [`Boolean`](arrow::datatypes::DataType::Boolean).
    Bool,

    /// Float.
    ///
    /// # Python
    /// The type is called `float`, documentation can be found here:
    ///
    /// - <https://docs.python.org/3/library/stdtypes.html#numeric-types-int-float-complex>
    /// - <https://docs.python.org/3/library/functions.html#float>
    ///
    /// # Arrow
    /// We map this to [`Float64`](arrow::datatypes::DataType::Float64).
    Float,

    /// Signed integer.
    ///
    /// # Python
    /// The type is called `int`, documentation can be found here:
    ///
    /// - <https://docs.python.org/3/library/stdtypes.html#numeric-types-int-float-complex>
    /// - <https://docs.python.org/3/library/functions.html#int>
    ///
    /// # Arrow
    /// We map this to [`Int64`](arrow::datatypes::DataType::Int64).
    Int,
}

/// [`PythonType`] plus "nullable" flag.
///
/// # Python
/// Nulls in Python are represented using `None`. Documentation is available here:
///
/// - <https://docs.python.org/3/library/stdtypes.html#the-null-object>
///
/// Nullable types are represented using a union with another type, e.g. `int | None`.
///
/// There used to be an older representation too: `typing.Optional[int]`. As of Python 3.14, this results in the same
/// representation as `int | None`. See <https://docs.python.org/3.14/whatsnew/3.14.html#typing>. So we support both.
#[derive(Debug)]
pub(crate) struct PythonNullableType {
    /// Python type.
    pub(crate) t: PythonType,

    /// Is this nullable within Python or not?
    pub(crate) nullable: bool,
}

/// Signature of a Python function.
#[derive(Debug)]
pub(crate) struct PythonFnSignature {
    /// Parameter is order.
    ///
    /// We only support unnamed arguments.
    pub(crate) parameters: Vec<PythonNullableType>,

    /// Return type.
    pub(crate) return_type: PythonNullableType,
}

/// Handle of a Python function.
#[derive(Debug)]
pub(crate) struct PythonFn {
    /// Function name.
    pub(crate) name: String,

    /// Type signature.
    pub(crate) signature: PythonFnSignature,

    /// Handle of the object within the Python VM.
    pub(crate) handle: Py<PyAny>,
}
