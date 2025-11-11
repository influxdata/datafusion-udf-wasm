//! Types that represent Python function signatures and handles.
use pyo3::{Py, PyAny};

/// Python types that we support.
///
/// Note that this type does NOT reason about nullability. See [`PythonNullableType`] for that.
///
/// # Naming
/// Since Python and Arrow use different names for the same type, we have to settle on some consistency. We chose to
/// use the Python name in CamelCase style, so Python's `datetime` will become `DateTime`.
#[derive(Debug, PartialEq, Eq, Hash)]
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

    /// Binary data (bytes).
    ///
    /// # Python
    /// The type is called `bytes`, documentation can be found here:
    ///
    /// - <https://docs.python.org/3/library/stdtypes.html#bytes-objects>
    /// - <https://docs.python.org/3/library/functions.html#bytes>
    ///
    /// # Arrow
    /// We map this to [`Binary`](arrow::datatypes::DataType::Binary).
    Bytes,

    /// Date (year, month, day).
    ///
    /// # Python
    /// The type is called `date`, documentation can be found here:
    ///
    /// - <https://docs.python.org/3/library/datetime.html#datetime.date>
    ///
    /// # Arrow
    /// We map this to [`Date32`](arrow::datatypes::DataType::Date32) which represents
    /// the number of days since the Unix epoch (1970-01-01).
    Date,

    /// Timestamp (date + time on that day).
    ///
    /// # Python
    /// The type is called `datetime`, documentation can be found here:
    ///
    /// - <https://docs.python.org/3/library/datetime.html#datetime.datetime>
    ///
    /// # Arrow
    /// We map this to [`Timestamp`](arrow::datatypes::DataType::Timestamp) with
    /// [`Microsecond`](arrow::datatypes::TimeUnit::Microsecond) resolution (same as Python) and no time zone.
    DateTime,

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

    /// None/Null.
    ///
    /// # Python
    /// The type is called `None`, documentation can be found here:
    ///
    /// - <https://docs.python.org/3/library/constants.html#None>
    /// - <https://docs.python.org/3/library/types.html#types.NoneType>
    ///
    /// # Arrow
    /// We map this to [`Null`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Null).
    None,

    /// String.
    ///
    /// # Python
    /// The type is called `str`, documentation can be found here:
    ///
    /// - <https://docs.python.org/3/library/stdtypes.html#text-sequence-type-str>
    ///
    /// # Arrow
    /// We map this to [`Utf8`](arrow::datatypes::DataType::Utf8).
    Str,

    /// Time (hour, minute, second, microsecond).
    ///
    /// # Python
    /// The type is called `time`, documentation can be found here:
    ///
    /// - <https://docs.python.org/3/library/datetime.html#datetime.time>
    ///
    /// # Arrow
    /// We map this to [`Time64`](arrow::datatypes::DataType::Time64) with
    /// [`Microsecond`](arrow::datatypes::TimeUnit::Microsecond) resolution (same as Python) and no time zone.
    Time,

    /// Timedelta (duration).
    ///
    /// # Python
    /// The type is called `timedelta`, documentation can be found here:
    ///
    /// - <https://docs.python.org/3/library/datetime.html#datetime.timedelta>
    ///
    /// # Arrow
    /// We map this to [`Duration`](arrow::datatypes::DataType::Duration) with
    /// [`Microsecond`](arrow::datatypes::TimeUnit::Microsecond) resolution (same as Python).
    Timedelta,
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
