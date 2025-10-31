//! Conversion routes from [`arrow`] to/from Python.
use std::{ops::ControlFlow, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, DurationMicrosecondBuilder,
        Float64Builder, Int64Builder, StringBuilder, Time64MicrosecondBuilder,
        TimestampMicrosecondBuilder,
    },
    datatypes::{DataType, TimeUnit},
};
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Timelike, Utc};
use datafusion_common::{
    cast::{
        as_binary_array, as_boolean_array, as_date32_array, as_duration_microsecond_array,
        as_float64_array, as_int64_array, as_string_array, as_time64_microsecond_array,
        as_timestamp_microsecond_array,
    },
    error::Result as DataFusionResult,
    exec_datafusion_err, exec_err,
};
use pyo3::{
    Bound, BoundObject, IntoPyObjectExt, PyAny, Python,
    types::{
        PyAnyMethods, PyBytes, PyDate, PyDateAccess, PyDateTime, PyDelta, PyInt, PyNone,
        PyStringMethods, PyTime, PyTimeAccess, PyTzInfoAccess,
    },
};

use crate::{
    inspect::py_representation,
    signature::{PythonNullableType, PythonType},
};

/// Iterator of optional Python values.
///
/// This is used to feed values into a Python function.
pub(crate) type PythonOptValueIter<'a> =
    Box<dyn Iterator<Item = DataFusionResult<Option<Bound<'a, PyAny>>>> + 'a>;

/// Iterator of Python values and the information if the Python method should be called for this row
/// ([`ControlFlow::Continue`], because we continue scanning for arguments) or not ([`ControlFlow::Break`]).
///
/// This is used to feed values into a Python function.
pub(crate) type PythonValueIter<'a> =
    Box<dyn Iterator<Item = DataFusionResult<ControlFlow<(), Bound<'a, PyAny>>>> + 'a>;

impl PythonType {
    /// Arrow [`DataType`] for a given Python type.
    pub(crate) fn data_type(&self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::Float => DataType::Float64,
            Self::Int => DataType::Int64,
            Self::Str => DataType::Utf8,
            Self::Bytes => DataType::Binary,
            Self::Date => DataType::Date32,
            Self::Time => DataType::Time64(TimeUnit::Microsecond),
            Self::Timedelta => DataType::Duration(TimeUnit::Microsecond),
        }
    }

    /// Convert arrow [`Array`] to iterator of optional Python values.
    fn arrow_to_python<'a>(
        &self,
        array: &'a dyn Array,
        py: Python<'a>,
    ) -> DataFusionResult<PythonOptValueIter<'a>> {
        match self {
            Self::Bool => {
                let array = as_boolean_array(array)?;

                let it = array.into_iter().map(move |maybe_val| {
                    maybe_val
                        .map(|val| {
                            val.into_bound_py_any(py).map_err(|e| {
                                exec_datafusion_err!(
                                    "cannot convert Rust `bool` value to Python: {e}"
                                )
                            })
                        })
                        .transpose()
                });

                Ok(Box::new(it))
            }
            Self::DateTime => {
                let array = as_timestamp_microsecond_array(array)?;
                if let Some(tz) = array.timezone() {
                    return exec_err!("expected no time zone but got {tz}");
                }

                let it = array.into_iter().map(move |maybe_val| {
                    maybe_val
                        .map(|val| {
                            let dt = DateTime::from_timestamp_micros(val).ok_or_else(|| exec_datafusion_err!("cannot create DateTime object from microsecond timestamp: {val}"))?;

                            PyDateTime::new(
                                py,
                                dt.year(),
                                dt
                                    .month()
                                    .try_into()
                                    .map_err(|e| exec_datafusion_err!("month out of range: {e}"))?,
                                dt
                                    .day()
                                    .try_into()
                                    .map_err(|e| exec_datafusion_err!("day out of range: {e}"))?,
                                dt
                                    .hour()
                                    .try_into()
                                    .map_err(|e| exec_datafusion_err!("hour out of range: {e}"))?,
                                dt
                                    .minute()
                                    .try_into()
                                    .map_err(|e| exec_datafusion_err!("minute out of range: {e}"))?,
                                dt
                                    .second()
                                    .try_into()
                                    .map_err(|e| exec_datafusion_err!("second out of range: {e}"))?,
                                dt.timestamp_subsec_micros(),
                                None,
                            ).map_err(|e| {
                                exec_datafusion_err!("cannot create PyDateTime: {e}")
                            })?.into_bound_py_any(py).map_err(|e| {
                                exec_datafusion_err!("cannot convert PyDateTime to any: {e}")
                            })
                        })
                        .transpose()
                });

                Ok(Box::new(it))
            }
            Self::Float => {
                let array = as_float64_array(array)?;

                let it = array.into_iter().map(move |maybe_val| {
                    maybe_val
                        .map(|val| {
                            val.into_bound_py_any(py).map_err(|e| {
                                exec_datafusion_err!(
                                    "cannot convert Rust `f64` value to Python: {e}"
                                )
                            })
                        })
                        .transpose()
                });

                Ok(Box::new(it))
            }
            Self::Int => {
                let array = as_int64_array(array)?;

                let it = array.into_iter().map(move |maybe_val| {
                    maybe_val
                        .map(|val| {
                            val.into_bound_py_any(py).map_err(|e| {
                                exec_datafusion_err!(
                                    "cannot convert Rust `i64` value to Python: {e}"
                                )
                            })
                        })
                        .transpose()
                });

                Ok(Box::new(it))
            }
            Self::Str => {
                let array = as_string_array(array)?;

                let it = array.into_iter().map(move |maybe_val| {
                    maybe_val
                        .map(|val| {
                            val.into_bound_py_any(py).map_err(|e| {
                                exec_datafusion_err!(
                                    "cannot convert Rust `str` value to Python: {e}"
                                )
                            })
                        })
                        .transpose()
                });

                Ok(Box::new(it))
            }
            Self::Bytes => {
                let array = as_binary_array(array)?;

                let it = array.into_iter().map(move |maybe_val| {
                    maybe_val
                        .map(|val| {
                            PyBytes::new(py, val).into_bound_py_any(py).map_err(|e| {
                                exec_datafusion_err!(
                                    "cannot convert Rust `&[u8]` value to Python bytes: {e}"
                                )
                            })
                        })
                        .transpose()
                });

                Ok(Box::new(it))
            }
            Self::Date => {
                let array = as_date32_array(array)?;

                let it = array.into_iter().map(move |maybe_val| {
                    maybe_val
                        .map(|val| {
                            let days_since_epoch = val;
                            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                                .ok_or_else(|| exec_datafusion_err!("cannot create epoch date"))?;
                            let date = epoch + chrono::Duration::days(days_since_epoch as i64);

                            PyDate::new(
                                py,
                                date.year(),
                                date.month()
                                    .try_into()
                                    .map_err(|e| exec_datafusion_err!("month out of range: {e}"))?,
                                date.day()
                                    .try_into()
                                    .map_err(|e| exec_datafusion_err!("day out of range: {e}"))?,
                            )
                            .map_err(|e| exec_datafusion_err!("cannot create PyDate: {e}"))?
                            .into_bound_py_any(py)
                            .map_err(|e| exec_datafusion_err!("cannot convert PyDate to any: {e}"))
                        })
                        .transpose()
                });

                Ok(Box::new(it))
            }
            Self::Time => {
                let array = as_time64_microsecond_array(array)?;

                let it = array.into_iter().map(move |maybe_val| {
                    maybe_val
                        .map(|val| {
                            let microseconds = val;
                            let total_seconds = microseconds / 1_000_000;
                            let remaining_microseconds = (microseconds % 1_000_000) as u32;

                            let hours = (total_seconds / 3600) as u8;
                            let minutes = ((total_seconds % 3600) / 60) as u8;
                            let seconds = (total_seconds % 60) as u8;

                            PyTime::new(py, hours, minutes, seconds, remaining_microseconds, None)
                                .map_err(|e| exec_datafusion_err!("cannot create PyTime: {e}"))?
                                .into_bound_py_any(py)
                                .map_err(|e| {
                                    exec_datafusion_err!("cannot convert PyTime to any: {e}")
                                })
                        })
                        .transpose()
                });

                Ok(Box::new(it))
            }
            Self::Timedelta => {
                let array = as_duration_microsecond_array(array)?;

                let it = array.into_iter().map(move |maybe_val| {
                    maybe_val
                        .map(|val| {
                            let microseconds = val;
                            let total_seconds = microseconds / 1_000_000;
                            let remaining_microseconds = microseconds % 1_000_000;

                            PyDelta::new(
                                py,
                                (total_seconds / 86400) as i32, // days
                                (total_seconds % 86400) as i32, // seconds
                                remaining_microseconds as i32,  // microseconds
                                false,
                            )
                            .map_err(|e| exec_datafusion_err!("cannot create PyDelta: {e}"))?
                            .into_bound_py_any(py)
                            .map_err(|e| exec_datafusion_err!("cannot convert PyDelta to any: {e}"))
                        })
                        .transpose()
                });

                Ok(Box::new(it))
            }
        }
    }

    /// Get a builder for the Arrow output [`Array`].
    ///
    /// This needs an "attached" [`Python`] to create Python objects.
    fn python_to_arrow<'py>(&self, num_rows: usize) -> Box<dyn ArrayBuilder<'py>> {
        match self {
            Self::Bool => Box::new(BooleanBuilder::with_capacity(num_rows)),
            Self::DateTime => Box::new(TimestampMicrosecondBuilder::with_capacity(num_rows)),
            Self::Float => Box::new(Float64Builder::with_capacity(num_rows)),
            Self::Int => Box::new(Int64Builder::with_capacity(num_rows)),
            Self::Str => Box::new(StringBuilder::with_capacity(num_rows, 1024)),
            Self::Bytes => Box::new(BinaryBuilder::with_capacity(num_rows, 1024)),
            Self::Date => Box::new(Date32Builder::with_capacity(num_rows)),
            Self::Time => Box::new(Time64MicrosecondBuilder::with_capacity(num_rows)),
            Self::Timedelta => Box::new(DurationMicrosecondBuilder::with_capacity(num_rows)),
        }
    }
}

impl PythonNullableType {
    /// Convert Arrow [`Array`] to python values.
    pub(crate) fn arrow_to_python<'a>(
        &self,
        array: &'a dyn Array,
        py: Python<'a>,
    ) -> DataFusionResult<PythonValueIter<'a>> {
        let it = self.t.arrow_to_python(array, py)?;

        if self.nullable {
            let none = PyNone::get(py)
                .into_bound_py_any(py)
                .map_err(|e| exec_datafusion_err!("cannot get None object: {e}"))?;

            let it = it.map(move |res| {
                let maybe_any = res?;
                Ok(ControlFlow::Continue(
                    maybe_any.unwrap_or_else(|| none.clone()),
                ))
            });
            Ok(Box::new(it))
        } else {
            let it = it.map(move |res| {
                let maybe_any = res?;
                Ok(maybe_any
                    .map(ControlFlow::Continue)
                    .unwrap_or(ControlFlow::Break(())))
            });
            Ok(Box::new(it))
        }
    }

    /// Get a builder for the Arrow output [`Array`].
    ///
    /// This needs an "attached" [`Python`] to create Python objects.
    pub(crate) fn python_to_arrow<'py>(
        &self,
        py: Python<'py>,
        num_rows: usize,
    ) -> Box<dyn ArrayBuilder<'py> + 'py> {
        let inner = self.t.python_to_arrow(num_rows);
        let none = PyNone::get(py).into_bound();
        Box::new(ArrayBuilderNullChecker {
            nullable: self.nullable,
            none,
            inner,
        })
    }
}

/// Abstract builder for Arrow output [`Array`].
pub(crate) trait ArrayBuilder<'py> {
    /// Push a new value.
    ///
    /// This may fail if the type doesn't match.
    fn push(&mut self, val: Bound<'py, PyAny>) -> DataFusionResult<()>;

    /// Skip this row, i.e. append a "null".
    fn skip(&mut self);

    /// Finish the conversation and create the output [`Array`].
    fn finish(&mut self) -> ArrayRef;
}

/// Output array builder that handles nullability reasoning.
///
/// This roughly corresponds to [`PythonNullableType`].
struct ArrayBuilderNullChecker<'py> {
    /// Did Python specify the type as nullable, i.e. are we expecting `None` values?
    nullable: bool,

    /// A handle to the Python VM `None` value.
    ///
    /// This is only stored here for faster conversions so we don't have to look it up every single time.
    none: Bound<'py, PyNone>,

    /// The type-specific converter that came out of [`PythonType::arrow_to_python`].
    inner: Box<dyn ArrayBuilder<'py>>,
}

impl<'py> ArrayBuilder<'py> for ArrayBuilderNullChecker<'py> {
    fn push(&mut self, val: Bound<'py, PyAny>) -> DataFusionResult<()> {
        match (self.nullable, val.is(&self.none)) {
            (false, true) => {
                exec_err!("method was not supposed to return None but did")
            }
            (false | true, false) => self.inner.push(val),
            (true, true) => {
                self.inner.skip();
                Ok(())
            }
        }
    }

    fn skip(&mut self) {
        self.inner.skip();
    }

    fn finish(&mut self) -> ArrayRef {
        self.inner.finish()
    }
}

impl<'py> ArrayBuilder<'py> for BooleanBuilder {
    fn push(&mut self, val: Bound<'py, PyAny>) -> DataFusionResult<()> {
        let val: bool = val.extract().map_err(|_| {
            exec_datafusion_err!("expected bool but got {}", py_representation(&val))
        })?;
        self.append_value(val);
        Ok(())
    }

    fn skip(&mut self) {
        self.append_null();
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<'py> ArrayBuilder<'py> for Float64Builder {
    fn push(&mut self, val: Bound<'py, PyAny>) -> DataFusionResult<()> {
        let val: f64 = val.extract().map_err(|_| {
            exec_datafusion_err!("expected `f64` but got {}", py_representation(&val))
        })?;
        self.append_value(val);
        Ok(())
    }

    fn skip(&mut self) {
        self.append_null();
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<'py> ArrayBuilder<'py> for Int64Builder {
    fn push(&mut self, val: Bound<'py, PyAny>) -> DataFusionResult<()> {
        // in Python, `bool` is a sub-class of int we should probably not silently cast bools to integers
        let val = val.cast_exact::<PyInt>().map_err(|_| {
            exec_datafusion_err!("expected `int` but got {}", py_representation(&val))
        })?;
        let val: i64 = val.extract().map_err(|_| {
            exec_datafusion_err!(
                "expected i64 but got {}, which is out-of-range",
                py_representation(val)
            )
        })?;
        self.append_value(val);
        Ok(())
    }

    fn skip(&mut self) {
        self.append_null();
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<'py> ArrayBuilder<'py> for StringBuilder {
    fn push(&mut self, val: Bound<'py, PyAny>) -> DataFusionResult<()> {
        let val: &str = val.extract().map_err(|_| {
            exec_datafusion_err!("expected `str` but got {}", py_representation(&val))
        })?;
        self.append_value(val);
        Ok(())
    }

    fn skip(&mut self) {
        self.append_null();
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<'py> ArrayBuilder<'py> for TimestampMicrosecondBuilder {
    fn push(&mut self, val: Bound<'py, PyAny>) -> DataFusionResult<()> {
        let val = val.cast_exact::<PyDateTime>().map_err(|_| {
            exec_datafusion_err!("expected `datetime` but got {}", py_representation(&val))
        })?;
        if let Some(tzinfo) = val.get_tzinfo() {
            let s = tzinfo
                .str()
                .and_then(|name| name.to_str().map(|s| s.to_owned()))
                .unwrap_or_else(|_| "<unknown>".to_owned());
            return exec_err!("expected no tzinfo, got {s}");
        }
        let val =
            NaiveDate::from_ymd_opt(val.get_year(), val.get_month().into(), val.get_day().into())
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "cannot create NaiveDate based on year-month-day of {}",
                        py_representation(val)
                    )
                })?
                .and_hms_micro_opt(
                    val.get_hour().into(),
                    val.get_minute().into(),
                    val.get_second().into(),
                    val.get_microsecond(),
                )
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "cannot create NaiveDateTime based on hour-minute-second-microsecond of {}",
                        py_representation(val)
                    )
                })?;
        let val = Utc.from_utc_datetime(&val);
        let val = val.timestamp_micros();
        self.append_value(val);
        Ok(())
    }

    fn skip(&mut self) {
        self.append_null();
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<'py> ArrayBuilder<'py> for BinaryBuilder {
    fn push(&mut self, val: Bound<'py, PyAny>) -> DataFusionResult<()> {
        let val = val.cast_exact::<PyBytes>().map_err(|_| {
            exec_datafusion_err!("expected `bytes` but got {}", py_representation(&val))
        })?;
        let val: &[u8] = val.extract().map_err(|_| {
            exec_datafusion_err!("cannot extract bytes from {}", py_representation(val))
        })?;
        self.append_value(val);
        Ok(())
    }

    fn skip(&mut self) {
        self.append_null();
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<'py> ArrayBuilder<'py> for Date32Builder {
    fn push(&mut self, val: Bound<'py, PyAny>) -> DataFusionResult<()> {
        let val = val.cast_exact::<PyDate>().map_err(|_| {
            exec_datafusion_err!("expected `date` but got {}", py_representation(&val))
        })?;

        let date =
            NaiveDate::from_ymd_opt(val.get_year(), val.get_month().into(), val.get_day().into())
                .ok_or_else(|| {
                exec_datafusion_err!(
                    "cannot create NaiveDate based on year-month-day of {}",
                    py_representation(val)
                )
            })?;

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
            .ok_or_else(|| exec_datafusion_err!("cannot create epoch date"))?;

        let days_since_epoch = date.signed_duration_since(epoch).num_days();
        let days_since_epoch_i32: i32 = days_since_epoch.try_into().map_err(|_| {
            exec_datafusion_err!(
                "date is out of range for Date32: {} days since epoch",
                days_since_epoch
            )
        })?;

        self.append_value(days_since_epoch_i32);
        Ok(())
    }

    fn skip(&mut self) {
        self.append_null();
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<'py> ArrayBuilder<'py> for Time64MicrosecondBuilder {
    fn push(&mut self, val: Bound<'py, PyAny>) -> DataFusionResult<()> {
        let val = val.cast_exact::<PyTime>().map_err(|_| {
            exec_datafusion_err!("expected `time` but got {}", py_representation(&val))
        })?;

        if let Some(tzinfo) = val.get_tzinfo() {
            let s = tzinfo
                .str()
                .and_then(|name| name.to_str().map(|s| s.to_owned()))
                .unwrap_or_else(|_| "<unknown>".to_owned());
            return exec_err!("expected no tzinfo, got {s}");
        }

        let hours = val.get_hour() as i64;
        let minutes = val.get_minute() as i64;
        let seconds = val.get_second() as i64;
        let microseconds = val.get_microsecond() as i64;

        let total_microseconds = hours * 3600 * 1_000_000
            + minutes * 60 * 1_000_000
            + seconds * 1_000_000
            + microseconds;

        self.append_value(total_microseconds);
        Ok(())
    }

    fn skip(&mut self) {
        self.append_null();
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<'py> ArrayBuilder<'py> for DurationMicrosecondBuilder {
    fn push(&mut self, val: Bound<'py, PyAny>) -> DataFusionResult<()> {
        let val = val.cast_exact::<PyDelta>().map_err(|_| {
            exec_datafusion_err!("expected `timedelta` but got {}", py_representation(&val))
        })?;

        // Extract the timedelta components using the standard methods
        let days: i64 = val
            .getattr("days")
            .map_err(|_| exec_datafusion_err!("cannot get days from timedelta"))?
            .extract()
            .map_err(|_| exec_datafusion_err!("cannot extract days as i64"))?;

        let seconds: i64 = val
            .getattr("seconds")
            .map_err(|_| exec_datafusion_err!("cannot get seconds from timedelta"))?
            .extract()
            .map_err(|_| exec_datafusion_err!("cannot extract seconds as i64"))?;

        let microseconds: i64 = val
            .getattr("microseconds")
            .map_err(|_| exec_datafusion_err!("cannot get microseconds from timedelta"))?
            .extract()
            .map_err(|_| exec_datafusion_err!("cannot extract microseconds as i64"))?;

        let total_microseconds = days * 86400 * 1_000_000 + seconds * 1_000_000 + microseconds;

        self.append_value(total_microseconds);
        Ok(())
    }

    fn skip(&mut self) {
        self.append_null();
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}
