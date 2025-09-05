//! Conversion routes from [`arrow`] to/from Python.
use std::{ops::ControlFlow, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, Int64Array, Int64Builder},
    datatypes::DataType,
};
use datafusion_common::{error::Result as DataFusionResult, exec_datafusion_err, exec_err};
use pyo3::{
    Bound, BoundObject, IntoPyObjectExt, PyAny, Python,
    types::{PyAnyMethods, PyNone},
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
            Self::Int => DataType::Int64,
        }
    }

    /// Convert arrow [`Array`] to iterator of optional Python values.
    fn arrow_to_python<'a>(
        &self,
        array: &'a dyn Array,
        py: Python<'a>,
    ) -> DataFusionResult<PythonOptValueIter<'a>> {
        match self {
            Self::Int => {
                let array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    exec_datafusion_err!("expected int64 array but got {}", array.data_type())
                })?;

                let it = (0..array.len()).map(move |idx| {
                    if array.is_null(idx) {
                        Ok(None)
                    } else {
                        let val = array.value(idx);
                        match val.into_bound_py_any(py) {
                            Ok(val) => Ok(Some(val)),
                            Err(e) => {
                                Err(exec_datafusion_err!("cannot convert value to python: {e}"))
                            }
                        }
                    }
                });
                Ok(Box::new(it))
            }
        }
    }

    /// Get a builder for the Arrow output [`Array`].
    fn python_to_arrow<'py>(
        &self,
        num_rows: usize,
    ) -> Box<dyn ArrayBuilder<'py, T = Option<Bound<'py, PyAny>>>> {
        match self {
            Self::Int => Box::new(Int64Builder::with_capacity(num_rows)),
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
    pub(crate) fn python_to_arrow<'py>(
        &self,
        py: Python<'py>,
        num_rows: usize,
    ) -> Box<dyn ArrayBuilder<'py, T = Bound<'py, PyAny>> + 'py> {
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
    /// Types that can be converted into the respective [`Array`] value.
    ///
    /// This is either `Bound<'py, PyAny>` (if called directly using the Python value) or `Option<Bound<'py, PyAny>>`
    /// (if we already reasoned about nullability).
    type T;

    /// Push a new value.
    ///
    /// This may fail if the type doesn't match.
    fn push(&mut self, val: Self::T) -> DataFusionResult<()>;

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
    inner: Box<dyn ArrayBuilder<'py, T = Option<Bound<'py, PyAny>>>>,
}

impl<'py> ArrayBuilder<'py> for ArrayBuilderNullChecker<'py> {
    type T = Bound<'py, PyAny>;

    fn push(&mut self, val: Self::T) -> DataFusionResult<()> {
        let val = if val.is(&self.none) { None } else { Some(val) };
        if !self.nullable && val.is_none() {
            return exec_err!("method was not supposed to return None but did");
        }
        self.inner.push(val)
    }

    fn skip(&mut self) {
        self.inner.skip();
    }

    fn finish(&mut self) -> ArrayRef {
        self.inner.finish()
    }
}

impl<'py> ArrayBuilder<'py> for Int64Builder {
    type T = Option<Bound<'py, PyAny>>;

    fn push(&mut self, val: Self::T) -> DataFusionResult<()> {
        match val {
            Some(val) => {
                let val: i64 = val.extract().map_err(|_| {
                    exec_datafusion_err!("expected i64 but got {}", py_representation(&val))
                })?;
                self.append_value(val);
                Ok(())
            }
            None => {
                self.append_null();
                Ok(())
            }
        }
    }

    fn skip(&mut self) {
        self.append_null();
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}
