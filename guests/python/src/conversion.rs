//! Conversion routes from [`arrow`] to/from Python.
use std::{ops::ControlFlow, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, BooleanBuilder, Float64Builder, Int64Builder},
    datatypes::DataType,
};
use datafusion_common::{
    cast::{as_boolean_array, as_float64_array, as_int64_array},
    error::Result as DataFusionResult,
    exec_datafusion_err, exec_err,
};
use pyo3::{
    Bound, BoundObject, IntoPyObjectExt, PyAny, Python,
    types::{PyAnyMethods, PyInt, PyNone},
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
            Self::Float => DataType::Float64,
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
        }
    }

    /// Get a builder for the Arrow output [`Array`].
    ///
    /// This needs an "attached" [`Python`] to create Python objects.
    fn python_to_arrow<'py>(&self, num_rows: usize) -> Box<dyn ArrayBuilder<'py>> {
        match self {
            Self::Bool => Box::new(BooleanBuilder::with_capacity(num_rows)),
            Self::Float => Box::new(Float64Builder::with_capacity(num_rows)),
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
        let val = val.downcast_exact::<PyInt>().map_err(|_| {
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
