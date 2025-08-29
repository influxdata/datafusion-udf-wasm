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

pub(crate) type PythonOptValueIter<'a> =
    Box<dyn Iterator<Item = DataFusionResult<Option<Bound<'a, PyAny>>>> + 'a>;

pub(crate) type PythonValueIter<'a> =
    Box<dyn Iterator<Item = DataFusionResult<ControlFlow<(), Bound<'a, PyAny>>>> + 'a>;

impl PythonType {
    pub(crate) fn data_type(&self) -> DataType {
        match self {
            Self::Int => DataType::Int64,
        }
    }

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

                let mut idx = 0;
                let it = std::iter::from_fn(move || {
                    if idx >= array.len() {
                        None
                    } else {
                        let val = if array.is_null(idx) {
                            Ok(None)
                        } else {
                            let val = array.value(idx);
                            match val.into_bound_py_any(py) {
                                Ok(val) => Ok(Some(val)),
                                Err(e) => {
                                    Err(exec_datafusion_err!("cannot convert value to python: {e}"))
                                }
                            }
                        };
                        idx += 1;
                        Some(val)
                    }
                });
                Ok(Box::new(it))
            }
        }
    }

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

pub(crate) trait ArrayBuilder<'py> {
    type T;

    fn push(&mut self, val: Self::T) -> DataFusionResult<()>;

    fn skip(&mut self);

    fn finish(&mut self) -> ArrayRef;
}

struct ArrayBuilderNullChecker<'py> {
    nullable: bool,
    none: Bound<'py, PyNone>,
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
