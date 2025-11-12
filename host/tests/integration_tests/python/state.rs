//! Tests statefulness of the Python VM.
//!
//! The examples in this module are NOT best practice, but only a demonstration. Usually state shouldn't be used to
//! generate outputs but for caching and pre-computation.

use std::sync::Arc;

use crate::integration_tests::python::test_utils::python_scalar_udfs;
use arrow::{
    array::{Array, ArrayRef, Int64Array},
    datatypes::{DataType, Field},
};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ScalarFunctionArgs, async_udf::AsyncScalarUDFImpl};

const CODE: &str = "
# Use system module to store our state.
#
# We could use a global variable as well, but using a system module we
# can also check how an interpreter/VM-wide state behaves.
import os

def _init_state() -> None:
    try:
        getattr(os, '_state')
    except AttributeError:
        os._state = 0

def f1() -> int:
    _init_state()
    os._state += 1
    return os._state

def f2() -> int:
    _init_state()
    os._state += 10
    return -os._state
";

#[tokio::test]
async fn test_cross_batches() {
    let [f1, _f2] = udfs().await;
    assert_eq!(
        call(&f1).await.as_ref(),
        &Int64Array::from_iter([Some(1), Some(2), Some(3)]) as &dyn Array,
    );
    assert_eq!(
        call(&f1).await.as_ref(),
        &Int64Array::from_iter([Some(4), Some(5), Some(6)]) as &dyn Array,
    );
}

#[tokio::test]
async fn test_cross_functions() {
    let [f1, f2] = udfs().await;
    assert_eq!(
        call(&f1).await.as_ref(),
        &Int64Array::from_iter([Some(1), Some(2), Some(3)]) as &dyn Array,
    );
    assert_eq!(
        call(&f2).await.as_ref(),
        &Int64Array::from_iter([Some(-13), Some(-23), Some(-33)]) as &dyn Array,
    );
    assert_eq!(
        call(&f1).await.as_ref(),
        &Int64Array::from_iter([Some(34), Some(35), Some(36)]) as &dyn Array,
    );
}

/// Ensure that:
///
/// - the pre-compilation does not set up state
/// - two instances of the same UDF do NOT share state
#[tokio::test]
async fn test_precompile_is_stateless() {
    let [f1_a, _f2_a] = udfs().await;
    assert_eq!(
        call(&f1_a).await.as_ref(),
        &Int64Array::from_iter([Some(1), Some(2), Some(3)]) as &dyn Array,
    );

    let [f1_b, _f2_b] = udfs().await;
    assert_eq!(
        call(&f1_b).await.as_ref(),
        &Int64Array::from_iter([Some(1), Some(2), Some(3)]) as &dyn Array,
    );

    assert_eq!(
        call(&f1_a).await.as_ref(),
        &Int64Array::from_iter([Some(4), Some(5), Some(6)]) as &dyn Array,
    );
}

async fn udfs() -> [impl AsyncScalarUDFImpl; 2] {
    python_scalar_udfs(CODE).await.unwrap().try_into().unwrap()
}

async fn call(udf: &impl AsyncScalarUDFImpl) -> ArrayRef {
    udf.invoke_async_with_args(
        ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        },
        &ConfigOptions::default(),
    )
    .await
    .unwrap()
}
