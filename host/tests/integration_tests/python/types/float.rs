use std::sync::Arc;

use arrow::{
    array::{Array, Float64Array},
    datatypes::{DataType, Field},
};
use datafusion_common::cast::as_float64_array;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::integration_tests::{
    python::test_utils::python_scalar_udf, test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_roundtrip() {
    const CODE: &str = "
def foo(x: float) -> float:
    return x
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(vec![DataType::Float64], Volatility::Volatile),
    );

    assert_eq!(
        udf.return_type(&[DataType::Float64]).unwrap(),
        DataType::Float64,
    );

    let values = &[
        Some(13.37),
        Some(f64::NAN),
        Some(f64::NEG_INFINITY),
        Some(f64::MIN_POSITIVE),
        Some(f64::INFINITY),
        Some(f64::MAX),
        Some(f64::MIN),
        Some(0.0),
        Some(-0.0),
    ];

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Float64Array::from_iter(
                values.iter().copied(),
            )))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Float64, true))],
            number_rows: values.len(),
            return_field: Arc::new(Field::new("r", DataType::Float64, true)),
        })
        .unwrap()
        .unwrap_array();
    assert_float_total_eq(&array, values);
}

#[test]
#[should_panic(expected = "Not equal")]
fn test_assert_float_total_eq_uses_total_eq() {
    let array = Float64Array::from_iter(&[Some(-0.0)]);
    assert_float_total_eq(&array, &[Some(0.0)]);
}

#[track_caller]
fn assert_float_total_eq(array: &dyn Array, expected: &[Option<f64>]) {
    assert_eq!(array.len(), expected.len());

    let array = as_float64_array(array).unwrap();
    let actual = array.into_iter().collect::<Vec<_>>();

    let is_eq = actual
        .iter()
        .zip(expected)
        .map(|(actual, expected)| match (actual, expected) {
            (None, None) => true,
            (Some(actual), Some(expected)) => actual.total_cmp(expected).is_eq(),
            _ => false,
        })
        .collect::<Vec<_>>();

    if !is_eq.iter().all(|eq| *eq) {
        panic!("Not equal:\n\nActual:\n{actual:?}\n\nExpected:\n{expected:?}\n\nEq:\n{is_eq:?}");
    }
}
