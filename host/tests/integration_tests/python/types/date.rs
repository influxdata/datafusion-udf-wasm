use std::sync::Arc;

use arrow::{
    array::Date32Array,
    datatypes::{DataType, Field},
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::integration_tests::{
    python::test_utils::python_scalar_udf, test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_ok() {
    const CODE: &str = "
from datetime import date, timedelta

def foo(x: date) -> date:
    return x + timedelta(days=1)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(vec![DataType::Date32], Volatility::Volatile),
    );

    assert_eq!(
        udf.return_type(&[DataType::Date32]).unwrap(),
        DataType::Date32,
    );

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Date32Array::from_iter([
                Some(0), // 1970-01-01
                None,
                Some(19000), // 2022-01-04
                Some(-365),  // 1968-12-31
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Date32, true))],
            number_rows: 4,
            return_field: Arc::new(Field::new("r", DataType::Date32, true)),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Date32Array::from_iter([
            Some(1), // 1970-01-02
            None,
            Some(19001), // 2022-01-05
            Some(-364),  // 1969-01-01
        ]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_datetime() {
    const CODE: &str = "
from datetime import date, datetime

def foo(x: date) -> date:
    return datetime.now()
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Date32Array::from_iter([
                Some(0),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Date32, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Date32, true)),
        })
        .unwrap_err();
    // The error message will contain the actual datetime, so we just check it contains the expected type info
    assert!(err.to_string().contains("expected `date` but got"));
    assert!(err.to_string().contains("of type `datetime`"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_str() {
    const CODE: &str = "
from datetime import date

def foo(x: date) -> date:
    return 'not a date'
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Date32Array::from_iter([
                Some(0),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Date32, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Date32, true)),
        })
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected `date` but got `not a date` of type `str`",
    );
}
