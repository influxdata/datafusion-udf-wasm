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
            arg_fields: vec![Arc::new(Field::new("time", DataType::Date32, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Date32, true)),
        })
        .unwrap_err();

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

#[tokio::test(flavor = "multi_thread")]
async fn test_date_arithmetic_overflow() {
    const CODE: &str = "
from datetime import date, timedelta

def foo(x: date) -> date:
    try:
        return x + timedelta(days=1000000)
    except OverflowError:
        return date(9999, 12, 31)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Date32Array::from_iter([
                Some(0),     // 1970-01-01
                Some(18000), // Some date in 2019
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Date32, true))],
            number_rows: 2,
            return_field: Arc::new(Field::new("r", DataType::Date32, true)),
        })
        .unwrap()
        .unwrap_array();

    assert_eq!(array.len(), 2);
    assert!(array.is_valid(0));
    assert!(array.is_valid(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_date_string_formatting() {
    const CODE: &str = "
from datetime import date

def foo(x: date) -> date:
    date_str = x.strftime('%Y-%m-%d')
    year, month, day = map(int, date_str.split('-'))
    return date(year, month, day)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Date32Array::from_iter([
                Some(0),     // 1970-01-01
                Some(365),   // 1971-01-01
                Some(19000), // 2022-01-04
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Date32, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Date32, true)),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Date32Array::from_iter([
            Some(0), // Should be unchanged
            Some(365),
            Some(19000),
        ]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_date_weekday_operations() {
    const CODE: &str = "
from datetime import date, timedelta

def foo(x: date) -> date:
    days_until_monday = (7 - x.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    return x + timedelta(days=days_until_monday)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Date32Array::from_iter([
                Some(0), // 1970-01-01 (Thursday)
                Some(3), // 1970-01-04 (Sunday)
                Some(4), // 1970-01-05 (Monday)
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Date32, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Date32, true)),
        })
        .unwrap()
        .unwrap_array();

    assert_eq!(array.len(), 3);
    assert!(array.is_valid(0));
    assert!(array.is_valid(1));
    assert!(array.is_valid(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_invalid_date_construction() {
    const CODE: &str = "
from datetime import date

def foo(x: date) -> date:
    # Try to create invalid date
    try:
        return date(2021, 13, 32)
    except ValueError:
        return x  # Return original on error
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Date32Array::from_iter([
                Some(19000),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Date32, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Date32, true)),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(array.as_ref(), &Date32Array::from_iter([Some(19000)]),);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_float() {
    const CODE: &str = "
from datetime import date

def foo(x: date) -> date:
    return 3.14
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
        @"Execution error: expected `date` but got `3.14` of type `float`",
    );
}
