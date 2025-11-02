use std::sync::Arc;

use arrow::{
    array::DurationMicrosecondArray,
    datatypes::{DataType, Field, TimeUnit},
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::integration_tests::{
    python::test_utils::python_scalar_udf, test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_ok() {
    const CODE: &str = "
from datetime import timedelta

def foo(x: timedelta) -> timedelta:
    return x + timedelta(hours=1)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(
            vec![DataType::Duration(TimeUnit::Microsecond)],
            Volatility::Volatile
        ),
    );

    assert_eq!(
        udf.return_type(&[DataType::Duration(TimeUnit::Microsecond)])
            .unwrap(),
        DataType::Duration(TimeUnit::Microsecond),
    );

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                DurationMicrosecondArray::from_iter([
                    Some(0), // 0 microseconds
                    None,
                    Some(1_000_000),             // 1 second
                    Some(24 * 3600 * 1_000_000), // 1 day
                    Some(-3600 * 1_000_000),     // -1 hour
                ]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 5,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &DurationMicrosecondArray::from_iter([
            Some(3600 * 1_000_000), // 1 hour
            None,
            Some(3601 * 1_000_000),      // 1 hour 1 second
            Some(25 * 3600 * 1_000_000), // 1 day 1 hour
            Some(0),                     // 0 (was -1 hour + 1 hour)
        ]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_negative_duration() {
    const CODE: &str = "
from datetime import timedelta

def foo(x: timedelta) -> timedelta:
    return timedelta(days=-1, hours=-2, minutes=-30)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                DurationMicrosecondArray::from_iter([Some(0)]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 1,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap()
        .unwrap_array();

    // -1 day - 2 hours - 30 minutes = -26.5 hours = -95400 seconds = -95400000000 microseconds
    let expected_microseconds = -95400 * 1_000_000i64;
    assert_eq!(
        array.as_ref(),
        &DurationMicrosecondArray::from_iter([Some(expected_microseconds)]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_datetime() {
    const CODE: &str = "
from datetime import timedelta, datetime

def foo(x: timedelta) -> timedelta:
    return datetime.now()
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                DurationMicrosecondArray::from_iter([Some(0)]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 1,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap_err();
    // The error message will contain the actual datetime, so we just check it contains the expected type info
    assert!(err.to_string().contains("expected `timedelta` but got"));
    assert!(err.to_string().contains("of type `datetime`"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_int() {
    const CODE: &str = "
from datetime import timedelta

def foo(x: timedelta) -> timedelta:
    return 42
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                DurationMicrosecondArray::from_iter([Some(0)]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 1,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected `timedelta` but got `42` of type `int`",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_timedelta_fractional_seconds() {
    const CODE: &str = "
from datetime import timedelta

def foo(x: timedelta) -> timedelta:
    total_seconds = x.total_seconds()
    new_seconds = total_seconds + 0.5
    return timedelta(seconds=new_seconds)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                DurationMicrosecondArray::from_iter([
                    Some(1_500_000),  // 1.5 seconds
                    Some(0),          // 0 seconds
                    Some(-2_000_000), // -2 seconds
                ]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 3,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &DurationMicrosecondArray::from_iter([
            Some(2_000_000),  // 2.0 seconds (1.5 + 0.5)
            Some(500_000),    // 0.5 seconds (0 + 0.5)
            Some(-1_500_000), // -1.5 seconds (-2 + 0.5)
        ]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_timedelta_arithmetic_operations() {
    const CODE: &str = "
from datetime import timedelta

def foo(x: timedelta) -> timedelta:
    one_hour = timedelta(hours=1)
    one_day = timedelta(days=1)
    
    result = x * 2 + one_hour - one_day
    return result
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                DurationMicrosecondArray::from_iter([
                    Some(3600 * 1_000_000),      // 1 hour
                    Some(12 * 3600 * 1_000_000), // 12 hours
                    Some(-1800 * 1_000_000),     // -30 minutes
                ]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 3,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap()
        .unwrap_array();

    // Expected: x*2 + 1hour - 1day
    // For 1 hour: 2*1h + 1h - 24h = -21h = -75600 seconds
    // For 12 hours: 2*12h + 1h - 24h = 1h = 3600 seconds
    // For -30 min: 2*(-30min) + 1h - 24h = -1h - 24h = -25h = -90000 seconds
    assert_eq!(
        array.as_ref(),
        &DurationMicrosecondArray::from_iter([
            Some(-75600 * 1_000_000), // -21 hours
            Some(3600 * 1_000_000),   // 1 hour
            Some(-90000 * 1_000_000), // -25 hours
        ]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_timedelta_division_operations() {
    const CODE: &str = "
from datetime import timedelta

def foo(x: timedelta) -> timedelta:
    if x.total_seconds() != 0:
        return x / 2
    else:
        return timedelta(seconds=1)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                DurationMicrosecondArray::from_iter([
                    Some(3600 * 1_000_000),  // 1 hour
                    Some(0),                 // 0 seconds
                    Some(-7200 * 1_000_000), // -2 hours
                ]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 3,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &DurationMicrosecondArray::from_iter([
            Some(1800 * 1_000_000),  // 30 minutes (1h / 2)
            Some(1_000_000),         // 1 second (0 -> 1s)
            Some(-3600 * 1_000_000), // -1 hour (-2h / 2)
        ]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_timedelta_string_representation() {
    const CODE: &str = "
from datetime import timedelta

def foo(x: timedelta) -> timedelta:
    str_repr = str(x)
    if 'day' in str_repr:
        return timedelta(days=1)
    else:
        return x * 2
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                DurationMicrosecondArray::from_iter([
                    Some(3600 * 1_000_000),       // 1 hour
                    Some(25 * 3600 * 1_000_000),  // 25 hours (> 1 day)
                    Some(-12 * 3600 * 1_000_000), // -12 hours
                ]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 3,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap()
        .unwrap_array();

    assert_eq!(array.len(), 3);
    assert!(array.is_valid(0));
    assert!(array.is_valid(1));
    assert!(array.is_valid(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_timedelta_comparison_operations() {
    const CODE: &str = "
from datetime import timedelta

def foo(x: timedelta) -> timedelta:
    zero = timedelta(0)
    one_hour = timedelta(hours=1)
    
    if x > one_hour:
        return timedelta(hours=24)
    elif x < zero:
        return timedelta(0)
    else:
        return one_hour
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                DurationMicrosecondArray::from_iter([
                    Some(7200 * 1_000_000),  // 2 hours (> 1 hour)
                    Some(-1800 * 1_000_000), // -30 minutes (< 0)
                    Some(1800 * 1_000_000),  // 30 minutes (0 < x < 1 hour)
                ]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 3,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &DurationMicrosecondArray::from_iter([
            Some(24 * 3600 * 1_000_000), // 24 hours
            Some(0),                     // 0
            Some(3600 * 1_000_000),      // 1 hour
        ]),
    );
}
