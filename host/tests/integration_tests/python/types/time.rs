use std::sync::Arc;

use arrow::{
    array::Time64MicrosecondArray,
    datatypes::{DataType, Field, TimeUnit},
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::integration_tests::{
    python::test_utils::python_scalar_udf, test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_ok() {
    const CODE: &str = "
from datetime import time, timedelta

def foo(x: time) -> time:
    # Add 1 hour to the time
    total_seconds = x.hour * 3600 + x.minute * 60 + x.second
    total_microseconds = total_seconds * 1000000 + x.microsecond
    new_microseconds = total_microseconds + 3600 * 1000000  # Add 1 hour
    
    # Convert back to time components
    new_total_seconds = new_microseconds // 1000000
    new_microsecond = new_microseconds % 1000000
    
    new_hour = (new_total_seconds // 3600) % 24
    new_minute = (new_total_seconds % 3600) // 60
    new_second = new_total_seconds % 60
    
    return time(new_hour, new_minute, new_second, new_microsecond)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(
            vec![DataType::Time64(TimeUnit::Microsecond)],
            Volatility::Volatile
        ),
    );

    assert_eq!(
        udf.return_type(&[DataType::Time64(TimeUnit::Microsecond)])
            .unwrap(),
        DataType::Time64(TimeUnit::Microsecond),
    );

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                Time64MicrosecondArray::from_iter([
                    Some(0), // 00:00:00.000000
                    None,
                    Some(12 * 3600 * 1_000_000), // 12:00:00.000000
                    Some(23 * 3600 * 1_000_000 + 59 * 60 * 1_000_000 + 59 * 1_000_000 + 999_999), // 23:59:59.999999
                ]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 4,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Time64MicrosecondArray::from_iter([
            Some(3600 * 1_000_000), // 01:00:00.000000
            None,
            Some(13 * 3600 * 1_000_000), // 13:00:00.000000
            Some(59 * 60 * 1_000_000 + 59 * 1_000_000 + 999_999), // 00:59:59.999999 (wrapped around)
        ]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_with_timezone() {
    const CODE: &str = "
from datetime import time, timezone, timedelta

def foo(x: time) -> time:
    return x.replace(tzinfo=timezone(timedelta(hours=1)))
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                Time64MicrosecondArray::from_iter([Some(0)]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 1,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected no tzinfo, got UTC+01:00",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_datetime() {
    const CODE: &str = "
from datetime import time, datetime

def foo(x: time) -> time:
    return datetime.now()
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                Time64MicrosecondArray::from_iter([Some(0)]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 1,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap_err();
    // The error message will contain the actual datetime, so we just check it contains the expected type info
    assert!(err.to_string().contains("expected `time` but got"));
    assert!(err.to_string().contains("of type `datetime`"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_microsecond_precision() {
    const CODE: &str = "
from datetime import time

def foo(x: time) -> time:
    new_microsecond = (x.microsecond + 1) % 1000000
    return x.replace(microsecond=new_microsecond)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                Time64MicrosecondArray::from_iter([
                    Some(999_999),                         // 00:00:00.999999
                    Some(12 * 3600 * 1_000_000 + 500_000), // 12:00:00.500000
                ]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 2,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Time64MicrosecondArray::from_iter([
            Some(0),                               // 00:00:00.000000 (999999 + 1 wrapped to 0)
            Some(12 * 3600 * 1_000_000 + 500_001), // 12:00:00.500001
        ]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_arithmetic_with_overflow() {
    const CODE: &str = "
from datetime import time

def foo(x: time) -> time:
    total_seconds = x.hour * 3600 + x.minute * 60 + x.second
    total_microseconds = total_seconds * 1000000 + x.microsecond
    new_microseconds = (total_microseconds + 2 * 3600 * 1000000) % (24 * 3600 * 1000000)
    
    new_total_seconds = new_microseconds // 1000000
    new_microsecond = new_microseconds % 1000000
    
    new_hour = (new_total_seconds // 3600) % 24
    new_minute = (new_total_seconds % 3600) // 60
    new_second = new_total_seconds % 60
    
    return time(new_hour, new_minute, new_second, new_microsecond)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                Time64MicrosecondArray::from_iter([
                    Some(22 * 3600 * 1_000_000),                       // 22:00:00.000000
                    Some(23 * 3600 * 1_000_000 + 30 * 60 * 1_000_000), // 23:30:00.000000
                ]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 2,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Time64MicrosecondArray::from_iter([
            Some(0),                                      // 00:00:00.000000 (22 + 2 = 24, wrapped to 0)
            Some(3600 * 1_000_000 + 30 * 60 * 1_000_000), // 01:30:00.000000 (23:30 + 2h = 01:30)
        ]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_string_parsing() {
    const CODE: &str = "
from datetime import time

def foo(x: time) -> time:
    time_str = x.strftime('%H:%M:%S.%f')
    hour, minute, second_micro = time_str.split(':')
    second, microsecond = second_micro.split('.')
    return time(int(hour), int(minute), int(second), int(microsecond))
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                Time64MicrosecondArray::from_iter([Some(
                    14 * 3600 * 1_000_000 + 25 * 60 * 1_000_000 + 30 * 1_000_000 + 123_456,
                )]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 1,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Time64MicrosecondArray::from_iter([Some(
            14 * 3600 * 1_000_000 + 25 * 60 * 1_000_000 + 30 * 1_000_000 + 123_456
        ),]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_invalid_time_construction() {
    const CODE: &str = "
from datetime import time

def foo(x: time) -> time:
    try:
        return time(25, 70, 70)
    except ValueError:
        return x
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                Time64MicrosecondArray::from_iter([Some(12 * 3600 * 1_000_000)]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 1,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Time64MicrosecondArray::from_iter([Some(12 * 3600 * 1_000_000)]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_timedelta() {
    const CODE: &str = "
from datetime import time, timedelta

def foo(x: time) -> time:
    return timedelta(hours=1)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                Time64MicrosecondArray::from_iter([Some(0)]),
            ))],
            arg_fields: vec![Arc::new(Field::new(
                "a1",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            ))],
            number_rows: 1,
            return_field: Arc::new(Field::new(
                "r",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            )),
        })
        .unwrap_err();
    assert!(err.to_string().contains("expected `time` but got"));
    assert!(err.to_string().contains("of type `timedelta`"));
}
