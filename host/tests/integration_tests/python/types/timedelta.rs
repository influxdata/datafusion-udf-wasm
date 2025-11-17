use std::sync::Arc;

use arrow::{
    array::DurationMicrosecondArray,
    datatypes::{DataType, Field, TimeUnit},
};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    async_udf::AsyncScalarUDFImpl,
};

use crate::integration_tests::{
    python::test_utils::python_scalar_udf, test_utils::ColumnarValueExt,
};

#[tokio::test]
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
        .invoke_async_with_args(ScalarFunctionArgs {
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
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
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

#[tokio::test]
async fn test_negative_duration() {
    const CODE: &str = "
from datetime import timedelta

def foo(x: timedelta) -> timedelta:
    return timedelta(days=-1, hours=-2, minutes=-30)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let array = udf
        .invoke_async_with_args(ScalarFunctionArgs {
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
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();

    // -1 day - 2 hours - 30 minutes = -26.5 hours = -95400 seconds = -95400000000 microseconds
    let expected_microseconds = -95400 * 1_000_000i64;
    assert_eq!(
        array.as_ref(),
        &DurationMicrosecondArray::from_iter([Some(expected_microseconds)]),
    );
}

#[tokio::test]
async fn test_return_datetime() {
    const CODE: &str = "
from datetime import timedelta, datetime

def foo(x: timedelta) -> timedelta:
    return datetime.now()
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_async_with_args(ScalarFunctionArgs {
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
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap_err();

    assert!(err.to_string().contains("expected `timedelta` but got"));
    assert!(err.to_string().contains("of type `datetime`"));
}

#[tokio::test]
async fn test_return_int() {
    const CODE: &str = "
from datetime import timedelta

def foo(x: timedelta) -> timedelta:
    return 42
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_async_with_args(ScalarFunctionArgs {
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
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected `timedelta` but got `42` of type `int`",
    );
}
