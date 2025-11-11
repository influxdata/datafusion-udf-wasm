use std::sync::Arc;

use arrow::{
    array::TimestampMicrosecondArray,
    datatypes::{DataType, Field, TimeUnit},
};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    async_udf::AsyncScalarUDFImpl,
};

use crate::integration_tests::python::test_utils::python_scalar_udf;

#[tokio::test(flavor = "multi_thread")]
async fn test_ok() {
    const CODE: &str = "
from datetime import datetime, timedelta

def foo(x: datetime) -> datetime:
    return x + timedelta(days=1)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(
            vec![DataType::Timestamp(TimeUnit::Microsecond, None)],
            Volatility::Volatile
        ),
    );

    assert_eq!(
        udf.return_type(&[DataType::Timestamp(TimeUnit::Microsecond, None)])
            .unwrap(),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );

    let array = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(
                    TimestampMicrosecondArray::from_iter([
                        Some(1),
                        None,
                        Some(1_757_520_791_123_456),
                        Some(-100_000_000_000),
                    ]),
                ))],
                arg_fields: vec![Arc::new(Field::new(
                    "a1",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ))],
                number_rows: 4,
                return_field: Arc::new(Field::new(
                    "r",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        array.as_ref(),
        &TimestampMicrosecondArray::from_iter([
            Some(86400000001),
            None,
            Some(1757607191123456),
            Some(-13600000000)
        ]),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pass_array_with_tz() {
    const CODE: &str = "
from datetime import datetime, timedelta

def foo(x: datetime) -> datetime:
    return x + timedelta(days=1)
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(
                    TimestampMicrosecondArray::from_iter([Some(1)]).with_timezone("Europe/Berlin"),
                ))],
                arg_fields: vec![Arc::new(Field::new(
                    "a1",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ))],
                number_rows: 1,
                return_field: Arc::new(Field::new(
                    "r",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected no time zone but got Europe/Berlin",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_with_tz() {
    const CODE: &str = "
from datetime import datetime, timedelta, timezone

def foo(x: datetime) -> datetime:
    return x.astimezone(timezone(timedelta(hours=1)))
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(
                    TimestampMicrosecondArray::from_iter([Some(1)]),
                ))],
                arg_fields: vec![Arc::new(Field::new(
                    "a1",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ))],
                number_rows: 1,
                return_field: Arc::new(Field::new(
                    "r",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected no tzinfo, got UTC+01:00",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_int() {
    const CODE: &str = "
from datetime import datetime

def foo(x: datetime) -> datetime:
    return 1
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(
                    TimestampMicrosecondArray::from_iter([Some(1)]),
                ))],
                arg_fields: vec![Arc::new(Field::new(
                    "a1",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ))],
                number_rows: 1,
                return_field: Arc::new(Field::new(
                    "r",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected `datetime` but got `1` of type `int`",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_return_date() {
    const CODE: &str = "
from datetime import datetime

def foo(x: datetime) -> datetime:
    return x.date()
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    let err = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(
                    TimestampMicrosecondArray::from_iter([Some(1)]),
                ))],
                arg_fields: vec![Arc::new(Field::new(
                    "a1",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ))],
                number_rows: 1,
                return_field: Arc::new(Field::new(
                    "r",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Execution error: expected `datetime` but got `1970-01-01` of type `date`",
    );
}
