use std::{sync::Arc, time::Duration};

use arrow::datatypes::{DataType, Field};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, async_udf::AsyncScalarUDFImpl};

use crate::integration_tests::evil::test_utils::try_scalar_udfs;

#[tokio::test]
async fn test_root() {
    let fut = try_scalar_udfs("spin::root");
    assert_timeout(fut).await;
}

#[tokio::test]
async fn test_udf_invoke() {
    let udfs = try_scalar_udfs("spin::udf_invoke").await.unwrap();
    assert_eq!(udfs.len(), 1);
    let udf = udfs.into_iter().next().unwrap();

    let fut = udf.invoke_async_with_args(ScalarFunctionArgs {
        args: vec![],
        arg_fields: vec![],
        number_rows: 1,
        return_field: Arc::new(Field::new("r", DataType::Null, true)),
        config_options: Arc::new(ConfigOptions::default()),
    });
    assert_timeout(fut).await;
}

#[tokio::test]
async fn test_udf_name() {
    let fut = try_scalar_udfs("spin::udf_name");
    assert_timeout(fut).await;
}

#[tokio::test]
async fn test_udf_signature() {
    let fut = try_scalar_udfs("spin::udf_signature");
    assert_timeout(fut).await;
}

#[tokio::test]
async fn test_udf_return_type_exact() {
    let fut = try_scalar_udfs("spin::udf_return_type_exact");
    assert_timeout(fut).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_udf_return_type_other() {
    let udfs = try_scalar_udfs("spin::udf_return_type_other")
        .await
        .unwrap();
    assert_eq!(udfs.len(), 1);
    let udf = udfs.into_iter().next().unwrap();

    let err = udf.return_type(&[]).unwrap_err();
    insta::assert_snapshot!(
        err,
        @"External error: deadline has elapsed",
    );
}

#[tokio::test]
async fn test_udfs() {
    let fut = try_scalar_udfs("spin::udfs");
    assert_timeout(fut).await;
}

/// Ensure that the future times out correctly.
///
/// There are two things that could happen instead:
///
/// - **future completes:** In that case the evil payload probably doesn't spin at all or not correctly.
/// - **hang:** The test just hangs forever. In that case the future does not correctly yield back to the tokio
///   runtime, either because it is actually pinning the CPU or because it is waiting for something (i.e. thread is
///   suspended). That would be a legitimate bug since the evil payload should not be able to force the host into a
///   hanging state.
async fn assert_timeout<Fut>(fut: Fut)
where
    Fut: Future,
    Fut::Output: std::fmt::Debug,
{
    tokio::time::timeout(Duration::from_millis(500), fut)
        .await
        .unwrap_err();
}
