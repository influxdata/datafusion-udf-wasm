use std::time::Duration;

use crate::integration_tests::evil::test_utils::try_scalar_udfs;

#[tokio::test]
async fn test_root() {
    let fut = try_scalar_udfs("spin::root");
    assert_timeout(fut).await;
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
///   suspended). That would be a legitimate bug since the evil paylod should not be able to force the host into a
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
