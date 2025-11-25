use crate::integration_tests::evil::test_utils::try_scalar_udfs;

#[tokio::test]
async fn test_udfs_duplicate_names() {
    let err = try_scalar_udfs("complex::udfs_duplicate_names")
        .await
        .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"External error: non-unique UDF name: 'foo'");
}
