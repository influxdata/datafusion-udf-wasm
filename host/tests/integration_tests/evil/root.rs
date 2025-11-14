use crate::integration_tests::evil::test_utils::try_scalar_udfs;

#[tokio::test]
async fn test_many_files() {
    let err = try_scalar_udfs("root::many_files").await.unwrap_err();

    insta::assert_snapshot!(
        err,
        @"IO error: inodes limit reached: limit<=10000 current==10000 requested+=1");
}
