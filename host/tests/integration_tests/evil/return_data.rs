use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ScalarFunctionArgs, async_udf::AsyncScalarUDFImpl};

use crate::integration_tests::evil::test_utils::try_scalar_udfs;

#[tokio::test]
async fn test_wrong_number_of_rows() {
    let [udf] = try_scalar_udfs("return_data")
        .await
        .unwrap()
        .try_into()
        .unwrap();

    let err = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 42,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"External error: UDF returned array of length 43 but should produce 42 rows",
    );
}
