use std::sync::Arc;

use arrow::{
    array::{Array, Int32Array},
    datatypes::{DataType, Field},
};
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_udf_wasm_host::{WasmComponentPrecompiled, WasmScalarUdf};

use crate::integration_tests::test_utils::ColumnarValueExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_add_one() {
    let component = WasmComponentPrecompiled::new(datafusion_udf_wasm_bundle::BIN_EXAMPLE.into())
        .await
        .unwrap();
    let mut udfs = WasmScalarUdf::new(&component, "".to_owned()).await.unwrap();
    assert_eq!(udfs.len(), 1);
    let udf = udfs.pop().unwrap();

    assert_eq!(udf.name(), "add_one");

    assert_eq!(
        udf.signature(),
        &Signature::uniform(1, vec![DataType::Int32], Volatility::Immutable),
    );

    assert_eq!(
        udf.return_type(&[DataType::Int32]).unwrap(),
        DataType::Int32,
    );
    insta::assert_snapshot!(
        udf.return_type(&[]).unwrap_err(),
        @"Error during planning: add_one expects exactly one argument",
    );

    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int32Array::from_iter([
                Some(3),
                None,
                Some(1),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Int32, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Int32, true)),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Int32Array::from_iter([Some(4), None, Some(2)]) as &dyn Array,
    );
    let scalar = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(3)))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Int32, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Int32, true)),
        })
        .unwrap()
        .unwrap_scalar();
    assert_eq!(scalar, ScalarValue::Int32(Some(4)));
}
