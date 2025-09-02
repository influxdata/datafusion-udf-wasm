use std::sync::Arc;

use arrow::{
    array::{Array, Int32Array},
    datatypes::{DataType, Field},
};
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_udf_wasm_host::WasmScalarUdf;

#[tokio::test(flavor = "multi_thread")]
async fn test_add_one() {
    let data = tokio::fs::read(format!(
        "{}/../target/wasm32-wasip2/debug/examples/add_one.wasm",
        env!("CARGO_MANIFEST_DIR")
    ))
    .await
    .unwrap();

    let mut udfs = WasmScalarUdf::new(&data, "".to_owned()).await.unwrap();
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
    assert_eq!(
        udf.return_type(&[]).unwrap_err().to_string(),
        "Error during planning: add_one expects exactly one argument",
    );

    let ColumnarValue::Array(array) = udf
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
    else {
        panic!("should be an array")
    };
    assert_eq!(
        array.as_ref(),
        &Int32Array::from_iter([Some(4), None, Some(2)]) as &dyn Array,
    );
    let ColumnarValue::Scalar(scalar) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(3)))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Int32, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Int32, true)),
        })
        .unwrap()
    else {
        panic!("should be a scalar")
    };
    assert_eq!(scalar, ScalarValue::Int32(Some(4)));
}
