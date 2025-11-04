use std::sync::Arc;

use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_udf_wasm_host::test_utils::python::python_scalar_udf;

use crate::integration_tests::test_utils::ColumnarValueExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_add_one() {
    const CODE: &str = "
def add_one(x: int) -> int:
    return x + 1
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(udf.name(), "add_one");

    assert_eq!(
        udf.signature(),
        &Signature::exact(vec![DataType::Int64], Volatility::Volatile),
    );

    assert_eq!(
        udf.return_type(&[DataType::Int64]).unwrap(),
        DataType::Int64,
    );
    assert_eq!(
        udf.return_type(&[]).unwrap_err().to_string(),
        "Error during planning: `add_one` expects 1 parameters but got 0",
    );

    // call with array
    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                Some(3),
                None,
                Some(1),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Int64, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Int64Array::from_iter([Some(4), None, Some(2)]) as &dyn Array,
    );

    // call with scalar, output will still be an array
    let array = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(3)))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Int64, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Int64Array::from_iter([Some(4), Some(4), Some(4)]) as &dyn Array,
    );
}
