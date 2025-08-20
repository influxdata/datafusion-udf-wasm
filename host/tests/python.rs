use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_common::{ScalarValue, assert_contains};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_udf_wasm_host::WasmScalarUdf;

#[tokio::test(flavor = "multi_thread")]
async fn test() {
    let data = tokio::fs::read(format!(
        "{}/../target/wasm32-wasip2/debug/datafusion_udf_wasm_python.wasm",
        env!("CARGO_MANIFEST_DIR")
    ))
    .await
    .unwrap();

    let mut udfs = WasmScalarUdf::new(&data).await.unwrap();
    assert_eq!(udfs.len(), 1);
    let udf = udfs.pop().unwrap();

    assert_eq!(udf.name(), "test");

    assert_eq!(
        udf.signature(),
        &Signature::uniform(0, vec![], Volatility::Immutable),
    );

    assert_eq!(udf.return_type(&[]).unwrap(), DataType::Utf8,);

    let ColumnarValue::Scalar(scalar) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
        })
        .unwrap()
    else {
        panic!("should be a scalar");
    };
    let ScalarValue::Utf8(s) = scalar else {
        panic!("scalar should be UTF8");
    };
    assert_contains!(s.unwrap(), "Hello Unknown, I'm Python");
}
