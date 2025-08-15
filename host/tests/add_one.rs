use datafusion::logical_expr::ScalarUDFImpl;
use datafusion_udf_wasm_host::WasmScalarUdf;

#[tokio::test]
async fn test_add_one() {
    let data = tokio::fs::read(format!(
        "{}/../target/wasm32-wasip2/debug/examples/add_one.wasm",
        env!("CARGO_MANIFEST_DIR")
    ))
    .await
    .unwrap();

    let mut udfs = WasmScalarUdf::new(&data).unwrap();
    assert_eq!(udfs.len(), 1);
    let udf = udfs.pop().unwrap();
    assert_eq!(udf.name(), "add_one");
}
