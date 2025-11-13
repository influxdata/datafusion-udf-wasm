use std::sync::Arc;

use arrow::{
    array::{Array, Int32Array},
    datatypes::{DataType, Field},
};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    async_udf::AsyncScalarUDFImpl,
};
use datafusion_udf_wasm_host::{WasmComponentPrecompiled, WasmScalarUdf};
use tokio::runtime::Handle;

// FIXME: remove `multi_thread` flavor.
//
// This test relies on a non-exact function signature to verify error handling
// in `return_type``. [WasmScalarUdf::return_type](ScalarUdfImpl::return_type)
// is *not* async, and will need to compute the return type if the function
// signature is not exact, which effectively means it will block; which is
// incompatible with the current single-threaded tokio runtime used in tests.
#[tokio::test(flavor = "multi_thread")]
async fn test_add_one() {
    let component = WasmComponentPrecompiled::new(datafusion_udf_wasm_bundle::BIN_EXAMPLE.into())
        .await
        .unwrap();
    let mut udfs = WasmScalarUdf::new(
        &component,
        &Default::default(),
        Handle::current(),
        "".to_owned(),
    )
    .await
    .unwrap();
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
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(Int32Array::from_iter([
                    Some(3),
                    None,
                    Some(1),
                ])))],
                arg_fields: vec![Arc::new(Field::new("a1", DataType::Int32, true))],
                number_rows: 3,
                return_field: Arc::new(Field::new("r", DataType::Int32, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        array.as_ref(),
        &Int32Array::from_iter([Some(4), None, Some(2)]) as &dyn Array,
    );
    let array = udf
        .invoke_async_with_args(
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(3)))],
                arg_fields: vec![Arc::new(Field::new("a1", DataType::Int32, true))],
                number_rows: 3,
                return_field: Arc::new(Field::new("r", DataType::Int32, true)),
            },
            &ConfigOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        array.as_ref(),
        &Int32Array::from_iter([Some(4), Some(4), Some(4)]) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_invoke_with_args_returns_error() {
    let component = WasmComponentPrecompiled::new(datafusion_udf_wasm_bundle::BIN_EXAMPLE.into())
        .await
        .unwrap();
    let mut udfs = WasmScalarUdf::new(
        &component,
        &Default::default(),
        Handle::current(),
        "".to_owned(),
    )
    .await
    .unwrap();
    let udf = udfs.pop().unwrap();

    let result = udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(3)))],
        arg_fields: vec![Arc::new(Field::new("a1", DataType::Int32, true))],
        number_rows: 3,
        return_field: Arc::new(Field::new("r", DataType::Int32, true)),
    });

    assert!(result.is_err());
    let error = result.unwrap_err();
    insta::assert_snapshot!(
        error,
        @r"This feature is not implemented: synchronous invocation of WasmScalarUdf is not supported, use invoke_async_with_args instead"
    );
}
