use std::sync::Arc;

use arrow::{
    array::{Array, Int32Array},
    datatypes::{DataType, Field},
};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    async_udf::AsyncScalarUDFImpl,
};
use datafusion_udf_wasm_host::{
    StaticResourceLimits, WasmComponentPrecompiled, WasmPermissions, WasmScalarUdf,
};
use tokio::{runtime::Handle, sync::OnceCell};

use crate::integration_tests::test_utils::ColumnarValueExt;

// FIXME: remove `multi_thread` flavor.
//
// This test relies on a non-exact function signature to verify error handling
// in `return_type``. [WasmScalarUdf::return_type](ScalarUdfImpl::return_type)
// is *not* async, and will need to compute the return type if the function
// signature is not exact, which effectively means it will block; which is
// incompatible with the current single-threaded tokio runtime used in tests.
#[tokio::test(flavor = "multi_thread")]
async fn test_add_one() {
    let udf = udf().await;

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
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int32Array::from_iter([
                Some(3),
                None,
                Some(1),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Int32, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Int32, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_array();
    assert_eq!(
        array.as_ref(),
        &Int32Array::from_iter([Some(4), None, Some(2)]) as &dyn Array,
    );

    let scalar = udf
        .invoke_async_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(3)))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Int32, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Int32, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .await
        .unwrap()
        .unwrap_scalar();
    assert_eq!(scalar, ScalarValue::Int32(Some(4)));
}

#[tokio::test]
async fn test_invoke_with_args_returns_error() {
    let udf = udf().await;

    let result = udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(3)))],
        arg_fields: vec![Arc::new(Field::new("a1", DataType::Int32, true))],
        number_rows: 3,
        return_field: Arc::new(Field::new("r", DataType::Int32, true)),
        config_options: Arc::new(ConfigOptions::default()),
    });

    assert!(result.is_err());
    let error = result.unwrap_err();
    insta::assert_snapshot!(
        error,
        @r"This feature is not implemented: synchronous invocation of WasmScalarUdf is not supported, use invoke_async_with_args instead"
    );
}

#[test]
fn test_return_type_outside_tokio_context() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let udf = rt.block_on(udf());

    let err = udf.return_type(&[]).unwrap_err();
    insta::assert_snapshot!(
        err,
        @r"
    get tokio runtime for in-place blocking
    caused by
    External error: there is no reactor running, must be called from the context of a Tokio 1.x runtime
    "
    );
}

#[tokio::test]
async fn test_return_type_no_multithread_runtime() {
    let udf = udf().await;

    let err = udf.return_type(&[]).unwrap_err();
    insta::assert_snapshot!(
        err,
        @"This feature is not implemented: in-place blocking only works for tokio multi-thread runtimes, not for CurrentThread"
    );
}

#[tokio::test]
async fn test_stderr_is_included_in_mem() {
    let component = component().await;
    let err = WasmScalarUdf::new(
        component,
        &WasmPermissions::default().with_stderr_bytes(10_000_001),
        Handle::current(),
        &(Arc::new(GreedyMemoryPool::new(10_000_000)) as _),
        "".to_owned(),
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @"Resources exhausted: Failed to allocate additional 9.5 MB for WASM UDF resources with 0.0 B already allocated for this reservation - 9.5 MB remain available for the total pool"
    );
}

#[tokio::test]
async fn test_component_initial_mem_is_included_in_mem() {
    let component = component().await;
    let err = WasmScalarUdf::new(
        component,
        &WasmPermissions::default(),
        Handle::current(),
        &(Arc::new(GreedyMemoryPool::new(1_000_000)) as _),
        "".to_owned(),
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    link WASM components
    caused by
    External error: initialize bindings
    "
    );
}

#[tokio::test]
async fn test_limit_initial_n_instances() {
    let component = component().await;
    let err = WasmScalarUdf::new(
        component,
        &WasmPermissions::default().with_resource_limits(StaticResourceLimits {
            n_instances: 0,
            ..Default::default()
        }),
        Handle::current(),
        &(Arc::new(UnboundedMemoryPool::default()) as _),
        "".to_owned(),
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    link WASM components
    caused by
    External error: initialize bindings
    "
    );
}

#[tokio::test]
async fn test_limit_initial_n_tables() {
    let component = component().await;
    let err = WasmScalarUdf::new(
        component,
        &WasmPermissions::default().with_resource_limits(StaticResourceLimits {
            n_tables: 0,
            ..Default::default()
        }),
        Handle::current(),
        &(Arc::new(UnboundedMemoryPool::default()) as _),
        "".to_owned(),
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    link WASM components
    caused by
    External error: initialize bindings
    "
    );
}

#[tokio::test]
async fn test_limit_initial_n_elements_per_table() {
    let component = component().await;
    let err = WasmScalarUdf::new(
        component,
        &WasmPermissions::default().with_resource_limits(StaticResourceLimits {
            n_elements_per_table: 0,
            ..Default::default()
        }),
        Handle::current(),
        &(Arc::new(UnboundedMemoryPool::default()) as _),
        "".to_owned(),
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    link WASM components
    caused by
    External error: initialize bindings
    "
    );
}

#[tokio::test]
async fn test_limit_initial_n_memories() {
    let component = component().await;
    let err = WasmScalarUdf::new(
        component,
        &WasmPermissions::default().with_resource_limits(StaticResourceLimits {
            n_memories: 0,
            ..Default::default()
        }),
        Handle::current(),
        &(Arc::new(UnboundedMemoryPool::default()) as _),
        "".to_owned(),
    )
    .await
    .unwrap_err();

    insta::assert_snapshot!(
        err,
        @r"
    link WASM components
    caused by
    External error: initialize bindings
    "
    );
}

async fn component() -> &'static WasmComponentPrecompiled {
    static COMPONENT: OnceCell<WasmComponentPrecompiled> = OnceCell::const_new();

    COMPONENT
        .get_or_init(async || {
            WasmComponentPrecompiled::new(datafusion_udf_wasm_bundle::BIN_EXAMPLE.into())
                .await
                .unwrap()
        })
        .await
}

async fn udf() -> WasmScalarUdf {
    let component = component().await;
    let mut udfs = WasmScalarUdf::new(
        component,
        &Default::default(),
        Handle::current(),
        &(Arc::new(UnboundedMemoryPool::default()) as _),
        "".to_owned(),
    )
    .await
    .unwrap();
    assert_eq!(udfs.len(), 1);
    udfs.pop().unwrap()
}
