use std::any::Any;

use datafusion::{
    arrow::datatypes::DataType,
    error::Result as DataFusionResult,
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature},
};
use wasmtime::{
    Engine, Store,
    component::{Component, Linker, ResourceAny},
};
use wasmtime_wasi::{
    ResourceTable,
    p2::{IoView, WasiCtx, WasiView},
};

use crate::error::WasmToDataFusionResultExt;

mod bindings;
mod conversion;
mod error;

struct WasmStateImpl {
    wasi_ctx: WasiCtx,
    resource_table: ResourceTable,
}

impl IoView for WasmStateImpl {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.resource_table
    }
}

impl WasiView for WasmStateImpl {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi_ctx
    }
}

#[derive(Debug)]
pub struct WasmScalarUdf {
    #[expect(dead_code)]
    resource: ResourceAny,
    name: String,
    signature: Signature,
}

impl WasmScalarUdf {
    pub fn new(wasm_binary: &[u8]) -> DataFusionResult<Vec<Self>> {
        let engine = Engine::default();

        let state = WasmStateImpl {
            wasi_ctx: WasiCtx::builder().build(),
            resource_table: ResourceTable::new(),
        };
        let mut store = Store::new(&engine, state);

        let component =
            Component::from_binary(&engine, wasm_binary).context("create WASM component")?;

        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker).context("link WASI p2")?;

        let bindings = bindings::Datafusion::instantiate(&mut store, &component, &mut linker)
            .context("initialize bindings")?;

        let udfs = bindings
            .datafusion_udf_wasm_udf_types()
            .call_scalar_udfs(&mut store)
            .context("call udfs() method")?;

        udfs.into_iter()
            .map(|resource| {
                let name = bindings
                    .datafusion_udf_wasm_udf_types()
                    .scalar_udf()
                    .call_name(&mut store, resource.clone())
                    .context("call ScalarUdf::name")?;

                let signature = bindings
                    .datafusion_udf_wasm_udf_types()
                    .scalar_udf()
                    .call_signature(&mut store, resource.clone())
                    .context("call ScalarUdf::signature")?
                    .into();

                Ok(Self {
                    resource,
                    name,
                    signature,
                })
            })
            .collect()
    }
}

impl ScalarUDFImpl for WasmScalarUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        todo!()
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        todo!()
    }
}
