use std::{any::Any, ops::DerefMut, sync::Arc};

use datafusion::{
    arrow::datatypes::DataType,
    error::Result as DataFusionResult,
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature},
};
use tokio::sync::Mutex;
use wasmtime::{
    Engine, Store,
    component::{Component, Linker, ResourceAny},
};
use wasmtime_wasi::{
    ResourceTable,
    p2::{IoView, WasiCtx, WasiView},
};

use crate::bindings::exports::datafusion_udf_wasm::udf::types as wit_types;
use crate::error::WasmToDataFusionResultExt;

mod bindings;
mod conversion;
mod error;

struct WasmStateImpl {
    wasi_ctx: WasiCtx,
    resource_table: ResourceTable,
}

impl std::fmt::Debug for WasmStateImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            wasi_ctx: _,
            resource_table,
        } = self;
        f.debug_struct("WasmStateImpl")
            .field("wasi_ctx", &"<WASI_CTX>")
            .field("resource_table", resource_table)
            .finish()
    }
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

pub struct WasmScalarUdf {
    store: Arc<Mutex<Store<WasmStateImpl>>>,
    bindings: Arc<bindings::Datafusion>,
    resource: ResourceAny,
    name: String,
    signature: Signature,
}

impl WasmScalarUdf {
    pub async fn new(wasm_binary: &[u8]) -> DataFusionResult<Vec<Self>> {
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

        let bindings = Arc::new(
            bindings::Datafusion::instantiate(&mut store, &component, &mut linker)
                .context("initialize bindings")?,
        );

        let udf_resources = bindings
            .datafusion_udf_wasm_udf_types()
            .call_scalar_udfs(&mut store)
            .context("call scalar_udfs() method")?;

        let store = Arc::new(Mutex::new(store));

        let mut udfs = Vec::with_capacity(udf_resources.len());
        for resource in udf_resources {
            let mut store_guard = store.lock().await;
            let store2: &mut Store<WasmStateImpl> = &mut store_guard;
            let name = bindings
                .datafusion_udf_wasm_udf_types()
                .scalar_udf()
                .call_name(store2, resource.clone())
                .context("call ScalarUdf::name")?;

            let store2: &mut Store<WasmStateImpl> = &mut store_guard;
            let signature = bindings
                .datafusion_udf_wasm_udf_types()
                .scalar_udf()
                .call_signature(store2, resource.clone())
                .context("call ScalarUdf::signature")?
                .into();

            udfs.push(Self {
                store: Arc::clone(&store),
                bindings: Arc::clone(&bindings),
                resource,
                name,
                signature,
            });
        }

        Ok(udfs)
    }
}

impl std::fmt::Debug for WasmScalarUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            store,
            bindings: _,
            resource,
            name,
            signature,
        } = self;

        f.debug_struct("WasmScalarUdf")
            .field("store", store)
            .field("bindings", &"<BINDINGS>")
            .field("resource", resource)
            .field("name", name)
            .field("signature", signature)
            .finish()
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

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        async_in_sync_context(async {
            let arg_types = arg_types
                .iter()
                .map(|t| wit_types::DataType::try_from(t.clone()))
                .collect::<Result<Vec<_>, _>>()?;
            let mut store_guard = self.store.lock().await;
            let return_type = self
                .bindings
                .datafusion_udf_wasm_udf_types()
                .scalar_udf()
                .call_return_type(store_guard.deref_mut(), self.resource.clone(), &arg_types)
                .context("call ScalarUdf::return_type")??;
            Ok(return_type.into())
        })
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        todo!()
    }
}

// this is a hack that is required because the respective DataFusion interfaces aren't fully async
// TODO: remove this!
fn async_in_sync_context<Fut>(fut: Fut) -> Fut::Output
where
    Fut: Future,
{
    tokio::task::block_in_place(move || tokio::runtime::Handle::current().block_on(fut))
}
