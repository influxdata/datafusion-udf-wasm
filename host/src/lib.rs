use std::{any::Any, borrow::Cow, collections::HashMap, ops::DerefMut, sync::Arc};

use datafusion::{
    arrow::datatypes::DataType,
    error::Result as DataFusionResult,
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature},
};
use tokio::sync::Mutex;
use wasmtime::{
    CacheStore, Engine, Store,
    component::{Component, Linker, ResourceAny},
};
use wasmtime_wasi::{
    ResourceTable,
    p2::{IoView, WasiCtx, WasiView, pipe::MemoryOutputPipe},
};

use crate::bindings::exports::datafusion_udf_wasm::udf::types as wit_types;
use crate::error::WasmToDataFusionResultExt;

mod bindings;
mod conversion;
mod error;

struct WasmStateImpl {
    stderr: MemoryOutputPipe,
    wasi_ctx: WasiCtx,
    resource_table: ResourceTable,
}

impl std::fmt::Debug for WasmStateImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            stderr,
            wasi_ctx: _,
            resource_table,
        } = self;
        f.debug_struct("WasmStateImpl")
            .field("stderr", stderr)
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

#[derive(Debug, Default)]
struct CompilationCache {
    data: std::sync::RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl CacheStore for CompilationCache {
    fn get(&self, key: &[u8]) -> Option<Cow<'_, [u8]>> {
        let guard = self.data.read().expect("not poisoned");
        guard.get(key).map(|data| Cow::Owned(data.clone()))
    }

    fn insert(&self, key: &[u8], value: Vec<u8>) -> bool {
        let mut guard = self.data.write().expect("not poisoned");
        guard.insert(key.to_owned(), value.to_owned());

        // always succeeds
        true
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
        let stderr = MemoryOutputPipe::new(1024);
        let wasi_ctx = WasiCtx::builder().stderr(stderr.clone()).build();
        let state = WasmStateImpl {
            stderr,
            wasi_ctx,
            resource_table: ResourceTable::new(),
        };

        let engine = Engine::new(
            wasmtime::Config::new()
                .async_support(true)
                .enable_incremental_compilation(Arc::new(CompilationCache::default()))
                .context("enable incremental compilation", None)?,
        )
        .context("create WASM engine", None)?;
        let mut store = Store::new(&engine, state);

        let component =
            Component::from_binary(&engine, wasm_binary).context("create WASM component", None)?;

        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker).context("link WASI p2", None)?;

        let bindings = Arc::new(
            bindings::Datafusion::instantiate_async(&mut store, &component, &linker)
                .await
                .context("initialize bindings", Some(&store.data().stderr.contents()))?,
        );

        let udf_resources = bindings
            .datafusion_udf_wasm_udf_types()
            .call_scalar_udfs(&mut store)
            .await
            .context(
                "call scalar_udfs() method",
                Some(&store.data().stderr.contents()),
            )?;

        let store = Arc::new(Mutex::new(store));

        let mut udfs = Vec::with_capacity(udf_resources.len());
        for resource in udf_resources {
            let mut store_guard = store.lock().await;
            let store2: &mut Store<WasmStateImpl> = &mut store_guard;
            let name = bindings
                .datafusion_udf_wasm_udf_types()
                .scalar_udf()
                .call_name(store2, resource)
                .await
                .context(
                    "call ScalarUdf::name",
                    Some(&store_guard.data().stderr.contents()),
                )?;

            let store2: &mut Store<WasmStateImpl> = &mut store_guard;
            let signature = bindings
                .datafusion_udf_wasm_udf_types()
                .scalar_udf()
                .call_signature(store2, resource)
                .await
                .context(
                    "call ScalarUdf::signature",
                    Some(&store_guard.data().stderr.contents()),
                )?
                .try_into()?;

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
                .map(|t| wit_types::DataType::from(t.clone()))
                .collect::<Vec<_>>();
            let mut store_guard = self.store.lock().await;
            let return_type = self
                .bindings
                .datafusion_udf_wasm_udf_types()
                .scalar_udf()
                .call_return_type(store_guard.deref_mut(), self.resource, &arg_types)
                .await
                .context(
                    "call ScalarUdf::return_type",
                    Some(&store_guard.data().stderr.contents()),
                )??;
            return_type.try_into()
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        async_in_sync_context(async {
            let args = args.try_into()?;
            let mut store_guard = self.store.lock().await;
            let return_type = self
                .bindings
                .datafusion_udf_wasm_udf_types()
                .scalar_udf()
                .call_invoke_with_args(store_guard.deref_mut(), self.resource, &args)
                .await
                .context(
                    "call ScalarUdf::return_type",
                    Some(&store_guard.data().stderr.contents()),
                )??;
            return_type.try_into()
        })
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
