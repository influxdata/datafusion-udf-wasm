use std::{any::Any, io::Cursor, ops::DerefMut, sync::Arc};

use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use tempfile::TempDir;
use tokio::sync::Mutex;
use wasmtime::{
    Engine, Store,
    component::{Component, Linker, ResourceAny},
};
use wasmtime_wasi::{
    DirPerms, FilePerms, ResourceTable,
    p2::{IoView, WasiCtx, WasiView, pipe::MemoryOutputPipe},
};

use crate::{
    bindings::exports::datafusion_udf_wasm::udf::types as wit_types,
    compilation_cache::CompilationCache, tokio_helpers::async_in_sync_context,
};
use crate::{error::WasmToDataFusionResultExt, tokio_helpers::blocking_io};

mod bindings;
mod compilation_cache;
mod conversion;
mod error;
mod tokio_helpers;

struct WasmStateImpl {
    root: TempDir,
    stderr: MemoryOutputPipe,
    wasi_ctx: WasiCtx,
    resource_table: ResourceTable,
}

impl std::fmt::Debug for WasmStateImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            root,
            stderr,
            wasi_ctx: _,
            resource_table,
        } = self;
        f.debug_struct("WasmStateImpl")
            .field("root", root)
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

pub struct WasmScalarUdf {
    store: Arc<Mutex<Store<WasmStateImpl>>>,
    bindings: Arc<bindings::Datafusion>,
    resource: ResourceAny,
    name: String,
    signature: Signature,
}

impl WasmScalarUdf {
    pub async fn new(wasm_binary: &[u8]) -> DataFusionResult<Vec<Self>> {
        // TODO: we need an in-mem file system for this, see
        //       - https://github.com/bytecodealliance/wasmtime/issues/8963
        //       - https://github.com/Timmmm/wasmtime_fs_demo
        let root = blocking_io(|| TempDir::new())
            .await
            .map_err(|e| DataFusionError::IoError(e))?;

        let stderr = MemoryOutputPipe::new(1024);
        let wasi_ctx = WasiCtx::builder()
            .stderr(stderr.clone())
            .preopened_dir(root.path(), "/", DirPerms::READ, FilePerms::READ)
            .context("pre-open root dir", None)?
            .build();
        let state = WasmStateImpl {
            root,
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

        // fill root FS
        let root_data = bindings
            .datafusion_udf_wasm_udf_types()
            .call_root_fs_tar(&mut store)
            .await
            .context(
                "call root_fs_tar() method",
                Some(&store.data().stderr.contents()),
            )?;
        if let Some(root_data) = root_data {
            let root_path = store.data().root.path().to_owned();
            blocking_io(move || {
                let mut a = tar::Archive::new(Cursor::new(root_data));
                a.unpack(root_path)
            })
            .await
            .map_err(|e| DataFusionError::IoError(e))?;
        }

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
                    "call ScalarUdf::invoke_with_args",
                    Some(&store_guard.data().stderr.contents()),
                )??;
            return_type.try_into()
        })
    }
}
