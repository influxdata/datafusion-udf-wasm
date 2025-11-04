//! Host-code for WebAssembly-based [DataFusion] UDFs.
//!
//!
//! [DataFusion]: https://datafusion.apache.org/
use std::{any::Any, ops::DerefMut, sync::Arc};

use ::http::HeaderName;
use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use tokio::sync::Mutex;
use wasmtime::{
    Engine, Store,
    component::{Component, ResourceAny},
};
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxView, WasiView, p2::pipe::MemoryOutputPipe};
use wasmtime_wasi_http::{
    HttpResult, WasiHttpCtx, WasiHttpView,
    bindings::http::types::ErrorCode as HttpErrorCode,
    body::HyperOutgoingBody,
    types::{
        DEFAULT_FORBIDDEN_HEADERS, HostFutureIncomingResponse, OutgoingRequestConfig,
        default_send_request_handler,
    },
};

use crate::{
    bindings::exports::datafusion_udf_wasm::udf::types as wit_types,
    error::{DataFusionResultExt, WasmToDataFusionResultExt},
    http::{HttpRequestValidator, RejectAllHttpRequests},
    linker::link,
    tokio_helpers::async_in_sync_context,
    vfs::{VfsCtxView, VfsLimits, VfsState, VfsView},
};

// unused-crate-dependencies false positives
#[cfg(test)]
use datafusion_udf_wasm_bundle as _;
#[cfg(test)]
use insta as _;
#[cfg(test)]
use wiremock as _;

mod bindings;
mod conversion;
mod error;
pub mod http;
mod linker;
mod tokio_helpers;
pub mod vfs;

/// State of the WASM payload.
struct WasmStateImpl {
    /// Virtual filesystem for the WASM payload.
    ///
    /// This filesystem is provided to the payload in memory with read-write support.
    vfs_state: VfsState,

    /// A limited buffer for stderr.
    ///
    /// This is especially useful for when the payload crashes.
    stderr: MemoryOutputPipe,

    /// WASI context.
    wasi_ctx: WasiCtx,

    /// WASI HTTP context.
    wasi_http_ctx: WasiHttpCtx,

    /// Resource tables.
    resource_table: ResourceTable,

    /// HTTP request validator.
    http_validator: Arc<dyn HttpRequestValidator>,
}

impl std::fmt::Debug for WasmStateImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            vfs_state,
            stderr,
            wasi_ctx: _,
            wasi_http_ctx: _,
            resource_table,
            http_validator,
        } = self;
        f.debug_struct("WasmStateImpl")
            .field("vfs_state", vfs_state)
            .field("stderr", stderr)
            .field("wasi_ctx", &"<WASI_CTX>")
            .field("resource_table", resource_table)
            .field("http_validator", http_validator)
            .finish()
    }
}

impl WasiView for WasmStateImpl {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.resource_table,
        }
    }
}

impl WasiHttpView for WasmStateImpl {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.wasi_http_ctx
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.resource_table
    }

    fn send_request(
        &mut self,
        mut request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        // Python `requests` sends this so we allow it but later drop it from the actual request.
        request.headers_mut().remove(hyper::header::CONNECTION);

        // technically we could return an error straight away, but `urllib3` doesn't handle that super well, so we
        // create a future and validate the error in there (before actually starting the request of course)

        let validator = Arc::clone(&self.http_validator);
        let handle = wasmtime_wasi::runtime::spawn(async move {
            // yes, that's another layer of futures. The WASI interface is somewhat nested.
            let fut = async {
                validator
                    .validate(&request, config.use_tls)
                    .map_err(|_| HttpErrorCode::HttpRequestDenied)?;

                log::debug!(
                    "UDF HTTP request: {} {}",
                    request.method().as_str(),
                    request.uri(),
                );
                default_send_request_handler(request, config).await
            };

            Ok(fut.await)
        });

        Ok(HostFutureIncomingResponse::pending(handle))
    }

    fn is_forbidden_header(&mut self, name: &HeaderName) -> bool {
        // Python `requests` sends this so we allow it but later drop it from the actual request.
        if name == hyper::header::CONNECTION {
            return false;
        }

        DEFAULT_FORBIDDEN_HEADERS.contains(name)
    }
}

impl VfsView for WasmStateImpl {
    fn vfs(&mut self) -> VfsCtxView<'_> {
        VfsCtxView {
            table: &mut self.resource_table,
            vfs_state: &mut self.vfs_state,
        }
    }
}

/// Pre-compiled WASM component.
///
/// The pre-compilation is stateless and can be used to [create](WasmScalarUdf::new) multiple instances that do not share
/// any state.
pub struct WasmComponentPrecompiled {
    /// WASM execution engine based on [`wasmtime`].
    engine: Engine,

    /// The pre-compiled component based on the binary provided by the API user.
    component: Component,
}

impl WasmComponentPrecompiled {
    /// Pre-compile WASM payload.
    ///
    /// Accepts a WASM payload in [binary format].
    ///
    ///
    /// [binary format]: https://webassembly.github.io/spec/core/binary/index.html
    pub async fn new(wasm_binary: Arc<[u8]>) -> DataFusionResult<Self> {
        tokio::task::spawn_blocking(move || {
            let engine = Engine::new(
                wasmtime::Config::new()
                    .async_support(true)
                    .memory_init_cow(true),
            )
            .context("create WASM engine", None)?;

            let compiled_component = engine
                .precompile_component(&wasm_binary)
                .context("pre-compile component", None)?;

            log::debug!(
                "Pre-compiled {} bytes of WASM bytecode into {} bytes",
                wasm_binary.len(),
                compiled_component.len()
            );

            // SAFETY: the compiled version was produced by us with the same engine. This is NOT external/untrusted input.
            let component_res = unsafe { Component::deserialize(&engine, compiled_component) };
            let component = component_res.context("create WASM component", None)?;

            Ok(Self { engine, component })
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
    }
}

impl std::fmt::Debug for WasmComponentPrecompiled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            engine,
            component: _,
        } = self;

        f.debug_struct("WasmComponentPrecompiled")
            .field("engine", engine)
            .field("component", &"<COMPONENT>")
            .finish()
    }
}

/// Permissions for a WASM component.
#[derive(Debug)]
pub struct WasmPermissions {
    /// Validator for HTTP requests.
    http: Arc<dyn HttpRequestValidator>,

    /// Virtual file system limits.
    vfs: VfsLimits,
}

impl WasmPermissions {
    /// Create default permissions.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for WasmPermissions {
    fn default() -> Self {
        Self {
            http: Arc::new(RejectAllHttpRequests),
            vfs: VfsLimits::default(),
        }
    }
}

impl WasmPermissions {
    /// Set HTTP validator.
    pub fn with_http<V>(self, http: V) -> Self
    where
        V: HttpRequestValidator,
    {
        Self {
            http: Arc::new(http),
            ..self
        }
    }

    /// Set virtual filesystem limits.
    pub fn with_vfs_limits(self, limits: VfsLimits) -> Self {
        Self {
            vfs: limits,
            ..self
        }
    }
}

/// A [`ScalarUDFImpl`] that wraps a WebAssembly payload.
pub struct WasmScalarUdf {
    /// Mutable state.
    ///
    /// This mostly contains [`WasmStateImpl`].
    store: Arc<Mutex<Store<WasmStateImpl>>>,

    /// WIT-based bindings that we resolved within the payload.
    bindings: Arc<bindings::Datafusion>,

    /// Resource handle for the Scalar UDF within the VM.
    ///
    /// This is somewhat an "object reference".
    resource: ResourceAny,

    /// Name of the UDF.
    ///
    /// This was pre-fetched during UDF generation because [`ScalarUDFImpl::name`] is sync and requires us to return a
    /// reference.
    name: String,

    /// Signature of the UDF.
    ///
    /// This was pre-fetched during UDF generation because [`ScalarUDFImpl::signature`] is sync and requires us to return a
    /// reference.
    signature: Signature,
}

impl WasmScalarUdf {
    /// Create multiple UDFs from a single WASM VM.
    ///
    /// UDFs bound to the same VM share state, however calling this method multiple times will yield independent WASM VMs.
    pub async fn new(
        component: &WasmComponentPrecompiled,
        permissions: &WasmPermissions,
        source: String,
    ) -> DataFusionResult<Vec<Self>> {
        let WasmComponentPrecompiled { engine, component } = component;

        // Create in-memory VFS
        let vfs_state = VfsState::new(&permissions.vfs);

        let stderr = MemoryOutputPipe::new(1024);
        let wasi_ctx = WasiCtx::builder().stderr(stderr.clone()).build();
        let state = WasmStateImpl {
            vfs_state,
            stderr,
            wasi_ctx,
            wasi_http_ctx: WasiHttpCtx::new(),
            resource_table: ResourceTable::new(),
            http_validator: Arc::clone(&permissions.http),
        };
        let (bindings, mut store) = link(engine, component, state)
            .await
            .context("link WASM components", None)?;

        // Populate VFS from tar archive
        let root_data = bindings
            .datafusion_udf_wasm_udf_types()
            .call_root_fs_tar(&mut store)
            .await
            .context(
                "call root_fs_tar() method",
                Some(&store.data().stderr.contents()),
            )?;
        if let Some(root_data) = root_data {
            store
                .data_mut()
                .vfs_state
                .populate_from_tar(&root_data)
                .map_err(DataFusionError::IoError)?;
        }

        let udf_resources = bindings
            .datafusion_udf_wasm_udf_types()
            .call_scalar_udfs(&mut store, &source)
            .await
            .context(
                "calling scalar_udfs() method failed",
                Some(&store.data().stderr.contents()),
            )?
            .context("scalar_udfs")?;

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
