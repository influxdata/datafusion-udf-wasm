//! Host-code for WebAssembly-based [DataFusion] UDFs.
//!
//!
//! [DataFusion]: https://datafusion.apache.org/
use std::{any::Any, collections::BTreeMap, hash::Hash, ops::DerefMut, sync::Arc, time::Duration};

use ::http::HeaderName;
use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_execution::memory_pool::MemoryPool;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl},
};
use tokio::{runtime::Handle, sync::Mutex, task::JoinSet};
use uuid::Uuid;
use wasmtime::{
    Engine, Store, UpdateDeadline,
    component::{Component, ResourceAny},
};
use wasmtime_wasi::{
    ResourceTable, WasiCtx, WasiCtxView, WasiView, async_trait, p2::pipe::MemoryOutputPipe,
};
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
    limiter::{Limiter, StaticResourceLimits},
    linker::link,
    tokio_helpers::async_in_sync_context,
    vfs::{VfsCtxView, VfsLimits, VfsState, VfsView},
};

// unused-crate-dependencies false positives
#[cfg(test)]
use datafusion_udf_wasm_bundle as _;
#[cfg(test)]
use regex as _;
#[cfg(test)]
use wiremock as _;

mod bindings;
mod conversion;
pub mod error;
pub mod http;
pub mod limiter;
mod linker;
mod tokio_helpers;
pub mod vfs;

/// State of the WASM payload.
struct WasmStateImpl {
    /// Virtual filesystem for the WASM payload.
    ///
    /// This filesystem is provided to the payload in memory with read-write support.
    vfs_state: VfsState,

    /// Resource limiter.
    limiter: Limiter,

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

    /// Handle to tokio I/O runtime.
    io_rt: Handle,
}

impl std::fmt::Debug for WasmStateImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            vfs_state,
            limiter,
            stderr,
            wasi_ctx: _,
            wasi_http_ctx: _,
            resource_table,
            http_validator,
            io_rt,
        } = self;
        f.debug_struct("WasmStateImpl")
            .field("vfs_state", vfs_state)
            .field("limiter", limiter)
            .field("stderr", stderr)
            .field("wasi_ctx", &"<WASI_CTX>")
            .field("resource_table", resource_table)
            .field("http_validator", http_validator)
            .field("io_rt", io_rt)
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
        let _guard = self.io_rt.enter();

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

/// Create WASM engine.
fn create_engine() -> DataFusionResult<Engine> {
    Engine::new(
        wasmtime::Config::new()
            .async_support(true)
            .epoch_interruption(true)
            .memory_init_cow(true)
            // Disable backtraces for now since debug info parsing doesn't seem to work and hence error
            // messages are nondeterministic.
            .wasm_backtrace(false),
    )
    .context("create WASM engine", None)
}

/// Pre-compiled WASM component.
///
/// The pre-compilation is stateless and can be used to [create](WasmScalarUdf::new) multiple instances that do not share
/// any state.
#[derive(Debug)]
pub struct WasmComponentPrecompiled {
    /// Binary representation of the pre-compiled component.
    compiled_component: Vec<u8>,
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
            // Create temporary engine that we need for compilation.
            let engine = create_engine()?;

            let compiled_component = engine
                .precompile_component(&wasm_binary)
                .context("pre-compile component", None)?;

            log::debug!(
                "Pre-compiled {} bytes of WASM bytecode into {} bytes",
                wasm_binary.len(),
                compiled_component.len()
            );

            Ok(Self { compiled_component })
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
    }
}

/// Permissions for a WASM component.
#[derive(Debug)]
pub struct WasmPermissions {
    /// Epoch tick time.
    epoch_tick_time: Duration,

    /// Timeout for blocking tasks, in number of [ticks](Self::epoch_tick_time).
    ///
    /// This is stored as tick count instead of a total timeout, since these two variables should be chosen to be
    /// somewhat consistent. E.g. it makes little sense to massively increase the epoch tick time without also
    /// increasing the timeout.
    inplace_blocking_max_ticks: u32,

    /// Validator for HTTP requests.
    http: Arc<dyn HttpRequestValidator>,

    /// Virtual file system limits.
    vfs: VfsLimits,

    /// Limit of the stored stderr data.
    stderr_bytes: usize,

    /// Static resource limits.
    resource_limits: StaticResourceLimits,

    /// Environment variables.
    envs: BTreeMap<String, String>,
}

impl WasmPermissions {
    /// Create default permissions.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for WasmPermissions {
    fn default() -> Self {
        let epoch_tick_time = Duration::from_millis(10);
        let inplace_blocking_timeout = Duration::from_secs(1);

        Self {
            epoch_tick_time,
            inplace_blocking_max_ticks: inplace_blocking_timeout
                .div_duration_f32(epoch_tick_time)
                .floor() as _,
            http: Arc::new(RejectAllHttpRequests),
            vfs: VfsLimits::default(),
            stderr_bytes: 1024, // 1KB
            resource_limits: StaticResourceLimits::default(),
            envs: BTreeMap::default(),
        }
    }
}

impl WasmPermissions {
    /// Set epoch tick time.
    ///
    /// WASM payload can only be interrupted when the background epoch timer ticks. However, there is a balance
    /// between too aggressive ticking and potentially longer latency.
    pub fn with_epoch_tick_time(self, t: Duration) -> Self {
        Self {
            epoch_tick_time: t,
            ..self
        }
    }

    /// Set timeout for blocking tasks, in number of [ticks](Self::with_epoch_tick_time).
    ///
    /// Exceeding the timeout will result in an error.
    ///
    /// This is configured as tick count instead of a total timeout, since these two variables should be chosen to be
    /// somewhat consistent. E.g. it makes little sense to massively increase the epoch tick time without also
    /// increasing the timeout.
    ///
    /// See <https://github.com/influxdata/datafusion-udf-wasm/issues/169> for a potential better solution in the future.
    pub fn with_inplace_blocking_max_ticks(self, ticks: u32) -> Self {
        Self {
            inplace_blocking_max_ticks: ticks,
            ..self
        }
    }

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

    /// Limit of the stored stderr data.
    pub fn with_stderr_bytes(self, limit: usize) -> Self {
        Self {
            stderr_bytes: limit,
            ..self
        }
    }

    /// Set static resource limits.
    ///
    /// Note that this does NOT limit the overall memory consumption of the payload. This will be done via [`MemoryPool`].
    pub fn with_resource_limits(self, limits: StaticResourceLimits) -> Self {
        Self {
            resource_limits: limits,
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

    /// Add environment variable.
    pub fn with_env(mut self, key: String, value: String) -> Self {
        self.envs.insert(key, value);
        self
    }
}

/// A [`ScalarUDFImpl`] that wraps a WebAssembly payload.
///
/// # Async, Blocking, Cancellation
/// Async methods will yield back to the runtime in periodical intervals. The caller should implement some form of
/// timeout, e.g. using [`tokio::time::timeout`]. It is safe to cancel async methods.
///
/// For the async interruption to work it is important that the I/O [runtime] passed to [`WasmScalarUdf::new`] is
/// different from the runtime used to call UDF methods, since the I/O runtime is also used to schedule an
/// [epoch timer](WasmPermissions::with_epoch_tick_time).
///
/// Methods that return references -- e.g. [`ScalarUDFImpl::name`] and [`ScalarUDFImpl::signature`] -- are cached
/// during UDF creation.
///
/// Some methods do NOT offer an async interface yet, e.g. [`ScalarUDFImpl::return_type`]. For these we try to cache
/// them during creation, but if that is not possible we need to block in place when the method is called. This only
/// works when a multi-threaded tokio runtime is used. There is a
/// [timeout](WasmPermissions::with_inplace_blocking_max_ticks). See
/// <https://github.com/influxdata/datafusion-udf-wasm/issues/169> for a potential future improvement on that front.
///
///
/// [runtime]: tokio::runtime::Runtime
pub struct WasmScalarUdf {
    /// Mutable state.
    ///
    /// This mostly contains [`WasmStateImpl`].
    store: Arc<Mutex<Store<WasmStateImpl>>>,

    /// Background task that keeps the WASM epoch timer running.
    epoch_task: Arc<JoinSet<()>>,

    /// Timeout for blocking tasks.
    inplace_blocking_timeout: Duration,

    /// WIT-based bindings that we resolved within the payload.
    bindings: Arc<bindings::Datafusion>,

    /// Resource handle for the Scalar UDF within the VM.
    ///
    /// This is somewhat an "object reference".
    resource: ResourceAny,

    /// Name of the UDF.
    ///
    /// This was pre-fetched during UDF generation because
    /// [`ScalarUDFImpl::name`] is sync and requires us to return a reference.
    name: String,

    /// We treat every UDF as unique, but we need a proxy value to express that.
    id: Uuid,

    /// Signature of the UDF.
    ///
    /// This was pre-fetched during UDF generation because
    /// [`ScalarUDFImpl::signature`] is sync and requires us to return a
    /// reference.
    signature: Signature,

    /// Return type of the UDF.
    ///
    /// This was pre-fetched during UDF generation because
    /// [`ScalarUDFImpl::return_type`] is sync and requires us to return a
    /// reference. We can only compute the return type if the underlying
    /// [TypeSignature] is [Exact](TypeSignature::Exact).
    return_type: Option<DataType>,
}

impl WasmScalarUdf {
    /// Create multiple UDFs from a single WASM VM.
    ///
    /// UDFs bound to the same VM share state, however calling this method
    /// multiple times will yield independent WASM VMs.
    pub async fn new(
        component: &WasmComponentPrecompiled,
        permissions: &WasmPermissions,
        io_rt: Handle,
        memory_pool: &Arc<dyn MemoryPool>,
        source: String,
    ) -> DataFusionResult<Vec<Self>> {
        let WasmComponentPrecompiled { compiled_component } = component;

        let engine = create_engine()?;

        // set up epoch timer
        let mut epoch_task = JoinSet::new();
        let epoch_tick_time = permissions.epoch_tick_time;
        let engine_weak = engine.weak();
        epoch_task.spawn_on(
            async move {
                // Create the interval within the I/O runtime so that this runtime drives it, not the CPU runtime.
                let mut epoch_ticker = tokio::time::interval(epoch_tick_time);
                epoch_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    epoch_ticker.tick().await;

                    match engine_weak.upgrade() {
                        Some(engine) => {
                            engine.increment_epoch();
                        }
                        None => {
                            return;
                        }
                    }
                }
            },
            &io_rt,
        );
        let epoch_task = Arc::new(epoch_task);
        let inplace_blocking_timeout = permissions
            .epoch_tick_time
            .saturating_mul(permissions.inplace_blocking_max_ticks);

        // SAFETY: the compiled version was produced by us with the same engine. This is NOT external/untrusted input.
        let component_res = unsafe { Component::deserialize(&engine, compiled_component) };
        let component = component_res.context("create WASM component", None)?;

        // resource/mem limiter
        let mut limiter = Limiter::new(permissions.resource_limits.clone(), memory_pool);

        // Create in-memory VFS
        let vfs_state = VfsState::new(permissions.vfs.clone());

        // set up WASI p2 context
        limiter.grow(permissions.stderr_bytes)?;
        let stderr = MemoryOutputPipe::new(permissions.stderr_bytes);
        let mut wasi_ctx_builder = WasiCtx::builder();
        wasi_ctx_builder.stderr(stderr.clone());
        permissions.envs.iter().for_each(|(k, v)| {
            wasi_ctx_builder.env(k, v);
        });

        // configure store
        // NOTE: Do that BEFORE linking so that memory limits are checked for the initial allocation of the WASM
        //       component as well.
        let state = WasmStateImpl {
            vfs_state,
            limiter,
            stderr,
            wasi_ctx: wasi_ctx_builder.build(),
            wasi_http_ctx: WasiHttpCtx::new(),
            resource_table: ResourceTable::new(),
            http_validator: Arc::clone(&permissions.http),
            io_rt,
        };
        let mut store = Store::new(&engine, state);
        store.epoch_deadline_callback(|_| {
            Ok(UpdateDeadline::YieldCustom(
                // increment deadline epoch by one step
                1,
                // tell tokio that we COULD yield (depending on the remaining cooperative budget)
                //
                // NOTE: This future will be executed in the callers context (i.e. whoever is using the WASM UDF code),
                //       NOT in the context of the epoch background timer.
                Box::pin(tokio::task::consume_budget()),
            ))
        });
        store.limiter(|state| &mut state.limiter);

        let bindings = link(&engine, &component, &mut store)
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
            let state = store.data_mut();

            state
                .vfs_state
                .populate_from_tar(&root_data, &mut state.limiter)
                .map_err(|e| DataFusionError::IoError(e).context("populate root FS from TAR"))?;
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
            let signature: Signature = bindings
                .datafusion_udf_wasm_udf_types()
                .scalar_udf()
                .call_signature(store2, resource)
                .await
                .context(
                    "call ScalarUdf::signature",
                    Some(&store_guard.data().stderr.contents()),
                )?
                .try_into()?;

            let return_type = match &signature.type_signature {
                TypeSignature::Exact(t) => {
                    let store2: &mut Store<WasmStateImpl> = &mut store_guard;
                    let r = bindings
                        .datafusion_udf_wasm_udf_types()
                        .scalar_udf()
                        .call_return_type(
                            store2,
                            resource,
                            &t.iter()
                                .map(|dt| wit_types::DataType::from(dt.clone()))
                                .collect::<Vec<_>>(),
                        )
                        .await
                        .context(
                            "call ScalarUdf::return_type",
                            Some(&store_guard.data().stderr.contents()),
                        )??;
                    Some(r.try_into()?)
                }
                _ => None,
            };

            udfs.push(Self {
                store: Arc::clone(&store),
                epoch_task: Arc::clone(&epoch_task),
                inplace_blocking_timeout,
                bindings: Arc::clone(&bindings),
                resource,
                name,
                id: Uuid::new_v4(),
                signature,
                return_type,
            });
        }

        Ok(udfs)
    }

    /// Convert this [WasmScalarUdf] into an [AsyncScalarUDF].
    pub fn as_async_udf(self) -> AsyncScalarUDF {
        AsyncScalarUDF::new(Arc::new(self))
    }

    /// Check that the provided argument types match the UDF signature.
    fn check_arg_types(&self, arg_types: &[DataType]) -> DataFusionResult<()> {
        if let TypeSignature::Exact(expected_types) = &self.signature.type_signature {
            if arg_types.len() != expected_types.len() {
                return Err(DataFusionError::Plan(format!(
                    "`{}` expects {} parameters but got {}",
                    self.name,
                    expected_types.len(),
                    arg_types.len()
                )));
            }

            for (i, (provided, expected)) in arg_types.iter().zip(expected_types.iter()).enumerate()
            {
                if provided != expected {
                    return Err(DataFusionError::Plan(format!(
                        "argument {} of `{}` should be {:?}, got {:?}",
                        i + 1,
                        self.name,
                        expected,
                        provided
                    )));
                }
            }
        }

        Ok(())
    }
}

impl std::fmt::Debug for WasmScalarUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            store,
            epoch_task,
            inplace_blocking_timeout,
            bindings: _,
            resource,
            name,
            id,
            signature,
            return_type,
        } = self;

        f.debug_struct("WasmScalarUdf")
            .field("store", store)
            .field("epoch_task", epoch_task)
            .field("inplace_blocking_timeout", inplace_blocking_timeout)
            .field("bindings", &"<BINDINGS>")
            .field("resource", resource)
            .field("name", name)
            .field("id", id)
            .field("signature", signature)
            .field("return_type", return_type)
            .finish()
    }
}

impl PartialEq<Self> for WasmScalarUdf {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for WasmScalarUdf {}

impl Hash for WasmScalarUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
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
        self.check_arg_types(arg_types)?;

        if let Some(return_type) = &self.return_type {
            return Ok(return_type.clone());
        }

        async_in_sync_context(
            async {
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
            },
            self.inplace_blocking_timeout,
        )
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Err(DataFusionError::NotImplemented(
            "synchronous invocation of WasmScalarUdf is not supported, use invoke_async_with_args instead".to_string(),
        ))
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for WasmScalarUdf {
    fn ideal_batch_size(&self) -> Option<usize> {
        None
    }

    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> DataFusionResult<ColumnarValue> {
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

        drop(store_guard);

        return_type.try_into()
    }
}
