//! WASM component handling.
use std::{ops::Deref, sync::Arc, time::Duration};

use datafusion_common::{DataFusionError, error::Result as DataFusionResult};
use datafusion_execution::memory_pool::MemoryPool;
use tokio::{
    runtime::Handle,
    sync::{Mutex, OwnedMutexGuard},
    task::JoinSet,
};
use wasmtime::{
    AsContext, AsContextMut, Engine, Store, StoreContext, StoreContextMut, UpdateDeadline,
    component::Component,
};
use wasmtime_wasi::{ResourceTable, WasiCtx, p2::pipe::MemoryOutputPipe};
use wasmtime_wasi_http::WasiHttpCtx;

use crate::{
    TrustedDataLimits, WasmPermissions, bindings, error::WasmToDataFusionResultExt,
    ignore_debug::IgnoreDebug, limiter::Limiter, linker::link, state::WasmStateImpl, vfs::VfsState,
};

/// Create WASM engine.
fn create_engine(flags: &CompilationFlags) -> DataFusionResult<Engine> {
    // TODO: Once https://github.com/bytecodealliance/wasmtime/pull/12089 is released, make this an `Option` and treat
    //       `None` as `Config::without_compiler`.
    let CompilationFlags { target } = flags;

    let mut config = wasmtime::Config::new();
    config.async_support(true);
    config.epoch_interruption(true);
    config.memory_init_cow(true);
    // Disable backtraces for now since debug info parsing doesn't seem to work and hence error
    // messages are nondeterministic.
    config.wasm_backtrace(false);

    if let Some(target) = &target {
        config
            .target(target)
            .with_context(|_| format!("cannot set target: {target}"), None)?;
    }

    Engine::new(&config).context("create WASM engine", None)
}

/// Code compilation flags.
///
/// This is used when [compiling a component](WasmComponentPrecompiled::compile).
#[derive(Debug, Default, Clone)]
pub struct CompilationFlags {
    /// Target (triplet).
    ///
    /// Set to [`None`] to use the host configuration. Note that this may lead to unportable compiled code.
    pub target: Option<String>,
}

/// Pre-compiled WASM component.
///
/// The pre-compilation is stateless and can be used to [create](crate::WasmScalarUdf::new) multiple instances that do not share
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
    pub async fn compile(
        wasm_binary: Arc<[u8]>,
        flags: &CompilationFlags,
    ) -> DataFusionResult<Self> {
        // Create temporary engine that we need for compilation.
        let engine = create_engine(flags)?;

        tokio::task::spawn_blocking(move || {
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

    /// Get raw, pre-compiled component data.
    ///
    /// See [`load`](Self::load) too.
    ///
    /// # Usage
    /// Compiling larger components can be relatively expensive. If you know that you are gonna use a fixed guest,
    /// then it might be worth pre-compiling the component once, e.g. as part of a [build script] or when a UDF is
    /// registered in a cluster environment.
    ///
    /// # Exposure
    /// It is generally safe to leak/expose the pre-compiled data to the user that provided the WASM bytecode (see
    /// [`compile`](Self::compile)). However, you must prevent the user from tampering the data, see "safety" section of
    /// [`load`](Self::load).
    ///
    /// The exposed data is opaque and we make no guarantees about the internal structure of it.
    ///
    ///
    /// [build script]: https://doc.rust-lang.org/cargo/reference/build-scripts.html
    pub fn store(&self) -> &[u8] {
        &self.compiled_component
    }

    /// Load pre-compiled component.
    ///
    /// # Safety
    /// The caller MUST ensure that the input is trusted, e.g. because it is part of the compilation pipeline or via
    /// some form of code signing. Loading untrusted input can lead to memory unsafety, data corruption and exposure,
    /// as well as remote code execution; in a similar way that executing an untrusted binary or calling [`dlopen`]
    /// on untrusted data would.
    ///
    /// # Version Stability
    /// You may feed pre-compiled data of older or newer version of [`datafusion_udf_wasm_host`](crate) or its
    /// dependencies. Doing so will lead to an error. It is safe to cache pre-compiled results and re-compile in the
    /// error case:
    ///
    /// ```
    /// # use datafusion_udf_wasm_host::WasmComponentPrecompiled;
    /// let res = unsafe {
    ///     WasmComponentPrecompiled::load(b"OLD".to_vec())
    /// };
    ///
    /// assert_eq!(
    ///     res.unwrap_err().to_string(),
    ///     "
    /// create WASM component
    /// caused by
    /// External error: failed to parse precompiled artifact as an ELF
    ///     ".trim(),
    /// );
    /// ```
    ///
    /// # Different Hosts
    /// It is safe to move a pre-compiled component from one host to another. However, loading may fail with an error
    /// if the source and target host have:
    /// - different architectures (e.g. `x64` vs `ARM64`, but also "has AVX512 vs no AVX512"),
    /// - different operating systems
    /// - different tunables or compilation flags
    /// - different WASM features
    ///
    ///
    /// [`dlopen`]: https://pubs.opengroup.org/onlinepubs/009696799/functions/dlopen.html
    pub unsafe fn load(data: Vec<u8>) -> DataFusionResult<Self> {
        let this = Self {
            compiled_component: data,
        };

        // test hydration
        let engine = create_engine(&CompilationFlags::default())?;
        this.hydrate(&engine)?;

        Ok(this)
    }

    /// Hydrate wasmtime component from raw data.
    fn hydrate(&self, engine: &Engine) -> DataFusionResult<Component> {
        let Self { compiled_component } = self;

        // SAFETY: Either we just produced this data ourselves within the same process (i.e. it is NOT external input)
        //         OR the API user promised us that the data is safe (see [`WasmComponentPrecompiled::load`]).
        let component_res = unsafe { Component::deserialize(engine, compiled_component) };
        let component = component_res.context("create WASM component", None)?;
        Ok(component)
    }
}

/// Stateful instance of a WASM component.
#[derive(Debug)]
pub(crate) struct WasmComponentInstance {
    /// Mutable state.
    ///
    /// This mostly contains [`WasmStateImpl`].
    store: Arc<Mutex<Store<WasmStateImpl>>>,

    /// Background task that keeps the WASM epoch timer running.
    #[expect(dead_code)]
    epoch_task: Arc<JoinSet<()>>,

    /// Timeout for blocking tasks.
    inplace_blocking_timeout: Duration,

    /// Trusted data limits.
    trusted_data_limits: TrustedDataLimits,

    /// WIT-based bindings that we resolved within the payload.
    bindings: IgnoreDebug<Arc<bindings::Datafusion>>,
}

impl WasmComponentInstance {
    /// Create new instance.
    pub(crate) async fn new(
        component: &WasmComponentPrecompiled,
        permissions: &WasmPermissions,
        io_rt: Handle,
        memory_pool: &Arc<dyn MemoryPool>,
    ) -> DataFusionResult<Self> {
        let engine = create_engine(&CompilationFlags::default())?;

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

        let component = component.hydrate(&engine)?;

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
            wasi_ctx: wasi_ctx_builder.build().into(),
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

        let store = Arc::new(Mutex::new(store));

        Ok(Self {
            store,
            epoch_task,
            inplace_blocking_timeout,
            trusted_data_limits: permissions.trusted_data_limits.clone(),
            bindings: Arc::clone(&bindings).into(),
        })
    }

    /// Get bindings.
    pub(crate) fn bindings(&self) -> &bindings::Datafusion {
        &self.bindings
    }

    /// Lock inner store.
    pub(crate) async fn lock_state(&self) -> LockedState {
        LockedState(Arc::clone(&self.store).lock_owned().await)
    }

    /// Timeout for blocking tasks.
    pub(crate) fn inplace_blocking_timeout(&self) -> Duration {
        self.inplace_blocking_timeout
    }

    /// Trusted data limits.
    pub(crate) fn trusted_data_limits(&self) -> &TrustedDataLimits {
        &self.trusted_data_limits
    }
}

/// Locked state.
pub(crate) struct LockedState(OwnedMutexGuard<Store<WasmStateImpl>>);

impl Deref for LockedState {
    type Target = WasmStateImpl;

    fn deref(&self) -> &Self::Target {
        self.0.deref().data()
    }
}

impl AsContext for LockedState {
    type Data = WasmStateImpl;

    fn as_context(&self) -> StoreContext<'_, Self::Data> {
        self.0.as_context()
    }
}

impl AsContextMut for LockedState {
    fn as_context_mut(&mut self) -> StoreContextMut<'_, Self::Data> {
        self.0.as_context_mut()
    }
}
