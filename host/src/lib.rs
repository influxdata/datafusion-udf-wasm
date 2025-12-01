//! Host-code for WebAssembly-based [DataFusion] UDFs.
//!
//!
//! [DataFusion]: https://datafusion.apache.org/
use std::{any::Any, collections::HashSet, hash::Hash, sync::Arc};

use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_execution::memory_pool::MemoryPool;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl},
};
use tokio::runtime::Handle;
use uuid::Uuid;
use wasmtime::component::ResourceAny;
use wasmtime_wasi::async_trait;

use crate::{
    bindings::exports::datafusion_udf_wasm::udf::types as wit_types,
    component::WasmComponentInstance,
    conversion::limits::{CheckedInto, ComplexityToken},
    error::{DataFusionResultExt, WasmToDataFusionResultExt, WitDataFusionResultExt},
    tokio_helpers::async_in_sync_context,
};

pub use crate::{
    component::WasmComponentPrecompiled,
    conversion::limits::TrustedDataLimits,
    http::{
        AllowCertainHttpRequests, HttpMethod, HttpRequestMatcher, HttpRequestRejected,
        HttpRequestValidator, RejectAllHttpRequests,
    },
    limiter::StaticResourceLimits,
    permissions::WasmPermissions,
    vfs::limits::VfsLimits,
};

// unused-crate-dependencies false positives
#[cfg(test)]
use datafusion_udf_wasm_bundle as _;
#[cfg(test)]
use regex as _;
#[cfg(test)]
use wiremock as _;

mod bindings;
mod component;
mod conversion;
mod error;
mod http;
mod ignore_debug;
mod limiter;
mod linker;
mod permissions;
mod state;
mod tokio_helpers;
mod vfs;

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
#[derive(Debug)]
pub struct WasmScalarUdf {
    /// WASM component instance.
    instance: Arc<WasmComponentInstance>,

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
        let instance =
            Arc::new(WasmComponentInstance::new(component, permissions, io_rt, memory_pool).await?);

        let udf_resources = {
            let mut state = instance.lock_state().await;
            instance
                .bindings()
                .datafusion_udf_wasm_udf_types()
                .call_scalar_udfs(&mut state, &source)
                .await
                .context(
                    "calling scalar_udfs() method failed",
                    Some(&state.stderr.contents()),
                )?
                .convert_err(permissions.trusted_data_limits.clone())
                .context("scalar_udfs")?
        };
        if udf_resources.len() > permissions.max_udfs {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "guest returned too many UDFs: got={}, limit={}",
                udf_resources.len(),
                permissions.max_udfs,
            )));
        }

        let mut udfs = Vec::with_capacity(udf_resources.len());
        let mut names_seen = HashSet::with_capacity(udf_resources.len());
        for resource in udf_resources {
            let mut state = instance.lock_state().await;
            let name = instance
                .bindings()
                .datafusion_udf_wasm_udf_types()
                .scalar_udf()
                .call_name(&mut state, resource)
                .await
                .context("call ScalarUdf::name", Some(&state.stderr.contents()))?;
            ComplexityToken::new(permissions.trusted_data_limits.clone())?
                .check_identifier(&name)
                .context("UDF name")?;
            if !names_seen.insert(name.clone()) {
                return Err(DataFusionError::External(
                    format!("non-unique UDF name: '{name}'").into(),
                ));
            }

            let signature: Signature = instance
                .bindings()
                .datafusion_udf_wasm_udf_types()
                .scalar_udf()
                .call_signature(&mut state, resource)
                .await
                .context("call ScalarUdf::signature", Some(&state.stderr.contents()))?
                .checked_into_root(&permissions.trusted_data_limits)?;

            let return_type = match &signature.type_signature {
                TypeSignature::Exact(t) => {
                    let r = instance
                        .bindings()
                        .datafusion_udf_wasm_udf_types()
                        .scalar_udf()
                        .call_return_type(
                            &mut state,
                            resource,
                            &t.iter()
                                .map(|dt| wit_types::DataType::from(dt.clone()))
                                .collect::<Vec<_>>(),
                        )
                        .await
                        .context(
                            "call ScalarUdf::return_type",
                            Some(&state.stderr.contents()),
                        )?
                        .convert_err(permissions.trusted_data_limits.clone())?;
                    Some(r.checked_into_root(&permissions.trusted_data_limits)?)
                }
                _ => None,
            };

            udfs.push(Self {
                instance: Arc::clone(&instance),
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
                let mut state = self.instance.lock_state().await;
                let return_type = self
                    .instance
                    .bindings()
                    .datafusion_udf_wasm_udf_types()
                    .scalar_udf()
                    .call_return_type(&mut state, self.resource, &arg_types)
                    .await
                    .context(
                        "call ScalarUdf::return_type",
                        Some(&state.stderr.contents()),
                    )?
                    .convert_err(self.instance.trusted_data_limits().clone())?;
                return_type.checked_into_root(self.instance.trusted_data_limits())
            },
            self.instance.inplace_blocking_timeout(),
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
        let mut state = self.instance.lock_state().await;
        let return_type = self
            .instance
            .bindings()
            .datafusion_udf_wasm_udf_types()
            .scalar_udf()
            .call_invoke_with_args(&mut state, self.resource, &args)
            .await
            .context(
                "call ScalarUdf::invoke_with_args",
                Some(&state.stderr.contents()),
            )?
            .convert_err(self.instance.trusted_data_limits().clone())?;

        match return_type.checked_into_root(self.instance.trusted_data_limits()) {
            Ok(ColumnarValue::Scalar(scalar)) => Ok(ColumnarValue::Scalar(scalar)),
            Ok(ColumnarValue::Array(array)) if array.len() as u64 != args.number_rows => {
                Err(DataFusionError::External(
                    format!(
                        "UDF returned array of length {} but should produce {} rows",
                        array.len(),
                        args.number_rows
                    )
                    .into(),
                ))
            }
            Ok(ColumnarValue::Array(array)) => Ok(ColumnarValue::Array(array)),
            Err(e) => Err(e),
        }
    }
}
