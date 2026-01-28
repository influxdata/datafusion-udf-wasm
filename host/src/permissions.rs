//! Permission for guests.

use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc, time::Duration};

use crate::{
    HttpRequestValidator, RejectAllHttpRequests, StaticResourceLimits, TrustedDataLimits, VfsLimits,
};

/// Permissions for a WASM component.
#[derive(Debug)]
pub struct WasmPermissions {
    /// Epoch tick time.
    pub(crate) epoch_tick_time: Duration,

    /// Timeout for blocking tasks, in number of [ticks](Self::epoch_tick_time).
    ///
    /// This is stored as tick count instead of a total timeout, since these two variables should be chosen to be
    /// somewhat consistent. E.g. it makes little sense to massively increase the epoch tick time without also
    /// increasing the timeout.
    pub(crate) inplace_blocking_max_ticks: u32,

    /// Validator for HTTP requests.
    pub(crate) http: Arc<dyn HttpRequestValidator>,

    /// Virtual file system limits.
    pub(crate) vfs: VfsLimits,

    /// Limit of the stored stderr data.
    pub(crate) stderr_bytes: usize,

    /// Static resource limits.
    pub(crate) resource_limits: StaticResourceLimits,

    /// Trusted data limits.
    pub(crate) trusted_data_limits: TrustedDataLimits,

    /// Maximum number of UDFs.
    pub(crate) max_udfs: usize,

    /// Maximum number of cached [`Field`]s.
    ///
    ///
    /// [`Field`]: arrow::datatypes::Field
    pub(crate) max_cached_fields: NonZeroUsize,

    /// Maximum number of cached [`ConfigOptions`].
    ///
    ///
    /// [`ConfigOptions`]: datafusion_common::config::ConfigOptions
    pub(crate) max_cached_config_options: NonZeroUsize,

    /// Environment variables.
    pub(crate) envs: BTreeMap<String, String>,
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
            trusted_data_limits: TrustedDataLimits::default(),
            max_udfs: 20,
            max_cached_fields: NonZeroUsize::new(1_000).expect("valid value"),
            max_cached_config_options: NonZeroUsize::new(1).expect("valid value"),
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
    ///
    ///
    /// [`MemoryPool`]: datafusion_execution::memory_pool::MemoryPool
    pub fn with_resource_limits(self, limits: StaticResourceLimits) -> Self {
        Self {
            resource_limits: limits,
            ..self
        }
    }

    /// Set trusted data limits.
    pub fn with_trusted_data_limits(self, limits: TrustedDataLimits) -> Self {
        Self {
            trusted_data_limits: limits,
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

    /// Get the maximum number of UDFs that a payload/guest can produce.
    pub fn max_udfs(&self) -> usize {
        self.max_udfs
    }

    /// Set the maximum number of UDFs that a payload/guest can produce.
    pub fn with_max_udfs(self, limit: usize) -> Self {
        Self {
            max_udfs: limit,
            ..self
        }
    }

    /// Maximum number of cached [`Field`]s.
    ///
    ///
    /// [`Field`]: arrow::datatypes::Field
    pub fn with_max_cached_fields(self, limit: NonZeroUsize) -> Self {
        Self {
            max_cached_fields: limit,
            ..self
        }
    }

    /// Maximum number of cached [`ConfigOptions`].
    ///
    ///
    /// [`ConfigOptions`]: datafusion_common::config::ConfigOptions
    pub fn with_max_cached_config_options(self, limit: NonZeroUsize) -> Self {
        Self {
            max_cached_config_options: limit,
            ..self
        }
    }

    /// Add environment variable.
    pub fn with_env(mut self, key: String, value: String) -> Self {
        self.envs.insert(key, value);
        self
    }
}
