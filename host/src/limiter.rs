//! Resource limiter.

use std::sync::Arc;

use anyhow::Context;
use datafusion_common::DataFusionError;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use std::sync::Mutex;
use wasmtime::ResourceLimiter;
use wasmtime_wasi::TrappableError;

use crate::error::LimitExceeded;

/// Static resource limits.
#[derive(Debug, Clone)]
#[expect(missing_copy_implementations, reason = "allow later extensions")]
pub struct StaticResourceLimits {
    /// Number of instances.
    pub n_instances: usize,

    /// Number of tables.
    ///
    /// Each table is limited two ways:
    ///
    /// - the number of elements per table, see [`n_elements_per_table`](Self::n_elements_per_table)
    /// - the [DataFusion memory system](datafusion_execution::memory_pool), since every table element needs some space
    pub n_tables: usize,

    /// Maximum number of elements per table.
    ///
    /// There are [`n_tables`](Self::n_tables) tables.
    pub n_elements_per_table: usize,

    /// Number of linear memories.
    ///
    /// The entirety of memory is limited in size by the [DataFusion memory system](datafusion_execution::memory_pool).
    pub n_memories: usize,
}

impl Default for StaticResourceLimits {
    fn default() -> Self {
        Self {
            n_instances: wasmtime::DEFAULT_INSTANCE_LIMIT,
            n_tables: wasmtime::DEFAULT_TABLE_LIMIT,
            n_elements_per_table: 100_000,
            n_memories: wasmtime::DEFAULT_MEMORY_LIMIT,
        }
    }
}

/// Resource limiter.
#[derive(Debug)]
pub(crate) struct Limiter {
    /// DataFusion memory reservation.
    ///
    /// This is ONLY used for bytes, not for any other resources.
    memory_reservation: Mutex<MemoryReservation>,

    /// Limits.
    limits: StaticResourceLimits,
}

impl Limiter {
    /// Create new limiter.
    pub(crate) fn new(limits: StaticResourceLimits, pool: &Arc<dyn MemoryPool>) -> Self {
        let memory_reservation =
            Mutex::new(MemoryConsumer::new("WASM UDF resources").register(pool));

        Self {
            memory_reservation,
            limits,
        }
    }

    /// Grow memory usage.
    pub(crate) fn grow(&self, bytes: usize) -> Result<(), GrowthError> {
        self.memory_reservation
            .lock()
            .unwrap()
            .try_grow(bytes)
            .map_err(|e| {
                log::debug!("failed to grow memory: {e}");
                GrowthError(e)
            })
    }

    /// Shrink memory usage.
    pub(crate) fn shrink(&self, bytes: usize) {
        self.memory_reservation.lock().unwrap().shrink(bytes);
    }

    /// Get current allocation size.
    pub(crate) fn size(&self) -> usize {
        self.memory_reservation.lock().unwrap().size()
    }

    /// Inner implementation of [`ResourceLimiter::table_growing`]
    fn table_growing_inner(&mut self, current: usize, desired: usize) -> anyhow::Result<()> {
        if desired > self.limits.n_elements_per_table {
            return Err(anyhow::Error::new(LimitExceeded {
                name: "table elements",
                limit: self.limits.n_elements_per_table as u64,
                current: current as u64,
                requested: desired as u64,
            }));
        }

        // every element needs a pointer to be stored
        let growth_n = desired
            .checked_sub(current)
            .expect("wasmtime accounting correct");
        let growth_mem = std::mem::size_of::<usize>()
            .checked_mul(growth_n)
            .context("memory usage overflow for table elements")?;

        // Note on error handling: since we didn't modify anything so far, failing here is OK. We don't need to roll
        // back state.
        self.grow(growth_mem).map_err(|e| e.0)?;

        // success
        Ok(())
    }
}

impl ResourceLimiter for Limiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        // NOTE: we don't care if the memory is bound or unbounded
        _maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        let growth = desired
            .checked_sub(current)
            .expect("wasmtime accounting correct");

        match self.grow(growth) {
            Ok(()) => Ok(true),
            Err(e) => {
                log::debug!("memory growth failed: {}", e.0);
                // reject allocation but do NOT trap
                Ok(false)
            }
        }
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        // NOTE: we don't care if the table is bound or unbounded
        _maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        match self.table_growing_inner(current, desired) {
            Ok(()) => Ok(true),
            Err(e) => {
                log::debug!("table growth failed: {e}");
                // reject allocation but do NOT trap
                Ok(false)
            }
        }
    }

    fn memory_grow_failed(&mut self, error: anyhow::Error) -> anyhow::Result<()> {
        log::debug!("memory growth failed: {error}");

        // reject allocation but do NOT trap
        Ok(())
    }

    fn table_grow_failed(&mut self, error: anyhow::Error) -> anyhow::Result<()> {
        log::debug!("table growth failed: {error}");

        // reject allocation but do NOT trap
        Ok(())
    }

    fn instances(&self) -> usize {
        self.limits.n_instances
    }

    fn tables(&self) -> usize {
        self.limits.n_tables
    }

    fn memories(&self) -> usize {
        self.limits.n_memories
    }
}

/// Error during memory growth.
///
/// This is similar to [`LimitExceeded`] but contains an opaque [`DataFusionError`].
#[derive(Debug)]
pub(crate) struct GrowthError(DataFusionError);

impl From<GrowthError> for anyhow::Error {
    fn from(e: GrowthError) -> Self {
        Self::new(e.0)
    }
}

impl From<GrowthError> for DataFusionError {
    fn from(e: GrowthError) -> Self {
        e.0
    }
}

impl From<GrowthError> for std::io::Error {
    fn from(e: GrowthError) -> Self {
        Self::new(std::io::ErrorKind::QuotaExceeded, e.0)
    }
}

impl From<GrowthError>
    for TrappableError<wasmtime_wasi::p2::bindings::filesystem::types::ErrorCode>
{
    fn from(e: GrowthError) -> Self {
        Self::trap(Box::new(e.0))
    }
}
