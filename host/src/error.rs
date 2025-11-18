//! Helper for simpler error handling.
use datafusion_common::DataFusionError;
use wasmtime_wasi::p2::FsError;

/// Extension for [`wasmtime::Error`].
pub(crate) trait WasmToDataFusionErrorExt {
    /// Add context to error.
    ///
    /// The context has:
    /// - `msg`: a human-readable context description
    /// - `stderr`: stderr output of the WASM payload if available
    fn context(self, msg: &str, stderr: Option<&[u8]>) -> DataFusionError;
}

impl WasmToDataFusionErrorExt for wasmtime::Error {
    fn context(self, msg: &str, stderr: Option<&[u8]>) -> DataFusionError {
        let mut context = msg.to_owned();

        if let Some(stderr) = stderr
            && !stderr.is_empty()
        {
            context.push_str(&format!("\n\nstderr:\n{}", String::from_utf8_lossy(stderr)));
        }

        // `anyhow` gives as a choice:
        // - keep the backtrace but don't allow error downcasting
        // - remove the backtrace and gain the ability to downcast error types
        //
        // Since users may want to turn error types into status codes (e.g. for gRPC / Flight), we should probably use
        // the latter option.
        DataFusionError::External(self.reallocate_into_boxed_dyn_error_without_backtrace())
            .context(context)
    }
}

/// Extension for [`Result`] containing a [`wasmtime::Error`].
pub(crate) trait WasmToDataFusionResultExt {
    /// [`Ok`] payload.
    type T;

    /// Add context to error.
    ///
    /// The context has:
    /// - `msg`: a human-readable context description
    /// - `stderr`: stderr output of the WASM payload if available
    fn context(self, msg: &str, stderr: Option<&[u8]>) -> Result<Self::T, DataFusionError>;
}

impl<T> WasmToDataFusionResultExt for Result<T, wasmtime::Error> {
    type T = T;

    fn context(self, msg: &str, stderr: Option<&[u8]>) -> Result<Self::T, DataFusionError> {
        self.map_err(|err| WasmToDataFusionErrorExt::context(err, msg, stderr))
    }
}

/// Extension trait for [`Result`] containing a [`DataFusionError`].
pub(crate) trait DataFusionResultExt {
    /// [`Ok`] payload.
    type T;

    /// Add description to error.
    ///
    /// See [`DataFusionError::context`].
    fn context(self, description: impl Into<String>) -> Result<Self::T, DataFusionError>;
}

impl<T, E> DataFusionResultExt for Result<T, E>
where
    E: Into<DataFusionError>,
{
    type T = T;

    fn context(self, description: impl Into<String>) -> Result<Self::T, DataFusionError> {
        self.map_err(|e| e.into().context(description))
    }
}

/// Failed allocation error.
#[derive(Debug, Clone)]
#[expect(missing_copy_implementations, reason = "allow later extensions")]
pub struct LimitExceeded {
    /// Name of the allocation type/resource.
    pub(crate) name: &'static str,

    /// Allocation limit.
    pub(crate) limit: u64,

    /// Current allocation size.
    pub(crate) current: u64,

    /// Requested/additional allocation.
    pub(crate) requested: u64,
}

impl std::fmt::Display for LimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            name,
            limit,
            current,
            requested,
        } = self;

        write!(
            f,
            "{name} limit reached: limit<={limit} current=={current} requested+={requested}"
        )
    }
}

impl std::error::Error for LimitExceeded {}

impl From<LimitExceeded> for std::io::Error {
    fn from(e: LimitExceeded) -> Self {
        Self::new(std::io::ErrorKind::QuotaExceeded, e.to_string())
    }
}

impl From<LimitExceeded> for FsError {
    fn from(e: LimitExceeded) -> Self {
        let e: std::io::Error = e.into();
        e.into()
    }
}
