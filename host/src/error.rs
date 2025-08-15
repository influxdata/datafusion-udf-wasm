use datafusion::error::DataFusionError;

pub(crate) trait WasmToDataFusionErrorExt {
    fn context(self, msg: &str) -> DataFusionError;
}

impl WasmToDataFusionErrorExt for wasmtime::Error {
    fn context(self, msg: &str) -> DataFusionError {
        DataFusionError::External(self.into_boxed_dyn_error()).context(msg)
    }
}

pub(crate) trait WasmToDataFusionResultExt {
    type T;

    fn context(self, msg: &str) -> Result<Self::T, DataFusionError>;
}

impl<T> WasmToDataFusionResultExt for Result<T, wasmtime::Error> {
    type T = T;

    fn context(self, msg: &str) -> Result<Self::T, DataFusionError> {
        self.map_err(|err| WasmToDataFusionErrorExt::context(err, msg))
    }
}
