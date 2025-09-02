use datafusion_common::DataFusionError;

pub(crate) trait WasmToDataFusionErrorExt {
    fn context(self, msg: &str, stderr: Option<&[u8]>) -> DataFusionError;
}

impl WasmToDataFusionErrorExt for wasmtime::Error {
    fn context(self, msg: &str, stderr: Option<&[u8]>) -> DataFusionError {
        let mut context = msg.to_owned();

        if let Some(stderr) = stderr {
            context.push_str(&format!("\n\nstderr:\n{}", String::from_utf8_lossy(stderr)));
        }

        DataFusionError::External(self.into_boxed_dyn_error()).context(context)
    }
}

pub(crate) trait WasmToDataFusionResultExt {
    type T;

    fn context(self, msg: &str, stderr: Option<&[u8]>) -> Result<Self::T, DataFusionError>;
}

impl<T> WasmToDataFusionResultExt for Result<T, wasmtime::Error> {
    type T = T;

    fn context(self, msg: &str, stderr: Option<&[u8]>) -> Result<Self::T, DataFusionError> {
        self.map_err(|err| WasmToDataFusionErrorExt::context(err, msg, stderr))
    }
}

pub(crate) trait DataFusionResultExt {
    type T;

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
