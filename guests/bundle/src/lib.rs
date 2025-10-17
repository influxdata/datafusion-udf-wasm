//! Bundles guests as pre-compiled WASM bytecode.

/// "add-one" example.
#[cfg(feature = "example")]
pub static BIN_EXAMPLE: &[u8] = include_bytes!(env!("BIN_PATH_EXAMPLE"));

/// Python UDF.
#[cfg(feature = "python")]
pub static BIN_PYTHON: &[u8] = include_bytes!(env!("BIN_PATH_PYTHON"));
