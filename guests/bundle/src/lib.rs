//! Bundles guests as pre-compiled WASM bytecode.

// Suppress "unused crate dependency" warning
//
// Note that this isn't required for `datafusion_udf_wasm_python` because it is marked as `cdylib`.
#[cfg(feature = "example")]
use datafusion_udf_wasm_guest as _;

/// "add-one" example.
#[cfg(feature = "example")]
pub static BIN_EXAMPLE: &[u8] = include_bytes!(env!("BIN_PATH_EXAMPLE"));

/// Python UDF.
#[cfg(feature = "python")]
pub static BIN_PYTHON: &[u8] = include_bytes!(env!("BIN_PATH_PYTHON"));
