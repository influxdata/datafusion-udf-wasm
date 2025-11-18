//! Methods used by multiple payloads.

use std::sync::Arc;

use datafusion_common::Result as DataFusionResult;
use datafusion_expr::ScalarUDFImpl;

/// Return empty root file system.
///
/// This always returns [`None`] because the example does not need any files.
pub(crate) fn root_empty() -> Option<Vec<u8>> {
    None
}

/// Returns empty list of UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs_empty(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![])
}
