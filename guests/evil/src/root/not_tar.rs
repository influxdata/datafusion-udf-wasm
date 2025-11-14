//! Evil payloads that reports bytes that are NOT a TAR file.
use std::sync::Arc;

use datafusion_common::Result as DataFusionResult;
use datafusion_expr::ScalarUDFImpl;

/// Return root file system.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn root() -> Option<Vec<u8>> {
    Some(b"foo".to_vec())
}

/// Returns UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![])
}
