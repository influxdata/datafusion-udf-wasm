//! Payload that spins while retrieving the root filesystem.
use std::sync::Arc;

use datafusion_common::Result as DataFusionResult;
use datafusion_expr::ScalarUDFImpl;

use crate::spin::spin;

/// Return root file system.
///
/// This always returns [`None`] because the example does not need any files.
pub(crate) fn root() -> Option<Vec<u8>> {
    spin();
    None
}

/// Returns our evil UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![])
}
