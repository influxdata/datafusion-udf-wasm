//! Payload that spins while retrieving the root filesystem.
use datafusion_common::Result as DataFusionResult;
use datafusion_expr::ScalarUDFImpl;
use std::sync::Arc;

use crate::spin::spin;

/// Return root file system.
///
/// This always returns [`None`] because the example does not need any files.
#[expect(
    clippy::unnecessary_wraps,
    reason = "function signature required for evil UDFs"
)]
pub(crate) fn root(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    spin();
    Ok(vec![])
}
