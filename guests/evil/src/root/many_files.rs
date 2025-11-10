//! Evil payloads that creates A LOT of files.
use std::sync::Arc;

use datafusion_common::Result as DataFusionResult;
use datafusion_expr::ScalarUDFImpl;

/// Return root file system.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn root() -> Option<Vec<u8>> {
    let mut ar = tar::Builder::new(Vec::new());

    const LIMIT: u64 = 10_000;
    for i in 0..=LIMIT {
        let mut header = tar::Header::new_gnu();
        header.set_path(i.to_string()).unwrap();
        header.set_size(0);
        header.set_cksum();

        ar.append(&header, b"".as_slice()).unwrap();
    }

    Some(ar.into_inner().unwrap())
}

/// Returns UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![])
}
