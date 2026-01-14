//! Create many UDFs.
use std::sync::Arc;

use datafusion_common::Result as DataFusionResult;
use datafusion_expr::ScalarUDFImpl;

use crate::complex::TestUdf;

/// Returns our evil UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    let limit: usize = std::env::var("limit").unwrap().parse().unwrap();

    Ok((0..=limit)
        .map(|i| {
            Arc::new(TestUdf {
                name: i.to_string(),
                ..Default::default()
            }) as _
        })
        .collect())
}
