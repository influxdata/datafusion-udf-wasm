//! Long UDF names.
use std::sync::Arc;

use datafusion_common::Result as DataFusionResult;
use datafusion_expr::ScalarUDFImpl;

use crate::complex::NamedUdf;

/// Returns our evil UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    let limit: usize = std::env::var("limit").unwrap().parse().unwrap();
    let name = std::iter::repeat_n('x', limit + 1).collect::<String>();

    Ok(vec![
        // Emit twice. This shouldn't trigger the "duplicate names" detection though because the length MUST be
        // checked BEFORE potentially hashing the names.
        Arc::new(NamedUdf(name.clone())),
        Arc::new(NamedUdf(name)),
    ])
}
