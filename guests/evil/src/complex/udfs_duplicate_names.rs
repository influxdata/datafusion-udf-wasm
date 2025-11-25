//! Duplicate UDF names.
use std::sync::Arc;

use datafusion_common::Result as DataFusionResult;
use datafusion_expr::ScalarUDFImpl;

use crate::complex::NamedUdf;

/// Returns our evil UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![
        Arc::new(NamedUdf("foo".to_owned())),
        Arc::new(NamedUdf("bar".to_owned())),
        Arc::new(NamedUdf("foo".to_owned())),
    ])
}
