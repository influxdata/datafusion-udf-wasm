//! UDF with many parameters.

use std::sync::Arc;

use datafusion_common::Result as DataFusionResult;
use datafusion_expr::{ScalarUDFImpl, Signature, TypeSignature, Volatility};

use crate::complex::TestUdf;

/// Returns our evil UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    let limit: usize = std::env::var("limit").unwrap().parse().unwrap();

    Ok(vec![Arc::new(TestUdf {
        signature: Signature {
            type_signature: TypeSignature::Uniform(limit + 1, vec![]),
            volatility: Volatility::Immutable,
            parameter_names: Some((0..=limit).map(|i| format!("p{i}")).collect()),
        },
        ..Default::default()
    })])
}
