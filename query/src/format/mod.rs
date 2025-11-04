/// Module for UDF code formatting implementations
pub mod python;

/// Trait for formatting UDF code before compilation allows for
/// language-specific formatting or preprocessing.
pub trait UdfCodeFormatter: Send + Sync {
    /// Format the given UDF code string
    fn format(&self, code: String) -> String;
}

/// Default implementation that returns code unchanged
#[derive(Debug, Clone, Copy)]
pub struct NoOpFormatter;

impl UdfCodeFormatter for NoOpFormatter {
    fn format(&self, code: String) -> String {
        code
    }
}
