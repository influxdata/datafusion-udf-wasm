//! Module for UDF code formatting implementations

/// Trait for formatting UDF code before compilation allows for
/// language-specific formatting or preprocessing.
pub trait UdfCodeFormatter: std::fmt::Debug + Send + Sync {
    /// Format the given UDF code string
    fn format(&self, code: String) -> String;
}

/// Default implementation that returns code unchanged
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpFormatter;

impl UdfCodeFormatter for NoOpFormatter {
    fn format(&self, code: String) -> String {
        code
    }
}

/// Code formatter that strips leading indentation
#[derive(Debug, Default, Clone, Copy)]
pub struct StripIndentationFormatter;

impl UdfCodeFormatter for StripIndentationFormatter {
    fn format(&self, code: String) -> String {
        strip_indentation(&code)
    }
}

/// Strips common leading indentation from all non-empty lines in the code string.
fn strip_indentation(code: &str) -> String {
    let indent = code
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.chars().take_while(|s| s.is_ascii_whitespace()).count())
        .min()
        .unwrap_or_default();

    code.lines()
        .flat_map(|l| l.chars().skip(indent).chain(std::iter::once('\n')))
        .collect::<String>()
}
