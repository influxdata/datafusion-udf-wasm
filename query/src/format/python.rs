use crate::format::UdfCodeFormatter;

/// Python code formatter for UDF code
#[derive(Debug, Clone, Copy)]
pub struct PythonCodeFormatter;

impl UdfCodeFormatter for PythonCodeFormatter {
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
