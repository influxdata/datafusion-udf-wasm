//! Limiter for conversion of untrusted data.
use std::{cell::RefCell, rc::Rc};

use datafusion_common::{DataFusionError, error::Result as DataFusionResult};

/// Limits that should be applied during conversion from untrusted to trusted data.
#[derive(Debug, Clone)]
#[expect(missing_copy_implementations, reason = "allow later extensions")]
pub struct TrustedDataLimits {
    /// Maximum length of identifiers like names, in bytes.
    ///
    /// Also see [`max_aux_string_length`](Self::max_aux_string_length).
    pub max_identifier_length: usize,

    /// Maximum length of auxiliary strings like error messages or metadata, in bytes.
    ///
    /// Does NOT apply to identifiers (see [`max_identifier_length`](Self::max_identifier_length)) or payload data
    /// like string arrays.
    pub max_aux_string_length: usize,

    /// Maximum data structure depth.
    ///
    /// # Data Structures
    /// Note that we do NOT support cyclic or self-referential data structures.
    ///
    /// What counts as an item is subject to interpretation, but the roughly the following things ARE considered
    /// individual items:
    ///
    /// - members of a struct
    /// - members of an array/vec
    /// - parts of a tuple
    ///
    /// The following things are NOT considered individual items:
    ///
    /// - individual characters/bytes of a string: only the string counts as one item, see
    ///   [`max_identifier_length`](Self::max_identifier_length) and
    ///   [`max_aux_string_length`](Self::max_aux_string_length) for limitations
    /// - individual bytes of a vec or "bytes": only the collection counts
    ///
    /// # Example
    /// The following tree has depth of 2:
    ///
    /// ```text
    ///    o
    ///    |
    /// +--+--+
    /// |     |
    /// o     o
    /// ```
    ///
    /// The following tree has depth of 4:
    ///
    /// ```text
    ///    o
    ///    |
    /// +--+--+
    /// |     |
    /// o     o
    /// |     |
    /// |   +-+-+-+
    /// |   |   | |
    /// o   o   o o
    ///           |
    ///           |
    ///           |
    ///           o
    /// ```
    pub max_depth: u64,

    /// Maximum data structure complexity.
    ///
    /// Complexity describes the number of "items" in a nested data structure, see [example](#example) and
    /// [`max_depth`](Self::max_depth) for more details.
    ///
    /// # Example
    /// The following tree has complexity of 3:
    ///
    /// ```text
    ///    o
    ///    |
    /// +--+--+
    /// |     |
    /// o     o
    /// ```
    ///
    /// The following tree has complexity of 8:
    ///
    /// ```text
    ///    o
    ///    |
    /// +--+--+
    /// |     |
    /// o     o
    /// |     |
    /// |   +-+-+-+
    /// |   |   | |
    /// o   o   o o
    ///           |
    ///           |
    ///           |
    ///           o
    /// ```
    pub max_complexity: u64,
}

impl Default for TrustedDataLimits {
    fn default() -> Self {
        Self {
            max_identifier_length: 50,
            max_aux_string_length: 10_000,
            max_depth: 10,
            max_complexity: 100,
        }
    }
}

/// Counter for complexity.
struct ComplexityCounter {
    /// Limits.
    limits: TrustedDataLimits,

    /// Current complexity.
    current_complexity: u64,
}

/// A token to count complexity of untrusted data.
pub(crate) struct ComplexityToken {
    /// Root.
    counter: Rc<RefCell<ComplexityCounter>>,

    /// Current depth.
    current_depth: u64,
}

impl ComplexityToken {
    /// Create new token.
    ///
    /// This is a private method for internal use.
    fn new_inner(
        counter: Rc<RefCell<ComplexityCounter>>,
        parent_depth: u64,
    ) -> DataFusionResult<Self> {
        let mut counter_guard = counter.borrow_mut();
        let d = parent_depth.saturating_add(1);
        assert!(d <= counter_guard.limits.max_depth);
        if d == counter_guard.limits.max_depth {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "data structure depth: limit={}",
                counter_guard.limits.max_depth
            )));
        }

        let c = counter_guard.current_complexity.saturating_add(1);
        assert!(c <= counter_guard.limits.max_complexity);
        if c == counter_guard.limits.max_complexity {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "data structure complexity: limit={}",
                counter_guard.limits.max_complexity
            )));
        }
        counter_guard.current_complexity = c;
        drop(counter_guard);

        Ok(Self {
            counter,
            current_depth: d,
        })
    }

    /// Create new counter from limits.
    pub(crate) fn new(limits: TrustedDataLimits) -> DataFusionResult<Self> {
        let counter = Rc::new(RefCell::new(ComplexityCounter {
            limits,
            current_complexity: 0,
        }));
        Self::new_inner(counter, 0)
    }

    /// Create token for sub child data structure.
    pub(crate) fn sub(&self) -> DataFusionResult<Self> {
        Self::new_inner(Rc::clone(&self.counter), self.current_depth)
    }

    /// Explicitly express that this serialization path has NO recursion.
    pub(crate) fn no_recursion(self) {
        drop(self);
    }

    /// Check identifier using [`TrustedDataLimits::max_identifier_length`].
    pub(crate) fn check_identifier(&self, id: &str) -> DataFusionResult<()> {
        let len = id.len();
        let limit = self.counter.borrow().limits.max_identifier_length;
        if len > limit {
            Err(DataFusionError::ResourcesExhausted(format!(
                "identifier length: got={len}, limit={limit}"
            )))
        } else {
            Ok(())
        }
    }

    /// Checks string in error messages, metadata, etc.
    ///
    /// Does NOT check the actual data string, e.g. in string arrays.
    pub(crate) fn check_aux_string(&self, s: &str) -> DataFusionResult<()> {
        let len = s.len();
        let limit = self.counter.borrow().limits.max_aux_string_length;
        if len > limit {
            Err(DataFusionError::ResourcesExhausted(format!(
                "auxiliary string length: got={len}, limit={limit}"
            )))
        } else {
            Ok(())
        }
    }
}

/// A conversion from untrusted to trusted data.
///
/// This is similar to [`TryFrom`] but:
///
/// - the error is always [`DataFusionError`]
/// - it takes [`ComplexityToken`] to check limits
pub(crate) trait CheckedFrom<T>: Sized {
    /// Try to convert untrusted to trusted data.
    fn checked_from(value: T, token: ComplexityToken) -> DataFusionResult<Self>;
}

/// The [`TryInto`] version of [`CheckedInto`].
pub(crate) trait CheckedInto<T>: Sized {
    /// Try to convert untrusted to trusted data.
    ///
    /// Automatically creates a sub-token.
    fn checked_into(self, token: &ComplexityToken) -> DataFusionResult<T>;

    /// Try to convert untrusted to trusted data.
    ///
    /// Automatically creates a root token.
    fn checked_into_root(self, limits: &TrustedDataLimits) -> DataFusionResult<T>;
}

impl<T, U> CheckedInto<U> for T
where
    U: CheckedFrom<T>,
{
    fn checked_into(self, token: &ComplexityToken) -> DataFusionResult<U> {
        U::checked_from(self, token.sub()?)
    }

    fn checked_into_root(self, limits: &TrustedDataLimits) -> DataFusionResult<U> {
        U::checked_from(self, ComplexityToken::new(limits.clone())?)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_tree() {
        let tree = tree();

        insta::assert_snapshot!(
            tree,
            @r"
        O--+--O--+--O
           |
           +--O--+--O
                 |
                 +--O
                 |
                 +--O--+--O
        ",
        );
    }

    #[test]
    fn test_ok() {
        let mut tree = tree();

        <()>::checked_from(
            &mut tree,
            ComplexityToken::new(TrustedDataLimits::default()).unwrap(),
        )
        .unwrap();

        insta::assert_snapshot!(
            tree,
            @r"
        X--+--X--+--X
           |
           +--X--+--X
                 |
                 +--X
                 |
                 +--X--+--X
        ",
        );
    }

    #[test]
    fn test_err_depth() {
        let limit = 4;

        // check that limit + 1 works
        <()>::checked_from(
            &mut tree(),
            ComplexityToken::new(TrustedDataLimits {
                max_depth: limit + 1,
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap();

        let mut tree = tree();

        let err = <()>::checked_from(
            &mut tree,
            ComplexityToken::new(TrustedDataLimits {
                max_depth: limit,
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap_err();

        insta::assert_snapshot!(
            err,
            @"Resources exhausted: data structure depth: limit=4",
        );

        insta::assert_snapshot!(
            tree,
            @r"
        X--+--X--+--X
           |
           +--X--+--X
                 |
                 +--X
                 |
                 +--X--+--O
        ",
        );
    }

    #[test]
    fn test_err_complexity() {
        let limit = 8;

        // check that limit + 1 works
        <()>::checked_from(
            &mut tree(),
            ComplexityToken::new(TrustedDataLimits {
                max_complexity: limit + 1,
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap();

        let mut tree = tree();

        let err = <()>::checked_from(
            &mut tree,
            ComplexityToken::new(TrustedDataLimits {
                max_complexity: limit,
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap_err();

        insta::assert_snapshot!(
            err,
            @"Resources exhausted: data structure complexity: limit=8",
        );

        insta::assert_snapshot!(
            tree,
            @r"
        X--+--X--+--X
           |
           +--X--+--X
                 |
                 +--X
                 |
                 +--X--+--O
        ",
        );
    }

    /// Example tree.
    fn tree() -> Node {
        Node::new(vec![
            Node::new(vec![Node::new(vec![])]),
            Node::new(vec![
                Node::new(vec![]),
                Node::new(vec![]),
                Node::new(vec![Node::new(vec![])]),
            ]),
        ])
    }

    /// Test data structure.
    struct Node {
        children: Vec<Self>,
        visited: bool,
    }

    impl Node {
        fn new(children: Vec<Self>) -> Self {
            Self {
                children,
                visited: false,
            }
        }

        fn print(&self) -> String {
            let me = if self.visited {
                "X".to_owned()
            } else {
                "O".to_owned()
            };

            if self.children.is_empty() {
                me
            } else {
                let mut out = String::new();

                for (i_child, child) in self.children.iter().enumerate() {
                    let first_child = i_child == 0;

                    if !first_child {
                        out.push_str("   |\n");
                    }

                    for (i_line, line) in child.print().lines().enumerate() {
                        let first_line = i_line == 0;

                        if first_line {
                            if first_child {
                                out.push_str(&format!("{me}--+--{line}\n"));
                            } else {
                                out.push_str(&format!("   +--{line}\n"));
                            }
                        } else {
                            out.push_str(&format!("      {line}\n"));
                        }
                    }
                }

                out
            }
        }
    }

    impl std::fmt::Display for Node {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.print())
        }
    }

    impl CheckedFrom<&mut Node> for () {
        fn checked_from(value: &mut Node, token: ComplexityToken) -> DataFusionResult<Self> {
            value.visited = true;
            for child in &mut value.children {
                <()>::checked_from(child, token.sub()?)?;
            }
            Ok(())
        }
    }
}
