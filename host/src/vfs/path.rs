//! Path types and parsing.

use std::{io::ErrorKind, ops::Deref};

use crate::{error::LimitExceeded, vfs::VfsLimits};

/// Path segment.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct PathSegment(
    // we use a `Box<str>` (= pointer + size) instead of a `String` (pointer + size + capacity) since:
    //
    // - segments are immutable
    // - we want a tight/exact allocation
    Box<str>,
);

impl PathSegment {
    /// Create new path segment.
    ///
    /// # Error
    /// Fails if the segment is [too long](VfsLimits::max_path_segment_size).
    ///
    /// # Panic
    /// The caller MUST ensure the following properties, otherwise this method panics:
    ///
    /// - the segment MUST NOT contain a NULL character
    /// - the segment MUST NOT contain a slash `/` character
    /// - the segment MUST NOT be empty
    pub(crate) fn new(s: &str, limit: &VfsLimits) -> Result<Self, LimitExceeded> {
        assert!(!s.contains('\0'));
        assert!(!s.contains('/'));
        assert!(!s.is_empty());

        let len = s.len() as u64;
        if len > limit.max_path_segment_size {
            return Err(LimitExceeded {
                name: "path segment",
                limit: limit.max_path_segment_size,
                current: 0,
                requested: len,
            });
        }

        Ok(Self(s.into()))
    }
}

impl Deref for PathSegment {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl AsRef<str> for PathSegment {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::fmt::Display for PathSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.deref().fmt(f)
    }
}

/// "Direction" for path traversal.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) enum PathTraversal {
    /// Go to parent.
    ///
    /// This is equivalent to `..`.
    Up,

    /// Stay at current level.
    ///
    /// This is equivalent to `.`.
    Stay,

    /// Look up directory child.
    Down(PathSegment),
}

impl PathTraversal {
    /// Parse path.
    pub(crate) fn parse(
        s: &str,
        limit: &VfsLimits,
    ) -> std::io::Result<(
        bool,
        impl Iterator<Item = Result<Self, LimitExceeded>> + std::fmt::Debug,
    )> {
        let len = s.len() as u64;
        if len > limit.max_path_length {
            return Err(LimitExceeded {
                name: "path",
                limit: limit.max_path_length,
                current: 0,
                requested: len,
            }
            .into());
        }

        if s.is_empty() {
            return Err(std::io::Error::new(
                ErrorKind::InvalidFilename,
                "path cannot be empty",
            ));
        }
        if s.contains('\0') {
            return Err(std::io::Error::new(
                ErrorKind::InvalidFilename,
                "path contains NULL byte",
            ));
        }

        let mut segments = s.split('/').peekable();
        let is_root = if segments.peek().expect("checked that not empty").is_empty() {
            segments.next();
            true
        } else {
            false
        };

        let segments = segments.map(|s| {
            let direction = match s {
                "" | "." => Self::Stay,
                ".." => Self::Up,
                other => Self::Down(PathSegment::new(other, limit)?),
            };
            Ok(direction)
        });
        Ok((is_root, segments))
    }
}

impl std::fmt::Display for PathTraversal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Up => write!(f, ".."),
            Self::Stay => write!(f, "."),
            Self::Down(segment) => segment.fmt(f),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parsing_ok() {
        assert_eq!(parse_unwrap("/"), (true, vec![PathTraversal::Stay]));
        assert_eq!(
            parse_unwrap("//"),
            (true, vec![PathTraversal::Stay, PathTraversal::Stay]),
        );
        assert_eq!(parse_unwrap("/."), (true, vec![PathTraversal::Stay]));
        assert_eq!(parse_unwrap("/.."), (true, vec![PathTraversal::Up]));
        assert_eq!(parse_unwrap("."), (false, vec![PathTraversal::Stay]));
        assert_eq!(parse_unwrap(".."), (false, vec![PathTraversal::Up]));
        assert_eq!(
            parse_unwrap("foo"),
            (false, vec![PathTraversal::Down(PathSegment("foo".into()))])
        );
        assert_eq!(
            parse_unwrap("/foo"),
            (true, vec![PathTraversal::Down(PathSegment("foo".into()))])
        );
        assert_eq!(
            parse_unwrap("/foo/./../bar"),
            (
                true,
                vec![
                    PathTraversal::Down(PathSegment("foo".into())),
                    PathTraversal::Stay,
                    PathTraversal::Up,
                    PathTraversal::Down(PathSegment("bar".into())),
                ]
            )
        );
    }

    #[test]
    fn test_parsing_err() {
        let limits = VfsLimits::default();

        insta::assert_snapshot!(
            PathTraversal::parse("", &limits).unwrap_err(),
            @"path cannot be empty",
        );
        insta::assert_snapshot!(
            PathTraversal::parse("\0", &limits).unwrap_err(),
            @"path contains NULL byte",
        );
        insta::assert_snapshot!(
            PathTraversal::parse("foo\0bar", &limits).unwrap_err(),
            @"path contains NULL byte",
        );
        insta::assert_snapshot!(
            PathTraversal::parse(
                &std::iter::repeat_n('x', (limits.max_path_length + 1) as usize).collect::<String>(),
                &limits,
            ).unwrap_err(),
            @"path limit reached: limit<=255 current==0 requested+=256",
        );
        insta::assert_snapshot!(
            PathTraversal::parse(
                &std::iter::repeat_n('x', (limits.max_path_segment_size + 1) as usize).collect::<String>(),
                &limits,
            ).unwrap().1.next().unwrap().unwrap_err(),
            @"path segment limit reached: limit<=50 current==0 requested+=51",
        );

        // ensure that length is checked BEFORE scanning the actual content
        insta::assert_snapshot!(
            PathTraversal::parse(
                &std::iter::repeat_n('x', limits.max_path_length as usize).chain(std::iter::once('\0')).collect::<String>(),
                &limits,
            ).unwrap_err(),
            @"path limit reached: limit<=255 current==0 requested+=256",
        );
        insta::assert_snapshot!(
            PathTraversal::parse(
                &std::iter::repeat_n(
                    std::iter::repeat_n('x', (limits.max_path_segment_size + 1) as usize).chain(std::iter::once('/')),
                    limits.max_path_length as usize,
                ).flatten().collect::<String>(),
                &limits,
            ).unwrap_err(),
            @"path limit reached: limit<=255 current==0 requested+=13260",
        );
    }

    fn parse_unwrap(s: &str) -> (bool, Vec<PathTraversal>) {
        let limits = VfsLimits::default();
        let (is_root, segments) = PathTraversal::parse(s, &limits).unwrap();
        let segments = segments.collect::<Result<Vec<_>, _>>().unwrap();
        (is_root, segments)
    }
}
