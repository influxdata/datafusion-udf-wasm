//! Evil payloads that reports bytes that are NOT a TAR file.

/// Return root file system.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn root() -> Option<Vec<u8>> {
    Some(b"foo".to_vec())
}
