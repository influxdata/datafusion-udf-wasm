//! Evil payloads that returns a HUGE sparse tar file (i.e. the file itself is small, but the sparse payload is big).

/// Return root file system.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn root() -> Option<Vec<u8>> {
    // Sparse TARs are really hard to get right using the public APIs in `tar`, hence we just include a test file
    // directly.
    Some(include_bytes!("sparse-large.tar").to_vec())
}
