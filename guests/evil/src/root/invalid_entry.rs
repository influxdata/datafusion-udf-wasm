//! Evil payloads that creates a TAR file with an invalid entry.

/// Return root file system.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn root() -> Option<Vec<u8>> {
    let mut ar = tar::Builder::new(Vec::new());

    let mut header = tar::Header::new_gnu();
    header.set_path("foo").unwrap();

    ar.append(&header, b"".as_slice()).unwrap();

    Some(ar.into_inner().unwrap())
}
