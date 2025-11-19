//! Evil payloads that creates a LARGE file.

/// Return root file system.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn root() -> Option<Vec<u8>> {
    let mut ar = tar::Builder::new(Vec::new());

    let limit: usize = std::env::var("limit").unwrap().parse().unwrap();
    let data = vec![0u8; limit];

    let mut header = tar::Header::new_gnu();
    header.set_path("foo").unwrap();
    header.set_size(0);
    header.set_cksum();

    ar.append(&header, data.as_slice()).unwrap();

    Some(ar.into_inner().unwrap())
}
