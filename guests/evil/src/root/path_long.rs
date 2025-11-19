//! Evil payloads that creates a file with a long path.

/// Return root file system.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn root() -> Option<Vec<u8>> {
    let mut ar = tar::Builder::new(Vec::new());

    let limit: usize = std::env::var("limit").unwrap().parse().unwrap();

    let mut header = tar::Header::new_gnu();
    header
        .set_path(std::iter::repeat_n('x', limit + 1).collect::<String>())
        .unwrap();
    header.set_size(0);
    header.set_cksum();
    ar.append(&header, b"".as_slice()).unwrap();

    Some(ar.into_inner().unwrap())
}
