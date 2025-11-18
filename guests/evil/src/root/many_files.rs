//! Evil payloads that creates A LOT of files.

/// Return root file system.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn root() -> Option<Vec<u8>> {
    let mut ar = tar::Builder::new(Vec::new());

    let limit: u64 = std::env::var("limit").unwrap().parse().unwrap();
    for i in 0..=limit {
        let mut header = tar::Header::new_gnu();
        header.set_path(i.to_string()).unwrap();
        header.set_size(0);
        header.set_cksum();

        ar.append(&header, b"".as_slice()).unwrap();
    }

    Some(ar.into_inner().unwrap())
}
