//! Evil payload that returns a tar file that is larger than allowed.

/// Return root file system.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn root() -> Option<Vec<u8>> {
    let mut ar = tar::Builder::new(Vec::new());

    let tar_size: usize = std::env::var("tar_size").unwrap().parse().unwrap();
    let data = vec![0u8; tar_size];

    let mut header = tar::Header::new_gnu();
    header.set_path("foo").unwrap();
    header.set_size(data.len() as u64);
    header.set_cksum();

    ar.append(&header, data.as_slice()).unwrap();

    Some(ar.into_inner().unwrap())
}
