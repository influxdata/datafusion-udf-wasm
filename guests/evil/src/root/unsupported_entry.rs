//! Evil payloads that creates an unsupported entry type.

/// Return root file system.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn root() -> Option<Vec<u8>> {
    let mut ar = tar::Builder::new(Vec::new());

    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Symlink);
    header.set_size(0);
    ar.append_link(&mut header, "foo", "bar").unwrap();

    Some(ar.into_inner().unwrap())
}
