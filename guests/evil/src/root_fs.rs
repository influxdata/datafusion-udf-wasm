//! Root filesystem helpers for evil guest payloads.

use std::{
    fs::{self, File},
    io::{Cursor, Error, ErrorKind, copy},
    path::{Component, Path, PathBuf},
};

/// Populate the guest root filesystem from a TAR archive, if one was provided.
pub(crate) fn populate_root_fs_from_tar(root_fs_tar: Option<&[u8]>) -> std::io::Result<bool> {
    let Some(root_fs_tar) = root_fs_tar else {
        return Ok(false);
    };

    let cursor = Cursor::new(root_fs_tar);
    let mut archive = tar::Archive::new(cursor);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        let path_display = path.display().to_string();
        let guest_path = guest_root_path(&path)?;

        match entry.header().entry_type() {
            tar::EntryType::Directory => {
                fs::create_dir(&guest_path)?;
            }
            tar::EntryType::Regular => {
                let mut file = File::create(&guest_path)?;
                copy(&mut entry, &mut file)?;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    format!("Unsupported TAR content: {other:?} @ {path_display}"),
                ));
            }
        }
    }

    Ok(true)
}

/// Convert a TAR entry path into the corresponding absolute path inside the guest root.
fn guest_root_path(path: &Path) -> std::io::Result<PathBuf> {
    let invalid_end = |suffix: &str| {
        Error::new(
            ErrorKind::InvalidFilename,
            format!("TAR target MUST end in a valid filename, not {suffix}"),
        )
    };

    match path.components().next_back() {
        Some(Component::CurDir) => return Err(invalid_end(".")),
        Some(Component::ParentDir) => return Err(invalid_end("..")),
        Some(Component::RootDir) => return Err(invalid_end("/")),
        Some(Component::Normal(_)) | Some(Component::Prefix(_)) => {}
        None => return Err(invalid_end("")),
    }

    Ok(Path::new("/").join(path))
}
