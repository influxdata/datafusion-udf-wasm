//! Build script for [CPython]+[`pyo3`]-based UDFs.
//!
//! This ensures this:
//! - **root file system:** If the `PYTHON_SDK_DIR` environment variable is set, we assume that we must package
//!   the [Python Standard Library].
//!
//!
//! [CPython]: https://www.python.org/
//! [Python Standard Library]: https://docs.python.org/3/library/index.html
//! [`pyo3`]: https://pyo3.rs/
use std::{fs::File, io::Write, path::PathBuf};

/// File endings that should be skipped when bundling the up the Python lib.
const SKIP_ENDINGS: &[&str] = &[".a", ".pyc", ".so", ".wasm"];

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    bundle_python_lib();
}

/// Bundle [Python Standard Library].
///
/// This is only done if the `PYTHON_SDK_DIR` environment variable is set.
///
///
/// [Python Standard Library]: https://docs.python.org/3/library/index.html
fn bundle_python_lib() {
    println!("cargo:rerun-if-env-changed=PYTHON_SDK_DIR");
    let tar_path = PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("python-lib.tar");
    let Ok(lib_dir) = std::env::var("PYTHON_SDK_DIR") else {
        std::fs::write(&tar_path, b"").unwrap();
        return;
    };
    let lib_dir = PathBuf::from(lib_dir);

    let file = File::create(&tar_path).unwrap();
    let mut archive = tar::Builder::new(file);
    archive.mode(tar::HeaderMode::Deterministic);
    for entry in walkdir::WalkDir::new(&lib_dir).sort_by_file_name() {
        let entry = entry.unwrap();

        let path_abs = entry.path();
        let path_rel = path_abs.strip_prefix(&lib_dir).unwrap();

        let path_str = path_rel.to_string_lossy();
        if SKIP_ENDINGS.iter().any(|ending| path_str.ends_with(ending)) || path_str.is_empty() {
            continue;
        }

        archive.append_path_with_name(path_abs, path_rel).unwrap();
    }
    archive.finish().unwrap();
    archive.into_inner().unwrap().flush().unwrap();
}
