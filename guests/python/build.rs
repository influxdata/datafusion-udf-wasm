//! Build script for [CPython]+[`pyo3`]-based UDFs.
//!
//! This ensures two things:
//! - **linking:** Set up correct linker arguments. This only happens when the `WASI_SDK_LINK_PATH` environment
//!   variable is set. Otherwise we assume that this is NOT a WASM build (e.g. during an ordinary `cargo check`) and
//!   that [`pyo3`] manages the linking itself.
//! - **root file system:** If the `PYO3_CROSS_LIB_DIR` environment variable is set, we assume that we must package
//!   the [Python Standard Library].
//!
//!
//! [CPython]: https://www.python.org/
//! [Python Standard Library]: https://docs.python.org/3/library/index.html
//! [`pyo3`]: https://pyo3.rs/
use sha2::Digest;
use std::{
    fs::File,
    io::{Read, Write},
    path::PathBuf,
};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    let downloaded = download_wasi_sdk().expect("WASI SDK download");

    if let Some(wasi_sdk) = downloaded {
        configure_linker(wasi_sdk);
    }

    bundle_python_lib();
}

/// Download WASI SDK sysroot for static linking.
///
/// This downloads the WASI SDK extracts it to the downloads directory. If the
/// sdk already exists, this function returns early.
///
/// Retrurns:
/// - `Ok(Some(String))`: Path to the WASI SDK sysroot if downloaded (i.e., if
///   building for wasm32-wasip2 target)
/// - `Ok(None)`: If not building for wasm32-wasip2 target
/// - `Err`: If an error occurred during download or extraction
fn download_wasi_sdk() -> Result<Option<String>, Box<dyn std::error::Error>> {
    let target =
        std::env::var("TARGET").expect("TARGET environment variable should be set by cargo!");

    // Only configure the WASI SDK when building for the wasm target. The downloads directory may
    // already exist from previous runs, but we must not trigger WASI-specific linking for native
    // `cargo check` builds.
    if target.as_str() != "wasm32-wasip2" {
        println!("cargo:info=not building for wasm32-wasip2 target, skipping WASI SDK download");
        return Ok(None);
    }

    const WASI_SDK_VERSION_MAJOR: &str = "24";
    const WASI_SDK_VERSION_MINOR: &str = "0";
    const SHA256_WASI_SDK_SYSROOT: &str =
        "35172f7d2799485b15a46b1d87f50a585d915ec662080f005d99153a50888f08";

    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let downloads_dir = manifest_dir.join("downloads");
    std::fs::create_dir_all(&downloads_dir)?;

    let wasi_sysroot_dir = downloads_dir.join("wasi-sysroot");
    let link_path = wasi_sysroot_dir.join("lib").join("wasm32-wasip2");

    // Skip if already downloaded
    if wasi_sysroot_dir.exists() {
        assert!(link_path.exists());
        println!("cargo:info=wasi sdk already present, skipping download");
        return Ok(link_path.to_string_lossy().to_string().into());
    }

    println!("cargo:warning=downloading WASI SDK sysroot...");

    let url = format!(
        "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-{}/wasi-sysroot-{}.{}.tar.gz",
        WASI_SDK_VERSION_MAJOR, WASI_SDK_VERSION_MAJOR, WASI_SDK_VERSION_MINOR
    );

    let tar_gz_path = downloads_dir.join("wasi-sysroot.tar.gz");

    // Download the file
    let response = ureq::get(&url)
        .call()
        .map_err(|e| format!("failed to download WASI SDK: {}", e))?;

    let mut file = File::create(&tar_gz_path)?;
    let mut reader = response.into_reader();
    let mut hasher = sha2::Sha256::new();
    let mut buffer = [0u8; 8192];
    while let Ok(n) = reader.read(&mut buffer)
        && n > 0
    {
        hasher.update(&buffer[..n]);
        file.write_all(&buffer[..n])?;
    }
    let digest = hasher.finalize();
    let hex_digest = format!("{:x}", digest);
    if hex_digest != SHA256_WASI_SDK_SYSROOT {
        std::fs::remove_file(&tar_gz_path)?;
        return Err(format!(
            "SHA256 mismatch for wasi-sysroot.tar.gz: expected {}, got {}",
            SHA256_WASI_SDK_SYSROOT, hex_digest
        )
        .into());
    }

    // Extract the tar.gz file
    let tar_gz = File::open(&tar_gz_path)?;
    let tar = flate2::read::GzDecoder::new(tar_gz);
    let mut archive = tar::Archive::new(tar);
    archive.unpack(&downloads_dir)?;

    let extracted_name = format!(
        "wasi-sysroot-{}.{}",
        WASI_SDK_VERSION_MAJOR, WASI_SDK_VERSION_MINOR
    );
    let extracted_path = downloads_dir.join(extracted_name);

    if !extracted_path.exists() {
        return Err(format!("expected directory not found: {}", extracted_path.display()).into());
    }

    let _mv = std::process::Command::new("mv")
        .arg(&extracted_path)
        .arg(&wasi_sysroot_dir)
        .status()?;

    std::fs::remove_file(&tar_gz_path)?;

    println!(
        "cargo:warning=WASI SDK sysroot downloaded to {}",
        wasi_sysroot_dir.display()
    );

    assert!(link_path.exists());
    Ok(link_path.to_string_lossy().to_string().into())
}

/// Set up correct linker arguments.
///
/// # Arguments
/// - `link_path`: Path to the WASI SDK
///
/// [`pyo3`]: https://pyo3.rs/
fn configure_linker(link_path: String) {
    println!("cargo:rerun-if-env-changed=WASI_SDK_LINK_PATH");

    // wasi libc
    println!("cargo:rustc-link-lib=dl");
    println!("cargo:rustc-link-lib=m");
    println!("cargo:rustc-link-lib=wasi-emulated-getpid");
    println!("cargo:rustc-link-lib=wasi-emulated-process-clocks");
    println!("cargo:rustc-link-lib=wasi-emulated-signal");
    println!("cargo:rustc-link-search=native={link_path}");

    // default stack size is 1048576, which is not enough for Python. Instead, we use the Python defaults.
    println!("cargo:rustc-link-arg=-zstack-size=16777216");

    // Python modules
    println!("cargo:rustc-link-lib=expat");
    println!("cargo:rustc-link-lib=Hacl_HMAC");
    println!("cargo:rustc-link-lib=Hacl_Hash_BLAKE2");
    println!("cargo:rustc-link-lib=Hacl_Hash_MD5");
    println!("cargo:rustc-link-lib=Hacl_Hash_SHA1");
    println!("cargo:rustc-link-lib=Hacl_Hash_SHA2");
    println!("cargo:rustc-link-lib=Hacl_Hash_SHA3");
    println!("cargo:rustc-link-lib=mpdec");
}

/// Bundle [Python Standard Library].
///
/// This is only done if the `PYO3_CROSS_LIB_DIR` environment variable is set.
///
///
/// [Python Standard Library]: https://docs.python.org/3/library/index.html
fn bundle_python_lib() {
    println!("cargo:rerun-if-env-changed=PYO3_CROSS_LIB_DIR");
    let tar_path = PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("python-lib.tar");
    let Ok(lib_dir) = std::env::var("PYO3_CROSS_LIB_DIR") else {
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
        if path_str.ends_with(".wasm") || path_str.ends_with(".a") || path_str.is_empty() {
            continue;
        }

        archive.append_path_with_name(path_abs, path_rel).unwrap();
    }
    archive.finish().unwrap();
}
