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
use reqwest::blocking::Client;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
};

/// Configuration for downloading and validating SDK files
#[derive(Debug)]
pub struct DownloadSpec {
    /// The URL to download from
    pub url: String,
    /// The expected SHA256 checksum of the downloaded file
    pub sha256: String,
    /// The filename to save the downloaded file as
    pub filename: String,
    /// The directory name to extract the archive to
    pub extract_to: String,
}

/// A downloader for WASM SDK files that validates checksums and extracts archives
#[derive(Debug)]
pub struct WasmSdkDownloader {
    /// The directory to store downloaded files
    downloads_dir: PathBuf,
    /// The HTTP client for downloading files
    client: Client,
}

impl WasmSdkDownloader {
    /// Create a new WasmSdkDownloader with the specified downloads directory
    pub fn new(downloads_dir: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        fs::create_dir_all(&downloads_dir)?;
        let client = Client::new();
        Ok(Self {
            downloads_dir,
            client,
        })
    }

    /// Download, validate, and extract multiple files
    pub fn download_and_extract(
        &self,
        specs: &[DownloadSpec],
    ) -> Result<(), Box<dyn std::error::Error>> {
        for spec in specs {
            self.download_and_extract_single(spec)?;
        }
        Ok(())
    }

    /// Download, validate, and extract a single file
    fn download_and_extract_single(
        &self,
        spec: &DownloadSpec,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let extract_path = self.downloads_dir.join(&spec.extract_to);

        // Skip if already extracted
        if extract_path.exists() {
            println!(
                "cargo:warning={} already present, skipping download",
                spec.extract_to
            );
            return Ok(());
        }

        let file_path = self.downloads_dir.join(&spec.filename);

        // Download file
        println!("cargo:warning=Downloading {}", spec.url);
        let response = self.client.get(&spec.url).send()?;
        if !response.status().is_success() {
            return Err(format!("Failed to download {}: {}", spec.url, response.status()).into());
        }

        let content = response.bytes()?;
        fs::write(&file_path, &content)?;

        // Validate checksum
        Self::validate_checksum(&file_path, &spec.sha256)?;

        // Extract archive
        Self::extract_archive(&file_path, &extract_path, &spec.filename)?;

        // Clean up downloaded file
        fs::remove_file(&file_path)?;

        Ok(())
    }

    /// Validate the SHA256 checksum of a file
    fn validate_checksum(
        file_path: &Path,
        expected_sha256: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut file = File::open(file_path)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0; 8192];

        loop {
            let bytes_read = file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }

        let actual_hash = hex::encode(hasher.finalize());

        if actual_hash != expected_sha256 {
            return Err(format!(
                "Checksum mismatch for {}: expected {}, got {}",
                file_path.display(),
                expected_sha256,
                actual_hash
            )
            .into());
        }

        println!(
            "cargo:warning=Checksum validated for {}",
            file_path.display()
        );
        Ok(())
    }

    /// Extract an archive file (zip or tar.gz)
    fn extract_archive(
        archive_path: &Path,
        extract_to: &Path,
        filename: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if filename.ends_with(".zip") {
            Self::extract_zip(archive_path, extract_to)?;
        } else if filename.ends_with(".tar.gz") {
            Self::extract_tar_gz(archive_path, extract_to)?;
        } else {
            return Err(format!("Unsupported archive format: {}", filename).into());
        }

        println!(
            "cargo:warning=Extracted {} to {}",
            filename,
            extract_to.display()
        );
        Ok(())
    }

    /// Extract a ZIP archive
    fn extract_zip(
        archive_path: &Path,
        extract_to: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::open(archive_path)?;
        let mut archive = zip::ZipArchive::new(file)?;

        for i in 0..archive.len() {
            let mut file = archive.by_index(i)?;
            let outpath = extract_to.join(file.name());

            if file.name().ends_with('/') {
                fs::create_dir_all(&outpath)?;
            } else {
                if let Some(p) = outpath.parent() {
                    fs::create_dir_all(p)?;
                }
                let mut outfile = File::create(&outpath)?;
                io::copy(&mut file, &mut outfile)?;
            }
        }
        Ok(())
    }

    /// Extract a TAR.GZ archive
    fn extract_tar_gz(
        archive_path: &Path,
        extract_to: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tar_gz = File::open(archive_path)?;
        let tar = flate2::read::GzDecoder::new(tar_gz);
        let mut archive = tar::Archive::new(tar);
        archive.unpack(extract_to)?;
        Ok(())
    }
}

fn main() {
    if let Err(e) = run_build() {
        panic!("Build failed: {}", e);
    }
}

/// Run the build process
fn run_build() -> Result<(), Box<dyn std::error::Error>> {
    configure_linker();
    download_dependencies()?;
    bundle_python_lib();
    Ok(())
}

/// Set up correct linker arguments.
///
/// This only happens when the `WASI_SDK_LINK_PATH` environment variable is set. Otherwise we assume that this is NOT
/// a WASM build (e.g. during an ordinary `cargo check`) and that [`pyo3`] manages the linking itself.
///
///
/// [`pyo3`]: https://pyo3.rs/
fn configure_linker() {
    println!("cargo:rerun-if-env-changed=WASI_SDK_LINK_PATH");
    let Ok(link_path) = std::env::var("WASI_SDK_LINK_PATH") else {
        return;
    };

    // Add Python SDK library path
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let python_sdk_path = PathBuf::from(out_dir).join("downloads").join("python-sdk");
    println!(
        "cargo:rustc-link-search=native={}",
        python_sdk_path.display()
    );

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
        fs::write(&tar_path, b"").unwrap();
        return;
    };
    let lib_dir = PathBuf::from(lib_dir);

    let file = File::create(&tar_path).unwrap();
    let mut archive = tar::Builder::new(file);
    for entry in walkdir::WalkDir::new(&lib_dir) {
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

/// Download all required dependencies using WasmSdkDownloader
fn download_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let downloads_dir = PathBuf::from(out_dir).join("downloads");

    // Set environment variables for the build process
    println!(
        "cargo:rustc-env=PYTHON_SDK_DIR={}",
        downloads_dir.join("python-sdk").display()
    );
    println!(
        "cargo:rustc-env=WASI_SYSROOT_DIR={}",
        downloads_dir.join("wasi-sysroot").display()
    );

    let downloader = WasmSdkDownloader::new(downloads_dir.clone())?;

    let specs = vec![
        DownloadSpec {
            url: format!(
                "https://github.com/brettcannon/cpython-wasi-build/releases/download/v{}.{}.0/python-{}.{}.0-wasi_sdk-{}.zip",
                PYTHON_VERSION_MAJOR,
                PYTHON_VERSION_MINOR,
                PYTHON_VERSION_MAJOR,
                PYTHON_VERSION_MINOR,
                WASI_SDK_VERSION_MAJOR
            ),
            sha256: SHA256_PYTHON_SDK.to_string(),
            filename: "python-sdk.zip".to_string(),
            extract_to: "python-sdk".to_string(),
        },
        DownloadSpec {
            url: format!(
                "https://github.com/brettcannon/cpython-wasi-build/releases/download/v{}.{}.0/_build-python-{}.{}.0-wasi_sdk-{}.zip",
                PYTHON_VERSION_MAJOR,
                PYTHON_VERSION_MINOR,
                PYTHON_VERSION_MAJOR,
                PYTHON_VERSION_MINOR,
                WASI_SDK_VERSION_MAJOR
            ),
            sha256: SHA256_PYTHON_SDK_BUILD.to_string(),
            filename: "build-python-sdk.zip".to_string(),
            extract_to: "build-python-sdk".to_string(),
        },
        DownloadSpec {
            url: format!(
                "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-{}/wasi-sysroot-{}.{}.tar.gz",
                WASI_SDK_VERSION_MAJOR, WASI_SDK_VERSION_MAJOR, WASI_SDK_VERSION_MINOR
            ),
            sha256: SHA256_WASI_SDK_SYSROOT.to_string(),
            filename: "wasi-sysroot.tar.gz".to_string(),
            extract_to: "wasi-sysroot-temp".to_string(),
        },
    ];

    downloader.download_and_extract(&specs)?;

    // Post-processing: move files from build-python-sdk to python-sdk (matching original Justfile logic)
    let python_sdk_dir = downloads_dir.join("python-sdk");
    let build_python_sdk_dir = downloads_dir.join("build-python-sdk");

    if build_python_sdk_dir.exists() {
        move_build_artifacts(&build_python_sdk_dir, &python_sdk_dir)?;
        fs::remove_dir_all(&build_python_sdk_dir)?;
    }

    // Post-processing: rename wasi-sysroot directory (matching original Justfile logic)
    let wasi_sysroot_temp = downloads_dir.join("wasi-sysroot-temp");
    let wasi_sysroot_final = downloads_dir.join("wasi-sysroot");
    let wasi_sysroot_versioned = wasi_sysroot_temp.join(format!(
        "wasi-sysroot-{}.{}",
        WASI_SDK_VERSION_MAJOR, WASI_SDK_VERSION_MINOR
    ));

    if wasi_sysroot_versioned.exists() && !wasi_sysroot_final.exists() {
        fs::rename(&wasi_sysroot_versioned, &wasi_sysroot_final)?;
        fs::remove_dir_all(&wasi_sysroot_temp)?;
    }

    Ok(())
}

/// Move build artifacts from build directory to SDK directory
fn move_build_artifacts(
    build_dir: &Path,
    target_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let artifacts = vec![
        format!("libpython{PYTHON_VERSION_MAJOR}.{PYTHON_VERSION_MINOR}.a"),
        "Modules/_decimal/libmpdec/libmpdec.a".to_string(),
        "Modules/_hacl/libHacl_HMAC.a".to_string(),
        "Modules/_hacl/libHacl_Hash_BLAKE2.a".to_string(),
        "Modules/_hacl/libHacl_Hash_MD5.a".to_string(),
        "Modules/_hacl/libHacl_Hash_SHA1.a".to_string(),
        "Modules/_hacl/libHacl_Hash_SHA2.a".to_string(),
        "Modules/_hacl/libHacl_Hash_SHA3.a".to_string(),
        "Modules/expat/libexpat.a".to_string(),
    ];

    for artifact in artifacts {
        let src = build_dir.join(&artifact);
        let filename = Path::new(&artifact).file_name().unwrap();
        let dst = target_dir.join(filename);

        if src.exists() {
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&src, &dst)?;
            println!("cargo:warning=Moved {} to {}", src.display(), dst.display());
        }
    }

    Ok(())
}

// Version constants (should match Justfile)
const PYTHON_VERSION_MAJOR: &str = "3";
const PYTHON_VERSION_MINOR: &str = "14";
const WASI_SDK_VERSION_MAJOR: &str = "24";
const WASI_SDK_VERSION_MINOR: &str = "0";

// SHA256 checksums (should match Justfile)
const SHA256_PYTHON_SDK: &str = "54aa3e33ebb45e03b5c13b86ce8742b45bb94e394925622fa66c4700f6782979";
const SHA256_PYTHON_SDK_BUILD: &str =
    "98e7cd352e512d1d2af6f999f4391df959eba1b213ef778646353620d697638a";
const SHA256_WASI_SDK_SYSROOT: &str =
    "35172f7d2799485b15a46b1d87f50a585d915ec662080f005d99153a50888f08";
