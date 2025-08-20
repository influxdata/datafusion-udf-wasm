use std::{fs::File, path::PathBuf};

fn main() {
    configure_linker();
    bundle_python_lib();
}

fn configure_linker() {
    println!("cargo:rerun-if-env-changed=WASI_SDK_LINK_PATH");
    let Ok(link_path) = std::env::var("WASI_SDK_LINK_PATH") else {
        return;
    };

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
