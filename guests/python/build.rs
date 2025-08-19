fn main() {
    println!("cargo:rerun-if-env-changed=WASI_SDK_LINK_PATH");
    if let Ok(link_path) = std::env::var("WASI_SDK_LINK_PATH") {
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
}
