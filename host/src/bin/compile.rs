//! Pre-compile component for given target.
#![expect(
    // unused-crate-dependencies false positives
    unused_crate_dependencies,
)]

use datafusion_udf_wasm_host::{CompilationFlags, WasmComponentPrecompiled};

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let (input, output, target) = match args.as_slice() {
        [_exe, i, o, t] => (i, o, Some(t.clone())),
        [_exe, i, o] => (i, o, None),
        _ => {
            eprintln!("Usage: <INPUT> <OUTPUT> [<TARGET>]");
            std::process::exit(1);
        }
    };

    let wasm_binary = std::fs::read(input).expect("read input file");
    let flags = CompilationFlags { target };

    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let component = rt
        .block_on(WasmComponentPrecompiled::compile(
            wasm_binary.into(),
            &flags,
        ))
        .unwrap();

    std::fs::write(output, component.store()).expect("write output");
}
