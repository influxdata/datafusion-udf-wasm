//! Pre-compile component for given target.
#![expect(
    // unused-crate-dependencies false positives
    unused_crate_dependencies,
)]

use std::io::{Read, Write};

use datafusion_udf_wasm_host::{CompilationFlags, WasmComponentPrecompiled};

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let (input, output, target) = match args.as_slice() {
        [_exe, i, o, t] => (i, o, Some(t.clone())),
        [_exe, i, o] => (i, o, None),
        _ => {
            eprintln!("Usage: <INPUT> <OUTPUT> [<TARGET>]");
            eprintln!();
            eprintln!("Use `-` for input/output for stdin/stdout.");
            std::process::exit(1);
        }
    };

    let wasm_binary = if input == "-" {
        let mut buf = vec![];
        std::io::stdin().read_to_end(&mut buf).expect("read stdin");
        buf
    } else {
        std::fs::read(input).expect("read input file")
    };

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

    let elf = component.store();
    if output == "-" {
        std::io::stdout().write_all(elf).expect("write to stdout");
    } else {
        std::fs::write(output, elf).expect("write output");
    }
}
