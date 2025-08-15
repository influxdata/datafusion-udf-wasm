# Rust Guest

## Build
Until <https://github.com/rust-lang/rust/issues/130323> is resolved, building the guest requires a nightly Rust toolchain with the `wasm32-wasi` target installed:

```console
rustup toolchain install nightly --target=wasm32-wasip2
```

Then build with:

```console
cargo +nightly build --target wasm32-wasip2
```

Optionally you can check that symbols are exported correctly (requires [`wasm-tools`]):

```console
wasm-tools component wit ../../target/wasm32-wasip2/debug/datafusion_udf_wasm_guest.wasm
```


[`wasm-tools`]: https://github.com/bytecodealliance/wasm-tools
