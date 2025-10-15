# Rust Guest

## Build
Building the guest requires the `wasm32-wasi` target to be installed:

```console
rustup target add wasm32-wasip2
```

Then build with:

```console
just build-add-one-debug
```

Optionally you can check that symbols are exported correctly (requires [`wasm-tools`]):

```console
wasm-tools component wit ../../target/wasm32-wasip2/debug/examples/add_one.wasm
```


[`wasm-tools`]: https://github.com/bytecodealliance/wasm-tools
