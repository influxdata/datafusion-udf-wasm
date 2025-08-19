# WebAssembly

## Exception Handling
See <https://github.com/WebAssembly/wasi-sdk/blob/main/SetjmpLongjmp.md>. You can convert from legacy to new exception handling using (needs `wasm-opt` from [Binaryen]):

```console
wasm-opt --translate-to-exnref -all -o new.wasm old.wasm
```

## WASI
WASIp2 (= "preview 2") should be used because it uses the future-proof component model. If you have a wasip1 binary, use <https://github.com/bytecodealliance/wasmtime/tree/main/crates/wasi-preview1-component-adapter> to convert it to p2.


[Binaryen]: https://github.com/WebAssembly/binaryen
