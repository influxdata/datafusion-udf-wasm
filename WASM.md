# WebAssembly
The main website is [webassembly.org](https://webassembly.org/).

## Hosts
We will use [wasmtime] for our host.

## Guests
Some guest languages have builtin WASM support, like [Rust]. Others (like [CPython]) use the [WASI SDK], which is a combination of [Clang] (which has a WASI target through [LLVM]) and [`wasi-libc`].

## WASM Features
[webassembly.org/features](https://webassembly.org/features/) provides a good overview. For features supported by [wasmtime], see [here](https://docs.wasmtime.dev/stability-wasm-proposals.html).

### Exception Handling
Exceptions are used for traditional exceptions and stack unwinding, but also for [C setjmp/longjmp] -- which is used by some guests that are built via the [WASI SDK].

Exception handling comes in two flavors: [Legacy Exceptions] and [New Exception Handling]. The first one will likely never be really supported by [wasmtime]. The second one will be supported soon. You can convert from [Legacy Exceptions] to [New Exception Handling] using (needs `wasm-opt` from [Binaryen]):

```console
wasm-opt --translate-to-exnref -all -o new.wasm old.wasm
```

## WASI
The WebAssembly System Interface ([WASI]) defines an interface for guests to get some IO functions via the host, e.g. stdin/stdout, network, a filesystem. [WASI] comes in multiple preview versions (there is NO non-preview version yet). [WASIp2] (= "preview 2") should be used because it uses the future-proof [Component Model] / [WIT].

### WASIp1 to WASIp2
If you have a [WASIp1] (= "preview 1"/"legacy") binary, use the [`wasi-preview1-component-adapter`] to convert it to [WASIp2].

### WASIp3
[WASIp3] is currently work-in-progress. We will switch to that once it is ready.

### Virtual Implementations
Through the magic of [composition](https://component-model.bytecodealliance.org/composing-and-distributing/composing.html), [WASI] interfaces (like file system IO) can be virtualized within the guest (= NO host support/implementation required!). Use [WASI Virt] to do that.


[Binaryen]: https://github.com/WebAssembly/binaryen
[C setjmp/longjmp]: https://github.com/WebAssembly/wasi-sdk/blob/main/SetjmpLongjmp.md
[CPython]: https://www.python.org/
[Clang]: https://clang.llvm.org/
[Component Model]: https://component-model.bytecodealliance.org/
[LLVM]: https://llvm.org/
[Legacy Exceptions]: https://github.com/WebAssembly/exception-handling/blob/master/proposals/exception-handling/legacy/Exceptions.md
[New Exception Handling]: https://github.com/WebAssembly/exception-handling/blob/master/proposals/exception-handling/Exceptions.md
[Rust]: https://www.rust-lang.org/
[WASI SDK]: https://github.com/WebAssembly/wasi-sdk
[WASI]: https://wasi.dev/
[WASIp1]: https://wasi.dev/interfaces#wasi-01
[WASIp2]: https://wasi.dev/interfaces#wasi-02
[WASIp3]: https://wasi.dev/interfaces#wasi-03
[WIT]: https://component-model.bytecodealliance.org/design/wit.html
[`wasi-libc`]: https://github.com/WebAssembly/wasi-libc
[`wasi-preview1-component-adapter`]: https://github.com/bytecodealliance/wasmtime/tree/main/crates/wasi-preview1-component-adapter
[wasmtime]: https://wasmtime.dev/
[WASI Virt]: https://github.com/bytecodealliance/WASI-virt
