# Python Guest

## Execution Models
### Embedded VM
#### Pyodide
Website: <https://pyodide.org/>.

Pros:
- Supports loads of dependencies
- Runs in the browser

Cons:
- Doesn't seem to be working with freestanding WASM runtimes / servers, esp. not without Node.js

#### Official CPython WASM Builds
Links:
- <https://github.com/python/cpython/tree/main/Tools/wasm>
- <https://devguide.python.org/getting-started/setup-building/#wasi>
- <https://github.com/psf/webassembly>
- <https://github.com/brettcannon/cpython-wasi-build/releases>

Pros:
- Official project, so it has a somewhat stable future and it is easier to get buy-in from the community

Cons:
- Can only run as a WASI CLI-like app (so we would need to interact with it via stdio or a fake network)
- Currently only offered as wasip1

#### pyo3 + Official CPython WASM Builds
Instead of using stdio to drive a Python interpreter, we use [pyo3].

#### webassembly-language-runtimes
Website: <https://github.com/webassemblylabs/webassembly-language-runtimes>

This was formally a VMWare project.

Cons:
- Seems dead?

### Ahead-of-Time Compilation
This is only going to work if

- the ahead-of-time compiler itself is lightweight enough to be embedded within a database (esp. it should not call to some random C host toolchain)
- the Python compiler/transpiler is solid and supports enough features

#### componentize-py
Website: <https://github.com/bytecodealliance/componentize-py>

#### py2wasm
Website: <https://github.com/wasmerio/py2wasm>

### Other Notes
- <https://wasmlabs.dev/articles/python-wasm-rust/>


[pyo3]: https://pyo3.rs/
