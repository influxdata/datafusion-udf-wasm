# Contributing

**Unless you explicitly state otherwise, any contribution you intentionally submit for inclusion in the work, as defined in the Apache-2.0 license, shall be dual-licensed as above, without any additional terms or conditions.**

## Required Software

### Rust
Install the `stable` [Rust] toolchain. The easiest way to do this is [rustup]. You also need the `nightly` toolchain for the `wasm32-wasip2` target. This can easily be done via (**note that this installs a NIGHTLY toolchain!**):

```console
rustup toolchain install nightly --target=wasm32-wasip2
```

### Yamllint
Install [yamllint] for linting.

### cargo deny
Install [cargo-deny] to check dependencies.

### Just
Install [just] to easily run all the tests/scripts.


## Checks
There is one "run everything" [just] recipe:

```console
just check
```

If this doesn't work, see ["Troubleshooting"](#troubleshooting).

To list all recipes that are available, run:

```console
just --list --list-submodules
```

## CI
There is a single CI job that just runs `just check`. The job log output is grouped by the respective just recipe. If the CI job fails (e.g. at `check-rust-fmt`), you should be able replicate that using a local `just` invocation (e.g. `just check-rust-fmt`).

## Troubleshooting
Here are some tips for when things don't work.

### `core` not found / install `wasm32-wasip2`
If your compilation fails with:

```text
error[E0463]: can't find crate for `core`
  |
  = note: the `wasm32-wasip2` target may not be installed
  = help: consider downloading the target with `rustup target add wasm32-wasip2`
  = help: consider building the standard library from source with `cargo build -Zbuild-std`
```

Then go back to ["Required Software > Rust"](#rust) and install the correct NIGHTLY toolchain. Note that the hint in the error message is wrong / incomplete. The correct way of installing the target for the nightly toolchain is -- which should NOT be required if you read & execute what is written in ["Required Software > Rust"](#rust):

```console
rustup target add wasm32-wasip2 --toolchain=nightly
```

### Python Fails To Start / No module named `encodings`
If the [Python] guest fails to start but prints the following to stderr:

```text
Could not find platform independent libraries <prefix>
Could not find platform dependent libraries <exec_prefix>
Fatal Python error: Failed to import encodings module
Python runtime state: core initialized
ModuleNotFoundError: No module named 'encodings'

Current thread 0x012bd368 (most recent call first):
  <no Python frame>
```

Then the [Python Standard Library] was not found or not bundled correctly. You may try to wipe `guests/python/download`. If that does not help, open a ticket.


[cargo-deny]: https://embarkstudios.github.io/cargo-deny/
[just]: https://github.com/casey/just
[Python]: https://www.python.org/
[Python Standard Library]: https://docs.python.org/3/library/index.html
[Rust]: https://www.rust-lang.org/
[rustup]: https://rustup.rs/
[yamllint]: https://github.com/adrienverge/yamllint
