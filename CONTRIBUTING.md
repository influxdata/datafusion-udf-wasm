# Contributing

**Unless you explicitly state otherwise, any contribution you intentionally submit for inclusion in the work, as defined in the Apache-2.0 license, shall be dual-licensed as above, without any additional terms or conditions.**

## Required Software

### Rust
Install the `stable` [Rust] toolchain. The easiest way to do this is [rustup]. You also need the `nightly` toolchain for the `wasm32-wasip2` target. This can easily be done via:

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

To list all recipes that are available, run:

```console
just --list --list-submodules
```

## CI
There is a single CI job that just runs `just check`. The job log output is grouped by the respective just recipe. If the CI job fails (e.g. at `check-rust-fmt`), you should be able replicate that using a local `just` invocation (e.g. `just check-rust-fmt`).


[cargo-deny]: https://embarkstudios.github.io/cargo-deny/
[just]: https://github.com/casey/just
[Rust]: https://www.rust-lang.org/
[rustup]: https://rustup.rs/
[yamllint]: https://github.com/adrienverge/yamllint
