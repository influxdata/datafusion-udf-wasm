# Contributing

Anyone with a Github account is free to file issues on the project. However, if you want to contribute documentation or
code then you will need to sign InfluxData's Individual Contributor License Agreement (CLA), which can be found with
more information [on our website](https://www.influxdata.com/legal/cla/).

## Required Software

### Rust
Install the `stable` [Rust] toolchain. The easiest way to do this is [rustup]. You also the `wasm32-wasip2` target. This can easily be done via:

```console
rustup target add wasm32-wasip2
```

### Yamllint
Install [yamllint] for linting.

### cargo deny
Install [cargo-deny] to check dependencies.

### Just
Install [just] to easily run all the tests/scripts.

### Python
Install [Python] to set up the environment for the [Python] guest.


## Checks
There is one "run everything" [just] recipe:

```console
just check
```

Some common issues can be auto-fixed by running:

```console
just fix
```

If this doesn't work, see ["Troubleshooting"](#troubleshooting).

To list all recipes that are available, run:

```console
just
```

## CI
There is a single CI job that just runs `just check`. The job log output is grouped by the respective just recipe. If the CI job fails (e.g. at `check-rust-fmt`), you should be able replicate that using a local `just` invocation (e.g. `just check-rust-fmt`).

## Snapshot Testing
We use [insta] for snapshot testing. You can install/build the CLI interface for it using:

```console
cargo install --locked cargo-insta
```

If tests fail due to outdated snapshots, review them using:

```console
cargo insta review
```

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

You should probably have followed ["Required Software > Rust"](#rust), but the compiler also tells you what to do:

```console
rustup target add wasm32-wasip2
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
[insta]: https://insta.rs/
[just]: https://github.com/casey/just
[Python]: https://www.python.org/
[Python Standard Library]: https://docs.python.org/3/library/index.html
[Rust]: https://www.rust-lang.org/
[rustup]: https://rustup.rs/
[yamllint]: https://github.com/adrienverge/yamllint
