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

### cargo deny
Install [cargo-deny] to check dependencies.

### Just
Install [just] to easily run all the tests/scripts.

### uv
Install [uv] for [Python]-based tooling.

### Tombi
Install [tombi] to format and lint [TOML] files.

### Spellcheck
Install [typos] to run an automatic spellcheck.

### Valgrind
We require [Valgrind] to run our benchmark. However, if you only want to "smoke test" benchmarks (via `just check-rust-bench`), [Valgrind] is NOT required.


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

## Benchmarks
We use [Valgrind] + [gungraun] for our benchmarks. This approach should result in less flakiness compared to wall-clock based micro-benchmarks (e.g. via [Criterion.rs]). It also yields more useful data (e.g. to fit linear regressions for cost models) than paired benchmarks like [Tango.rs].

To run a benchmark (e.g. `udf_overhead`), use:

```console
cargo bench --features=all-arch --package=datafusion-udf-wasm-host --bench=udf_overhead
```

You can also see all benchmarks CLI options if you pass `--help` to the benchmark binary (note the extra `--` separator):

```console
cargo bench --features=all-arch --package=datafusion-udf-wasm-host --bench=udf_overhead -- --help
```

If you just want to smoke-test the benchmarks w/o any actual measurement, you can use:

```console
just check-rust-bench
```

Smoke-testing does NOT require [Valgrind].

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

## Pre-built WASM Binaries
We offer pre-built WASM guest binaries to simplify integration into other software artifacts. This way you only need to depend on the host crates and don't need a WASI compilation toolchain. It also cuts build and test times, since you can include a release-optimized guest even during development and CI. Release-optimized guests are smaller and can be JIT-compiled and executed faster.

You find WASM builds published as releases named "WASM Binaries".

### Triggering A Build
To trigger a build, you need write access to this repository. Then use the [GitHub CLI] tool and run:

```console
$ gh workflow run Prebuild
```

This will trigger the build on the `main` branch. If you need a different branch, use:

```console
$ gh workflow run Prebuild --ref <BRANCH_NAME>
```


[cargo-deny]: https://embarkstudios.github.io/cargo-deny/
[Criterion.rs]: https://github.com/criterion-rs/criterion.rs
[gungraun]: https://gungraun.github.io/gungraun/
[GitHub CLI]: https://cli.github.com/
[insta]: https://insta.rs/
[just]: https://github.com/casey/just
[Python]: https://www.python.org/
[Python Standard Library]: https://docs.python.org/3/library/index.html
[Rust]: https://www.rust-lang.org/
[rustup]: https://rustup.rs/
[Tango.rs]: https://github.com/bazhenov/tango
[tombi]: https://tombi-toml.github.io/tombi
[TOML]: https://toml.io/
[typos]: https://github.com/crate-ci/typos
[uv]: https://docs.astral.sh/uv/
[Valgrind]: https://valgrind.org/
[yamllint]: https://github.com/adrienverge/yamllint
