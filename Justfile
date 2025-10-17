mod guests

# default Just target - list recipes
default:
    @just --list --list-submodules

# check Rust files via `cargo build`
check-rust-build: guests::rust::check-build guests::python::check-build

# check Rust files via `cargo check`
check-rust-check $JUSTCHECK="1":
    @echo ::group::check-rust-check
    cargo check --workspace --all-features
    @echo ::endgroup::

# check Rust files via `cargo clippy`
check-rust-clippy $JUSTCHECK="1":
    @echo ::group::check-rust-clippy
    cargo clippy --all-features --all-targets --workspace -- -D warnings
    @echo ::endgroup::

# check Rust formatting
check-rust-fmt $JUSTCHECK="1":
    @echo ::group::check-rust-fmt
    cargo fmt --all -- --check
    @echo ::endgroup::

# test Rust code
check-rust-test $RUST_BACKTRACE="1":
    @echo ::group::check-rust-test
    cargo test --all-features --workspace
    @echo ::endgroup::

# build Rust docs
check-rust-doc $JUSTCHECK="1":
    @echo ::group::check-rust-doc
    cargo doc --document-private-items --all-features --workspace
    @echo ::endgroup::

# dry-run Rust benchmarks
check-rust-bench:
    @echo ::group::check-rust-bench
    cargo bench --profile=dev --all-features --workspace -- --test
    @echo ::endgroup::

# check Rust dependencies with cargo-deny
check-rust-deny:
    @echo ::group::check-rust-deny
    cargo deny check
    @echo ::endgroup::

# run ALL Rust checks
check-rust: check-rust-fmt check-rust-check check-rust-build check-rust-clippy check-rust-test check-rust-doc check-rust-bench check-rust-deny

# lint YAML files
check-yaml:
    @echo ::group::check-yaml
    yamllint -s .
    @echo ::endgroup::

# run ALL checks
check: check-rust check-yaml

# clean Rust build artifacts
clean-rust:
    @echo ::group::clean-rust
    cargo clean
    @echo ::endgroup::

# clean build artifacts
clean: clean-rust guests::python::clean

# fix Rust check/rustc warnings
fix-rust-check:
    @echo ::group::fix-rust-check
    cargo fix --workspace --allow-dirty --allow-staged
    @echo ::endgroup::

# fix Rust clippy warnings
fix-rust-clippy:
    @echo ::group::fix-rust-clippy
    cargo clippy --all-targets --all-features --workspace --fix --allow-dirty --allow-staged
    @echo ::endgroup::

# fix Rust formatting
fix-rust-fmt:
    @echo ::group::fix-rust-fmt
    cargo fmt --all
    @echo ::endgroup::

# fix common Rust issues automatically
fix-rust: fix-rust-clippy fix-rust-check fix-rust-fmt

# fix common issues automatically
fix: fix-rust
