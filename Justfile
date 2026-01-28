mod guests

# default Just target - list recipes
default:
    @just --list --list-submodules

# check Python formatting
check-python-fmt:
    @echo ::group::check-python-fmt
    uv --project=python-tooling run --isolated --locked -- ruff format --check
    @echo ::endgroup::

# check Python lints
check-python-lint:
    @echo ::group::check-python-lint
    uv --project=python-tooling run --isolated --locked -- ruff check
    @echo ::endgroup::

# ensure that Python lockfile is up-to-date
check-python-lock:
    @echo ::group::check-python-lock
    uv --project=python-tooling lock --check
    @echo ::endgroup::

# check Python typing
check-python-ty:
    @echo ::group::check-python-ty
    uv --project=python-tooling run --isolated --locked -- ty check --error-on-warning
    @echo ::endgroup::

# all Python checks
check-python: check-python-fmt check-python-lint check-python-lock check-python-ty

# check Rust files via `cargo build`
check-rust-build: guests::rust::check-build guests::python::check-build

# check Rust files via `cargo check` and no default features
check-rust-check-no-default-features $JUSTCHECK="1":
    @echo ::group::check-rust-check-no-default-features
    cargo check --workspace --no-default-features
    @echo ::endgroup::

# check Rust files via `cargo check` and all features
check-rust-check-all-features $JUSTCHECK="1":
    @echo ::group::check-rust-check-all-features
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
check-rust: check-rust-fmt check-rust-check-no-default-features check-rust-check-all-features check-rust-build check-rust-clippy check-rust-test check-rust-doc check-rust-bench check-rust-deny

# check spelling
check-spelling:
    @echo ::group::check-spelling
    typos
    @echo ::endgroup::

# check TOML formatting
check-toml-fmt:
    @echo ::group::check-toml-fmt
    tombi format --check
    @echo ::endgroup::

# lint TOML files with taplo
check-toml-lint:
    @echo ::group::check-toml-lint
    tombi lint
    @echo ::endgroup::

# check TOML files
check-toml: check-toml-fmt check-toml-lint

# check WIT files
check-wit:
    #!/usr/bin/env bash
    set -euo pipefail

    echo ::group::check-wit

    readonly wit='wit/world.wit'
    readonly sed_filter='s/^[^@]+@([0-9.]+).*/\1/g'

    echo
    if ! git diff --exit-code origin/main "$wit"; then
        echo
        echo "change detected"

        version_main="$(git show origin/main:"$wit" | grep package | sed -E "$sed_filter")"
        version_head="$(cat "$wit" | grep package | sed -E "$sed_filter")"
        echo "version main: $version_main"
        echo "version HEAD: $version_head"

        if [ "$version_head" == "$version_main" ]; then
            echo "please update the WIT version!"
            echo ::endgroup::
            exit 1
        else
            echo "WIT version was updated"
        fi
    else
        echo
        echo "no change detected"
    fi

    echo ::endgroup::

# lint YAML files
check-yaml:
    @echo ::group::check-yaml
    uv --project=python-tooling run --isolated --locked -- yamllint -s .
    @echo ::endgroup::

# run ALL checks
check: check-python check-rust check-spelling check-toml check-wit check-yaml

# clean Rust build artifacts
clean-rust:
    @echo ::group::clean-rust
    cargo clean
    @echo ::endgroup::

# clean build artifacts
clean: clean-rust guests::python::clean

# fix Python formatting
fix-python-fmt:
    @echo ::group::fix-python-fmt
    uv --project=python-tooling run --isolated --locked -- ruff format
    @echo ::endgroup::

# fix Python lints
fix-python-lint:
    @echo ::group::fix-python-lint
    uv --project=python-tooling run --isolated --locked -- ruff check --fix
    @echo ::endgroup::

# lock Python env
fix-python-lock:
    @echo ::group::fix-python-lock
    uv --project=python-tooling lock
    @echo ::endgroup::

# fix Python-related issues
fix-python: fix-python-lint fix-python-fmt fix-python-lock

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

# fix typos
fix-spellcheck:
    @echo ::group::fix-rust-fmt
    typos --write-changes
    @echo ::endgroup::

# fix TOML formatting
fix-toml-fmt:
    @echo ::group::fix-toml-fmt
    tombi format
    @echo ::endgroup::

# fix common TOML issues
fix-toml: fix-toml-fmt

# fix common issues automatically
fix: fix-python fix-spellcheck fix-rust fix-toml
