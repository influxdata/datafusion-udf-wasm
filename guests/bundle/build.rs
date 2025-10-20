//! Build script.
#![allow(unused_crate_dependencies)]

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str::FromStr,
};

fn main() {
    let profile: Profile = std::env::var("PROFILE").unwrap().parse().unwrap();
    let package_locations = package_locations();

    // does it look like we are running under clippy or rust-analyzer
    // This code was inspired by
    // https://github.com/bytecodealliance/componentize-py/blob/139d0ed85f09095e0a4cfa112e97ce589371315e/build.rs#L35-L42
    //
    // This doesn't detect the following things though:
    // - `cargo check`: https://github.com/rust-lang/cargo/issues/4001
    // - `cargo doc`: https://github.com/rust-lang/cargo/issues/8811
    println!("cargo::rerun-if-env-changed=JUSTCHECK");
    let stub = matches!(
        std::env::var("CARGO_CFG_FEATURE").as_deref(),
        Ok("cargo-clippy")
    ) || std::env::var("CLIPPY_ARGS").is_ok()
        || std::env::var("CARGO_EXPAND_NO_RUN_NIGHTLY").is_ok()
        || std::env::var("DOCS_RS").is_ok()
        || std::env::var("JUSTCHECK").is_ok();

    for feature in FEATURES {
        println!("processing {}", feature.name);
        feature.build_or_stub(stub, profile, &package_locations);
    }
}

/// Get locations for all packages in the dependency tree.
fn package_locations() -> HashMap<String, PathBuf> {
    let json = Command::new(std::env::var("CARGO").unwrap())
        .current_dir(std::env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .arg("metadata")
        .run();

    let json: serde_json::Value = serde_json::from_str(&json).expect("valid json");

    json.as_object()
        .unwrap()
        .get("packages")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|val| {
            let package = val.as_object().unwrap();
            let name = package.get("name").unwrap().as_str().unwrap().to_owned();
            let manifest_path =
                PathBuf::from(package.get("manifest_path").unwrap().as_str().unwrap())
                    .parent()
                    .unwrap()
                    .to_owned();
            (name, manifest_path)
        })
        .collect::<HashMap<_, _>>()
}

/// Extension trait for [`Command`].
trait CommandExt {
    /// Sanitize environment variables.
    fn sanitize_env(&mut self) -> &mut Self;

    /// Run command, check status, and convert output to a string.
    fn run(&mut self) -> String;
}

impl CommandExt for Command {
    fn sanitize_env(&mut self) -> &mut Self {
        let mut cmd = self.env_clear();

        // Code inspired by
        // https://github.com/bytecodealliance/componentize-py/blob/139d0ed85f09095e0a4cfa112e97ce589371315e/build.rs#L117-L125
        for (k, v) in std::env::vars_os() {
            let Ok(k) = k.into_string() else {
                continue;
            };
            if k.starts_with("CARGO") || k.starts_with("RUST") {
                continue;
            }
            cmd = cmd.env(k, v);
        }

        cmd
    }

    fn run(&mut self) -> String {
        let output = self
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
            .wait_with_output()
            .unwrap();

        assert!(output.status.success());
        String::from_utf8(output.stdout).expect("valid UTF-8")
    }
}

/// Known cargo profile.
#[derive(Debug, Clone, Copy)]
enum Profile {
    /// Debug/dev.
    Debug,

    /// Release.
    Release,
}

impl Profile {
    /// Get static string for profile.
    fn as_str(&self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Release => "release",
        }
    }
}

impl FromStr for Profile {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "debug" => Ok(Self::Debug),
            "release" => Ok(Self::Release),
            other => Err(other.to_owned()),
        }
    }
}

impl std::fmt::Display for Profile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

/// Feature description.
struct Feature {
    /// Lowercase feature name.
    name: &'static str,

    /// Package that contains the feature code.
    package: &'static str,

    /// `just` command prefix that compiles the feature.
    ///
    /// This will call `just prefix{profile}` within the package directory.
    just_cmd_prefix: &'static str,

    /// Path components to file in target directory.
    ///
    /// So `["foo", "bar.bin"]` will resolve to `CARGO_TARGET_DIR/wasm32-wasip2/foo/bar.bin`.
    just_out_file: &'static [&'static str],
}

impl Feature {
    /// Build or stub feature.
    fn build_or_stub(
        &self,
        stub: bool,
        profile: Profile,
        package_locations: &HashMap<String, PathBuf>,
    ) {
        let Self {
            name,
            package,
            just_cmd_prefix,
            just_out_file,
        } = self;

        let name_upper = name.to_uppercase();
        if std::env::var_os(format!("CARGO_FEATURE_{name_upper}")).is_none() {
            // feature not selected
            return;
        }

        let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

        let out_file = if stub {
            let out_file = out_dir.join(format!("{name}.wasm"));
            // write empty stub file
            std::fs::write(&out_file, b"").unwrap();
            out_file
        } else {
            let target_dir = out_dir.join(name);

            just_build(
                package_locations.get(*package).unwrap(),
                &format!("{just_cmd_prefix}{profile}"),
                &target_dir,
            );

            just_out_file.iter().fold(
                target_dir.join("wasm32-wasip2").join(profile.as_str()),
                |path, part| path.join(part),
            )
        };

        println!(
            "cargo::rustc-env=BIN_PATH_{name_upper}={}",
            out_file.display(),
        );
    }
}

/// Build a target with `just`.
fn just_build(cwd: &Path, just_cmd: &str, cargo_target_dir: &Path) {
    Command::new("just")
        .current_dir(cwd)
        .arg(just_cmd)
        .sanitize_env()
        .env("CARGO_TARGET_DIR", cargo_target_dir.as_os_str())
        .run();
}

/// All supported features.
///
/// This must be in-sync with the feature list in `Cargo.toml` and the imports in `src/lib.rs`.
const FEATURES: &[Feature] = &[
    Feature {
        name: "example",
        package: "datafusion-udf-wasm-guest",
        just_cmd_prefix: "build-add-one-",
        just_out_file: &["examples", "add_one.wasm"],
    },
    Feature {
        name: "python",
        package: "datafusion-udf-wasm-python",
        just_cmd_prefix: "",
        just_out_file: &["datafusion_udf_wasm_python.wasm"],
    },
];
