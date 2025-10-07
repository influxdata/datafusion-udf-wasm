//! Build script.

use std::{
    collections::HashMap,
    path::PathBuf,
    process::{Command, Stdio},
    str::FromStr,
};

fn main() {
    let profile: Profile = std::env::var("PROFILE").unwrap().parse().unwrap();
    let package_locations = package_locations();

    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

    // It seems that rust-analyzer uses a different nightly compiler and mixing different nightly compilers doesn't
    // work. So we need to pick a working directory by version.
    let rustc_nightly_version = rustc_nightly_version();
    let target_base_dir = out_dir.join(rustc_nightly_version);

    if std::env::var_os("CARGO_FEATURE_EXAMPLE").is_some() {
        let target_dir = target_base_dir.join("example");

        Command::new("just")
            .current_dir(package_locations.get("datafusion-udf-wasm-guest").unwrap())
            .arg(format!("build-add-one-{profile}"))
            .sanitize_env()
            .env("CARGO_TARGET_DIR", target_dir.as_os_str())
            .run();

        let binary_path = target_dir
            .join("wasm32-wasip2")
            .join(profile.as_str())
            .join("examples")
            .join("add_one.wasm");
        println!(
            "cargo::rustc-env=BIN_PATH_EXAMPLE={}",
            binary_path.display(),
        );
    }

    if std::env::var_os("CARGO_FEATURE_PYTHON").is_some() {
        let target_dir = target_base_dir.join("python");

        Command::new("just")
            .current_dir(package_locations.get("datafusion-udf-wasm-python").unwrap())
            .arg(profile.as_str())
            .sanitize_env()
            .env("CARGO_TARGET_DIR", target_dir.as_os_str())
            .run();

        let binary_path = target_dir
            .join("wasm32-wasip2")
            .join(profile.as_str())
            .join("datafusion_udf_wasm_python.wasm");
        println!("cargo::rustc-env=BIN_PATH_PYTHON={}", binary_path.display(),);
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

/// Get rustc nightly version (as revision).
fn rustc_nightly_version() -> String {
    Command::new("rustc")
        .current_dir(std::env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .arg("+nightly")
        .arg("--version")
        .sanitize_env()
        .run()
        .split_whitespace()
        .nth(2)
        .unwrap()
        .chars()
        .skip(1)
        .collect()
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
        for k in ["HOME", "PATH", "USER"] {
            let Some(v) = std::env::var_os(k) else {
                continue;
            };
            cmd = cmd.env(k, v);
        }
        for (k, v) in std::env::vars_os() {
            let Ok(k) = k.into_string() else {
                continue;
            };
            if k.starts_with("XDG_") {
                cmd = cmd.env(k, v);
            }
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
