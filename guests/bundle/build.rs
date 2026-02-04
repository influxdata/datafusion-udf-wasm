//! Build script.
#![allow(unused_crate_dependencies)]

use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str::FromStr,
};

fn main() {
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
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

    let mut gen_file = File::create(out_dir.join("gen.rs")).unwrap();

    for feature in FEATURES {
        println!("processing {}", feature.name);
        feature.build_or_stub(stub, profile, &package_locations, &out_dir, &mut gen_file);
    }

    gen_file.flush().unwrap();

    println!("cargo::rerun-if-changed=build.rs");
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

            // Generally we do NOT want to forward rustc and cargo arguments set for this build
            // script. However some of them are used in CI to speed up compilation and reduce disk
            // space usage. So we hard-code these here.
            if (k.starts_with("CARGO") || k.starts_with("RUST"))
                && !["CARGO_PROFILE_DEV_DEBUG", "CARGO_INCREMENTAL"]
                    .into_iter()
                    .any(|s| s == k)
            {
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

/// Artifact type.
enum ArtifactType {
    /// Library.
    Lib,

    /// Example.
    Example(&'static str),
}

/// Just(file) command.
struct JustCmd {
    /// Artifact type.
    artifact_type: ArtifactType,

    /// Name of the resulting constant.
    const_name: &'static str,

    /// Documentation for the created constant.
    doc: &'static str,
}

/// Feature description.
struct Feature {
    /// Lowercase feature name.
    name: &'static str,

    /// Package that contains the feature code.
    package: &'static str,

    /// Just commands.
    just_cmds: &'static [JustCmd],
}

impl Feature {
    /// Build or stub feature.
    fn build_or_stub(
        &self,
        stub: bool,
        profile: Profile,
        package_locations: &HashMap<String, PathBuf>,
        out_dir: &Path,
        gen_file: &mut File,
    ) {
        let Self {
            name,
            package,
            just_cmds,
        } = self;

        let name_upper = name.to_uppercase();
        if std::env::var_os(format!("CARGO_FEATURE_{name_upper}")).is_none() {
            // feature not selected
            return;
        }

        let cwd = package_locations.get(*package).unwrap();
        let target_dir = out_dir.join(name);

        for just_cmd in *just_cmds {
            let JustCmd {
                artifact_type,
                const_name,
                doc,
            } = just_cmd;
            let out_file = if stub {
                let out_file = out_dir.join(format!("{name}.wasm"));
                // write empty stub file
                std::fs::write(&out_file, b"").unwrap();
                out_file
            } else {
                let mut just_cmd = "build-".to_owned();
                match artifact_type {
                    ArtifactType::Lib => {}
                    ArtifactType::Example(example) => {
                        just_cmd.push_str(example);
                        just_cmd.push('-');
                    }
                }
                just_cmd.push_str(profile.as_str());

                just_build(cwd, &just_cmd, &target_dir);

                let out = target_dir.join("wasm32-wasip2").join(profile.as_str());
                match artifact_type {
                    ArtifactType::Lib => out.join(format!("{}.wasm", package.replace("-", "_"))),
                    ArtifactType::Example(example) => out
                        .join("examples")
                        .join(format!("{}.wasm", example.replace("-", "_"))),
                }
            };

            println!(
                "cargo::rustc-env=BIN_PATH_{const_name}={}",
                out_file.display(),
            );

            writeln!(gen_file, "/// {doc}").unwrap();
            writeln!(gen_file, r#"#[cfg(feature = "{name}")]"#).unwrap();
            writeln!(gen_file, r#"pub static BIN_{const_name}: &[u8] = include_bytes!(env!("BIN_PATH_{const_name}"));"#).unwrap();

            // we cannot really depend directly on examples, so we need to tell Cargo about it
            if let ArtifactType::Example(example) = artifact_type {
                println!(
                    "cargo::rerun-if-changed={}",
                    cwd.join("examples")
                        .join(format!("{}.rs", example.replace("-", "_")))
                        .display(),
                );
            }
        }
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
/// This must be in-sync with the feature list in `Cargo.toml`.
const FEATURES: &[Feature] = &[
    Feature {
        name: "evil",
        package: "datafusion-udf-wasm-evil",
        just_cmds: &[JustCmd {
            artifact_type: ArtifactType::Lib,
            const_name: "EVIL",
            doc: "Evil payloads.",
        }],
    },
    Feature {
        name: "example",
        package: "datafusion-udf-wasm-guest",
        just_cmds: &[
            JustCmd {
                artifact_type: ArtifactType::Example("add-one"),
                const_name: "EXAMPLE_ADD_ONE",
                doc: r#""add-one" example."#,
            },
            JustCmd {
                artifact_type: ArtifactType::Example("sub-str"),
                const_name: "EXAMPLE_SUB_STR",
                doc: r#""sub-str" example."#,
            },
        ],
    },
    Feature {
        name: "python",
        package: "datafusion-udf-wasm-python",
        just_cmds: &[JustCmd {
            artifact_type: ArtifactType::Lib,
            const_name: "PYTHON",
            doc: "Python UDF.",
        }],
    },
];
