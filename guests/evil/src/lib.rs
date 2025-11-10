//! Library that multiplexes different evil payloads.
//!
//! We need that because evil payloads may act long before the actual UDFs are available.
use std::sync::Arc;

use datafusion_common::Result as DataFusionResult;
use datafusion_expr::ScalarUDFImpl;
use datafusion_udf_wasm_guest::export;

mod root;
mod runtime;

/// Method that returns the root filesystem.
type RootFn = Box<dyn Fn() -> Option<Vec<u8>>>;

/// Method that enumerates UDFs.
type UdfsFn = Box<dyn Fn(String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>>>;

/// An evil.
struct Evil {
    /// Root file system.
    root: RootFn,

    /// Returns  UDFs.
    udfs: UdfsFn,
}

impl Evil {
    /// Get evil, multiplexed by env.
    fn get() -> Self {
        match std::env::var("EVIL").expect("evil specified").as_str() {
            "root::many_files" => Self {
                root: Box::new(root::many_files::root),
                udfs: Box::new(root::many_files::udfs),
            },
            "runtime" => Self {
                root: Box::new(runtime::root),
                udfs: Box::new(runtime::udfs),
            },
            other => panic!("unknown evil: {other}"),
        }
    }
}

/// Return root file system.
fn root() -> Option<Vec<u8>> {
    (Evil::get().root)()
}

/// Returns our evil UDFs.
fn udfs(source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    (Evil::get().udfs)(source)
}

export! {
    root_fs_tar: root,
    scalar_udfs: udfs,
}
