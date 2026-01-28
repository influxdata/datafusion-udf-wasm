//! Library that multiplexes different evil payloads.
//!
//! We need that because evil payloads may act long before the actual UDFs are available.
use std::sync::Arc;

use datafusion_common::Result as DataFusionResult;
use datafusion_expr::ScalarUDFImpl;
use datafusion_udf_wasm_guest::export;

mod common;
mod complex;
mod env;
mod fs;
mod net;
mod return_data;
mod root;
mod runtime;
mod spin;

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
            "complex::error" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(complex::error::udfs),
            },
            "complex::many_inputs" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(complex::many_inputs::udfs),
            },
            "complex::params_long_name" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(complex::params_long_name::udfs),
            },
            "complex::params_many" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(complex::params_many::udfs),
            },
            "complex::return_type" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(complex::return_type::udfs),
            },
            "complex::return_value" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(complex::return_value::udfs),
            },
            "complex::udf_long_name" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(complex::udf_long_name::udfs),
            },
            "complex::udfs_duplicate_names" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(complex::udfs_duplicate_names::udfs),
            },
            "complex::udfs_many" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(complex::udfs_many::udfs),
            },
            "env" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(env::udfs),
            },
            "fs" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(fs::udfs),
            },
            "net" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(net::udfs),
            },
            "return_data" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(return_data::udfs),
            },
            "root::invalid_entry" => Self {
                root: Box::new(root::invalid_entry::root),
                udfs: Box::new(common::udfs_empty),
            },
            "root::large_file" => Self {
                root: Box::new(root::large_file::root),
                udfs: Box::new(common::udfs_empty),
            },
            "root::many_files" => Self {
                root: Box::new(root::many_files::root),
                udfs: Box::new(common::udfs_empty),
            },
            "root::not_tar" => Self {
                root: Box::new(root::not_tar::root),
                udfs: Box::new(common::udfs_empty),
            },
            "root::tar_too_large" => Self {
                root: Box::new(root::tar_too_large::root),
                udfs: Box::new(common::udfs_empty),
            },
            "root::path_long" => Self {
                root: Box::new(root::path_long::root),
                udfs: Box::new(common::udfs_empty),
            },
            "root::sparse" => Self {
                root: Box::new(root::sparse::root),
                udfs: Box::new(common::udfs_empty),
            },
            "root::unsupported_entry" => Self {
                root: Box::new(root::unsupported_entry::root),
                udfs: Box::new(common::udfs_empty),
            },
            "runtime" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(runtime::udfs),
            },
            "spin::root" => Self {
                root: Box::new(spin::root::root),
                udfs: Box::new(common::udfs_empty),
            },
            "spin::udf_invoke" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(spin::udf_invoke::udfs),
            },
            "spin::udf_name" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(spin::udf_name::udfs),
            },
            "spin::udf_return_type_exact" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(spin::udf_return_type_exact::udfs),
            },
            "spin::udf_return_type_other" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(spin::udf_return_type_other::udfs),
            },
            "spin::udf_signature" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(spin::udf_signature::udfs),
            },
            "spin::udfs" => Self {
                root: Box::new(common::root_empty),
                udfs: Box::new(spin::udfs::udfs),
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
