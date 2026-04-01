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
mod runtime;
mod spin;

/// Method that enumerates UDFs.
type UdfsFn = Box<dyn Fn(String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>>>;

/// An evil.
struct Evil {
    /// Returns  UDFs.
    udfs: UdfsFn,
}

impl Evil {
    /// Get evil, multiplexed by env.
    fn get() -> Self {
        match std::env::var("EVIL").expect("evil specified").as_str() {
            "complex::error" => Self {
                udfs: Box::new(complex::error::udfs),
            },
            "complex::many_inputs" => Self {
                udfs: Box::new(complex::many_inputs::udfs),
            },
            "complex::params_long_name" => Self {
                udfs: Box::new(complex::params_long_name::udfs),
            },
            "complex::params_many" => Self {
                udfs: Box::new(complex::params_many::udfs),
            },
            "complex::return_type" => Self {
                udfs: Box::new(complex::return_type::udfs),
            },
            "complex::return_value" => Self {
                udfs: Box::new(complex::return_value::udfs),
            },
            "complex::udf_long_name" => Self {
                udfs: Box::new(complex::udf_long_name::udfs),
            },
            "complex::udfs_duplicate_names" => Self {
                udfs: Box::new(complex::udfs_duplicate_names::udfs),
            },
            "complex::udfs_many" => Self {
                udfs: Box::new(complex::udfs_many::udfs),
            },
            "env" => Self {
                udfs: Box::new(env::udfs),
            },
            "fs" => Self {
                udfs: Box::new(fs::udfs),
            },
            "fs::large_file" => Self {
                udfs: Box::new(common::udfs_empty),
            },
            "fs::many_files" => Self {
                udfs: Box::new(common::udfs_empty),
            },
            "fs::path_long" => Self {
                udfs: Box::new(common::udfs_empty),
            },
            "net" => Self {
                udfs: Box::new(net::udfs),
            },
            "return_data" => Self {
                udfs: Box::new(return_data::udfs),
            },
            "runtime" => Self {
                udfs: Box::new(runtime::udfs),
            },
            "spin::root" => Self {
                udfs: Box::new(spin::root::root),
            },
            "spin::udf_invoke" => Self {
                udfs: Box::new(spin::udf_invoke::udfs),
            },
            "spin::udf_name" => Self {
                udfs: Box::new(spin::udf_name::udfs),
            },
            "spin::udf_return_type_exact" => Self {
                udfs: Box::new(spin::udf_return_type_exact::udfs),
            },
            "spin::udf_return_type_other" => Self {
                udfs: Box::new(spin::udf_return_type_other::udfs),
            },
            "spin::udf_signature" => Self {
                udfs: Box::new(spin::udf_signature::udfs),
            },
            "spin::udfs" => Self {
                udfs: Box::new(spin::udfs::udfs),
            },
            other => panic!("unknown evil: {other}"),
        }
    }
}

/// Returns our evil UDFs.
fn udfs(source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    (Evil::get().udfs)(source)
}

export! {
    scalar_udfs: udfs,
}
