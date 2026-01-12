//! Host-code for WebAssembly-based [DataFusion] UDFs.
//!
//!
//! [DataFusion]: https://datafusion.apache.org/

pub use crate::{
    component::WasmComponentPrecompiled,
    conversion::limits::TrustedDataLimits,
    http::{
        AllowCertainHttpRequests, HttpMethod, HttpRequestMatcher, HttpRequestRejected,
        HttpRequestValidator, RejectAllHttpRequests,
    },
    limiter::StaticResourceLimits,
    permissions::WasmPermissions,
    udf::WasmScalarUdf,
    vfs::limits::VfsLimits,
};

#[cfg(feature = "compiler")]
pub use crate::component::CompilationFlags;

// unused-crate-dependencies false positives
#[cfg(test)]
use datafusion_udf_wasm_bundle as _;
#[cfg(test)]
use gungraun as _;
#[cfg(test)]
use regex as _;
#[cfg(test)]
use target_lexicon as _;
#[cfg(test)]
use wiremock as _;

mod bindings;
mod component;
mod conversion;
mod error;
mod http;
mod ignore_debug;
mod limiter;
mod linker;
mod permissions;
mod state;
mod tokio_helpers;
mod udf;
mod vfs;
