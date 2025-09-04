//! Auto-generated bindings based on WIT.
use wasmtime::component::bindgen;

bindgen!({
    world: "datafusion",
    path: "../wit/world.wit",
    exports: { default: async },
});
