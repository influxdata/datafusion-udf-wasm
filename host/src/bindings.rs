use wasmtime::component::bindgen;

bindgen!({
    world: "datafusion",
    path: "../wit/world.wit",
    async: true,
});
