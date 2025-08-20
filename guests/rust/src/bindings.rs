use wit_bindgen::generate;

generate!({
    world: "datafusion",
    path: "../../wit",
    export_macro_name: "_export",
    pub_export_macro: true,
});
