use wit_bindgen::generate;

generate!({
    world: "datafusion",
    path: "../../wit",
    pub_export_macro: true,
});
