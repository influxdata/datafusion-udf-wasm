use datafusion::{
    arrow::{
        datatypes::{DataType, Field, Schema},
        ipc::{
            convert::{IpcSchemaEncoder, fb_to_schema},
            root_as_schema,
        },
    },
    error::DataFusionError,
};

pub fn datatype2bytes(dt: DataType) -> Vec<u8> {
    let schema = Schema::new(vec![Field::new("a", dt, false)]);
    let fb = IpcSchemaEncoder::new().schema_to_fb(&schema);
    fb.finished_data().to_owned()
}

pub fn bytes2datatype(bytes: &[u8]) -> Result<DataType, DataFusionError> {
    let ipc_schema = root_as_schema(bytes).map_err(|e| DataFusionError::Internal(e.to_string()))?;
    let schema = fb_to_schema(ipc_schema);
    if schema.fields().len() != 1 {
        return Err(DataFusionError::Internal("Invalid schema".to_owned()));
    }
    let field = schema
        .fields
        .into_iter()
        .next()
        .expect("just checked length");
    Ok(field.data_type().clone())
}
