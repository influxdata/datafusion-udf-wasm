//! Convert [`arrow`] types to/from bytes.
//!
//! This uses the [Arrow IPC] schema.
//!
//!
//! [Arrow IPC]: https://arrow.apache.org/docs/format/IPC.html
use std::{io::Cursor, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, RecordBatch},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    ipc::{
        convert::{IpcSchemaEncoder, fb_to_schema},
        reader::StreamReader,
        root_as_schema,
        writer::StreamWriter,
    },
};

/// Convert an [`Array`] to bytes.
///
/// This is done by encoding writing this as a [`RecordBatch`] with a single [`Field`].
///
/// See [`bytes2array`] for the reverse method.
pub fn array2bytes(array: ArrayRef) -> Vec<u8> {
    let buffer = Vec::new();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        array.data_type().clone(),
        array.null_count() > 0,
    )]));
    let mut writer = StreamWriter::try_new(buffer, &schema).expect("writing to buffer never fails");

    let batch = RecordBatch::try_new(schema, vec![array]).expect("batch always valid");
    writer.write(&batch).expect("writing to buffer never fails");

    writer.into_inner().expect("writing to buffer never fails")
}

/// Decodes [`Array`] from bytes.
///
/// See [`array2bytes`] for the reverse method and the format description.
pub fn bytes2array(bytes: &[u8]) -> Result<ArrayRef, ArrowError> {
    let bytes = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(bytes, None)?;
    let Some(res) = reader.next() else {
        return Err(ArrowError::InvalidArgumentError(
            "no record batch found".to_owned(),
        ));
    };
    let batch = res?;
    let columns = batch.columns();
    if columns.len() != 1 {
        return Err(ArrowError::InvalidArgumentError("invalid batch".to_owned()));
    }
    let array = Arc::clone(&columns[0]);
    if reader.next().is_some() || !reader.is_finished() {
        return Err(ArrowError::InvalidArgumentError("trailing data".to_owned()));
    }
    Ok(array)
}

/// Encodes [`DataType`] as bytes.
///
/// This is done by embedding the [`DataType`] into a [`Schema`] with a single [`Field`].
///
/// See [`bytes2datatype`] for the reverse method.
pub fn datatype2bytes(dt: DataType) -> Vec<u8> {
    let schema = Schema::new(vec![Field::new("a", dt, false)]);
    let fb = IpcSchemaEncoder::new().schema_to_fb(&schema);
    fb.finished_data().to_owned()
}

/// Decodes [`DataType`] from bytes.
///
/// See [`datatype2bytes`] for the reverse method and format description.
pub fn bytes2datatype(bytes: &[u8]) -> Result<DataType, ArrowError> {
    let ipc_schema =
        root_as_schema(bytes).map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?;
    let schema = fb_to_schema(ipc_schema);
    if schema.fields().len() != 1 {
        return Err(ArrowError::InvalidArgumentError(
            "Invalid schema".to_owned(),
        ));
    }
    let field = schema
        .fields
        .into_iter()
        .next()
        .expect("just checked length");
    Ok(field.data_type().clone())
}
