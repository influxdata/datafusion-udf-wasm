// Docs are not strictly required for tests.
#![expect(missing_docs)]

use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema},
    ipc::convert::IpcSchemaEncoder,
};
use datafusion_udf_wasm_arrow2bytes::{bytes2datatype, datatype2bytes};

#[test]
fn test_roundtrip() {
    roundtrip(DataType::Int64);
    roundtrip(DataType::List(Arc::new(Field::new(
        "inner",
        DataType::Utf8,
        true,
    ))));
}

#[test]
fn test_err_invalid_bytes() {
    let err = bytes2datatype(b"").unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Invalid argument error: Range [0, 4) is out of bounds."
    );
}

#[test]
fn test_err_no_field() {
    let schema = Schema::empty();
    let fb = IpcSchemaEncoder::new().schema_to_fb(&schema);
    let data = fb.finished_data().to_owned();
    let err = bytes2datatype(&data).unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Invalid argument error: Invalid schema",
    );
}

#[test]
fn test_err_two_fields() {
    let schema = Schema::new(vec![
        Field::new("foo", DataType::Binary, false),
        Field::new("bar", DataType::Binary, false),
    ]);
    let fb = IpcSchemaEncoder::new().schema_to_fb(&schema);
    let data = fb.finished_data().to_owned();
    let err = bytes2datatype(&data).unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Invalid argument error: Invalid schema",
    );
}

#[test]
fn test_deeply_nested() {
    let dt = (0..100).fold(DataType::Int64, |dt, _| {
        DataType::List(Arc::new(Field::new("x", dt, true)))
    });
    let bytes = datatype2bytes(dt);
    let err = bytes2datatype(&bytes).unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Invalid argument error: Nested table depth limit reached."
    );
}

#[track_caller]
fn roundtrip(dt: DataType) {
    let bytes = datatype2bytes(dt.clone());
    let dt2 = bytes2datatype(&bytes).unwrap();
    assert_eq!(dt, dt2);
}
