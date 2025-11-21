// Docs are not strictly required for tests.
#![expect(missing_docs)]

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, Int64Array, ListArray, RecordBatch, RecordBatchOptions, StringDictionaryBuilder,
    },
    datatypes::{DataType, Field, Int32Type, Schema},
    ipc::writer::StreamWriter,
};
use datafusion_udf_wasm_arrow2bytes::{array2bytes, bytes2array};

#[test]
fn test_roundtrip() {
    roundtrip(Arc::new(Int64Array::from_iter([Some(1), None, Some(3)])));

    let mut builder = StringDictionaryBuilder::<Int32Type>::new();
    builder.append("foo").unwrap();
    builder.append_null();
    builder.append("bar").unwrap();
    builder.append("foo").unwrap();
    roundtrip(Arc::new(builder.finish()));
}

#[test]
fn test_err_invalid_bytes_1() {
    let err = bytes2array(b"foobar").unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Io error: failed to fill whole buffer",
    );
}

#[test]
fn test_err_invalid_bytes_2() {
    let err = bytes2array(b"\x01\0\0\0foobar").unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Parser error: Unable to get root as message: RangeOutOfBounds { range: 0..4, error_trace: ErrorTrace([]) }",
    );
}

#[test]
fn test_err_no_schema() {
    let err = bytes2array(b"").unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Ipc error: Expected schema message, found empty stream.",
    );
}

#[test]
fn test_err_no_record_batch() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let writer = StreamWriter::try_new(Vec::new(), &schema).expect("writing to buffer never fails");
    let bytes = writer.into_inner().unwrap();
    let err = bytes2array(&bytes).unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Invalid argument error: no record batch found",
    );
}

#[test]
fn test_err_no_columns() {
    let schema = Arc::new(Schema::empty());
    let batch = RecordBatch::try_new_with_options(
        Arc::clone(&schema),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let mut writer =
        StreamWriter::try_new(Vec::new(), &schema).expect("writing to buffer never fails");
    writer.write(&batch).unwrap();
    let bytes = writer.into_inner().unwrap();
    let err = bytes2array(&bytes).unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Invalid argument error: invalid batch",
    );
}

#[test]
fn test_err_multiple_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::new_null(0)),
            Arc::new(Int64Array::new_null(0)),
        ],
    )
    .unwrap();
    let mut writer =
        StreamWriter::try_new(Vec::new(), &schema).expect("writing to buffer never fails");
    writer.write(&batch).unwrap();
    let bytes = writer.into_inner().unwrap();
    let err = bytes2array(&bytes).unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Invalid argument error: invalid batch",
    );
}

#[test]
fn test_err_two_messages() {
    let mut bytes = array2bytes(Arc::new(Int64Array::new_null(0)));
    let bytes2 = bytes.clone();
    bytes.extend_from_slice(&bytes2);
    let err = bytes2array(&bytes).unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Invalid argument error: trailing data",
    );
}

#[test]
fn test_deeply_nested() {
    let dt = (0..100).fold(DataType::Int64, |dt, _| {
        DataType::List(Arc::new(Field::new("x", dt, true)))
    });
    let bytes = array2bytes(Arc::new(ListArray::new_null(
        Arc::new(Field::new("x", dt, true)),
        0,
    )));
    let err = bytes2array(&bytes).unwrap_err();
    insta::assert_snapshot!(
        err,
        @"Parser error: Unable to get root as message: DepthLimitReached"
    );
}

#[track_caller]
fn roundtrip(array: ArrayRef) {
    let bytes = array2bytes(Arc::clone(&array));
    let array2 = bytes2array(&bytes).unwrap();
    assert_eq!(&array, &array2);
}
