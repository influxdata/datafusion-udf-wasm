// Docs are not strictly required for tests.
#![expect(missing_docs)]

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int64Array, StringDictionaryBuilder},
    datatypes::Int32Type,
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

#[track_caller]
fn roundtrip(array: ArrayRef) {
    let bytes = array2bytes(Arc::clone(&array));
    let array2 = bytes2array(&bytes).unwrap();
    assert_eq!(&array, &array2);
}
