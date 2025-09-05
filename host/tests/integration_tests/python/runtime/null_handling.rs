use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, Int64Array},
    datatypes::{DataType, Field},
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

use crate::integration_tests::{
    python::test_utils::python_scalar_udf, test_utils::ColumnarValueExt,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_no_optionals() {
    const CODE: &str = "
def add(x: int, y: int) -> int:
    assert x is not None
    assert y is not None
    return x + y
";

    assert_eq!(
        xy_null_test(CODE).await.as_ref(),
        &Int64Array::from_iter([None, None, None, Some(44)]) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_first_arg_optional() {
    const CODE: &str = "
def add(x: int | None, y: int) -> int:
    if x is None:
        x = 9
    assert y is not None
    return x + y
";

    assert_eq!(
        xy_null_test(CODE).await.as_ref(),
        &Int64Array::from_iter([None, Some(29), None, Some(44)]) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_second_arg_optional() {
    const CODE: &str = "
def add(x: int, y: int | None) -> int:
    assert x is not None
    if y is None:
        y = 90
    return x + y
";

    assert_eq!(
        xy_null_test(CODE).await.as_ref(),
        &Int64Array::from_iter([None, None, Some(93), Some(44)]) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_both_args_optional() {
    const CODE: &str = "
def add(x: int | None, y: int | None) -> int:
    if x is None:
        x = 9
    if y is None:
        y = 90
    return x + y
";

    assert_eq!(
        xy_null_test(CODE).await.as_ref(),
        &Int64Array::from_iter([Some(99), Some(29), Some(93), Some(44)]) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_optional_passthrough() {
    const CODE: &str = "
def add(x: int | None, y: int | None) -> int | None:
    if x is None or y is None:
        return None
    return x + y
";

    assert_eq!(
        xy_null_test(CODE).await.as_ref(),
        &Int64Array::from_iter([None, None, None, Some(44)]) as &dyn Array,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_optional_flip() {
    const CODE: &str = "
def add(x: int | None, y: int | None) -> int | None:
    if x is None:
        x = 9
    else:
        return None

    if y is None:
        y = 90
    else:
        return None

    return x + y
";

    assert_eq!(
        xy_null_test(CODE).await.as_ref(),
        &Int64Array::from_iter([Some(99), None, None, None]) as &dyn Array,
    );
}

async fn xy_null_test(code: &str) -> ArrayRef {
    let udf = python_scalar_udf(code).await.unwrap();
    udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                None,
                None,
                Some(3),
                Some(4),
            ]))),
            ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                None,
                Some(20),
                None,
                Some(40),
            ]))),
        ],
        arg_fields: vec![
            Arc::new(Field::new("x", DataType::Int64, true)),
            Arc::new(Field::new("y", DataType::Int64, true)),
        ],
        number_rows: 4,
        return_field: Arc::new(Field::new("r", DataType::Int64, true)),
    })
    .unwrap()
    .unwrap_array()
}
