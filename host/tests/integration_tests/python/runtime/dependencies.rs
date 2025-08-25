use std::sync::Arc;

use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

use crate::integration_tests::python::test_utils::python_scalar_udf;

#[tokio::test(flavor = "multi_thread")]
async fn call_other_function() {
    const CODE: &str = "
def _sub1(x: int) -> int:
    return x + 1

def _sub2(x: int) -> int:
    return x * 10

def foo(x: int) -> int:
    return _sub1(x) + _sub2(x)
";

    let udf = python_scalar_udf(CODE).await.unwrap();
    let ColumnarValue::Array(array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter([
                Some(1),
                Some(2),
                Some(3),
            ])))],
            arg_fields: vec![Arc::new(Field::new("a1", DataType::Int64, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
        })
        .unwrap()
    else {
        panic!("should be an array")
    };
    assert_eq!(
        array.as_ref(),
        &Int64Array::from_iter([Some(12), Some(23), Some(34)]) as &dyn Array,
    );
}
