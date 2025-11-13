use arrow::datatypes::DataType;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};

use crate::integration_tests::python::test_utils::python_scalar_udf;

#[tokio::test]
async fn test_reduction_bool() {
    const CODE: &str = "
def foo(x: bool | None | bool | None) -> None:
    pass
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(vec![DataType::Boolean], Volatility::Volatile),
    );
}

#[tokio::test]
async fn test_reduction_int() {
    const CODE: &str = "
def foo(x: int | None | int | None) -> None:
    pass
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(vec![DataType::Int64], Volatility::Volatile),
    );
}

#[tokio::test]
async fn test_reduction_str() {
    const CODE: &str = "
def foo(x: str | None | str | None) -> None:
    pass
";
    let udf = python_scalar_udf(CODE).await.unwrap();

    assert_eq!(
        udf.signature(),
        &Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
    );
}
