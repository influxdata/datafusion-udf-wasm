use crate::integration_tests::python::test_utils::python_scalar_udfs;
use datafusion_expr::ScalarUDFImpl;

#[tokio::test(flavor = "multi_thread")]
async fn test_underscore() {
    const CODE: &str = "
def foo(x: int) -> int:
    return x

def _bar(x: int) -> int:
    return x
";

    assert_eq!(found_udfs(CODE).await, ["foo".to_owned()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_non_callalbes() {
    const CODE: &str = "
variable = 1

def foo(x: int) -> int:
    return x
";

    assert_eq!(found_udfs(CODE).await, ["foo".to_owned()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_imports() {
    const CODE: &str = "
from sys import exit

def foo(x: int) -> int:
    return x
";

    assert_eq!(found_udfs(CODE).await, ["foo".to_owned()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_classes() {
    const CODE: &str = "
class C:
    pass

def foo(x: int) -> int:
    return x
";

    assert_eq!(found_udfs(CODE).await, ["foo".to_owned()]);
}

async fn found_udfs(code: &str) -> Vec<String> {
    let udfs = python_scalar_udfs(code).await.unwrap();
    let mut udfs = udfs
        .into_iter()
        .map(|udf| udf.name().to_owned())
        .collect::<Vec<_>>();
    udfs.sort_unstable();
    udfs
}
