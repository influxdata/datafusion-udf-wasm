use datafusion_common::DataFusionError;

use crate::integration_tests::python::test_utils::python_scalar_udfs;

#[tokio::test(flavor = "multi_thread")]
async fn test_invalid_syntax() {
    const CODE: &str = ")";

    insta::assert_snapshot!(
        err(CODE).await,
        @r"
    scalar_udfs
    caused by
    Error during planning: SyntaxError: unmatched ')' (<string>, line 1)
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_missing_return_type() {
    const CODE: &str = "
def add_one(x: int):
    return x + 1
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: inspect type of `add_one`

    Caused by:
    TypeError: inspect return type

    Caused by:
    TypeError: type missing
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_missing_arg_type() {
    const CODE: &str = "
def add_one(x) -> int:
    return x + 1
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: inspect type of `add_one`

    Caused by:
    TypeError: inspect parameter 1

    Caused by:
    TypeError: type missing
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_union_type_2() {
    const CODE: &str = "
def add_one(x: int | str) -> int:
    return x + 1
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: inspect type of `add_one`

    Caused by:
    TypeError: inspect parameter 1

    Caused by:
    TypeError: only unions with None are supported
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_union_type_3() {
    const CODE: &str = "
def add_one(x: int | str | None) -> int:
    return x + 1
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: inspect type of `add_one`

    Caused by:
    TypeError: inspect parameter 1

    Caused by:
    TypeError: only unions of length 2 are supported, got 3
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_type_annotation_is_not_a_type() {
    const CODE: &str = "
def add_one(x: 1337) -> int:
    return x + 1
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: inspect type of `add_one`

    Caused by:
    TypeError: inspect parameter 1

    Caused by:
    TypeError: unknown annotation type: `1337` of type `int`
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unsupported_type() {
    const CODE: &str = "
def add_one(x: list[int]) -> int:
    return x + 1
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: inspect type of `add_one`

    Caused by:
    TypeError: inspect parameter 1

    Caused by:
    TypeError: unknown annotation type: `list[int]` of type `GenericAlias`
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_custom_type() {
    const CODE: &str = "
class C:
    pass

def add_one(x: C) -> int:
    return x + 1
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: inspect type of `add_one`

    Caused by:
    TypeError: inspect parameter 1

    Caused by:
    TypeError: unknown annotation type: `<class '__main__.C'>` of type `type`
    ",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_exception() {
    const CODE: &str = "
raise Exception('foo')
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r"
    scalar_udfs
    caused by
    Error during planning: Exception: foo
    ",
    );
}

async fn err(code: &str) -> DataFusionError {
    python_scalar_udfs(code).await.unwrap_err()
}
