use crate::integration_tests::python::test_utils::python_scalar_udfs;
use datafusion_common::DataFusionError;

#[tokio::test]
async fn test_invalid_syntax() {
    const CODE: &str = ")";

    insta::assert_snapshot!(
        err(CODE).await,
        @r#"
    scalar_udfs
    caused by
    Error during planning:   File "<string>", line 1
        )
        ^
    SyntaxError: unmatched ')'
    "#,
    );
}

#[tokio::test]
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
    Error during planning: TypeError: type missing

    The above exception was the direct cause of the following exception:

    TypeError: inspect return type

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `add_one`
    ",
    );
}

#[tokio::test]
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
    Error during planning: TypeError: type missing

    The above exception was the direct cause of the following exception:

    TypeError: inspect parameter 1

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `add_one`
    ",
    );
}

#[tokio::test]
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
    Error during planning: TypeError: only unions of form `T | None` are supported, but got a union of 2 distinct none-NULL types

    The above exception was the direct cause of the following exception:

    TypeError: inspect parameter 1

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `add_one`
    ",
    );
}

#[tokio::test]
async fn test_union_type_2_and_none() {
    const CODE: &str = "
def add_one(x: int | str | None) -> int:
    return x + 1
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: only unions of form `T | None` are supported, but got a union of 2 distinct none-NULL types

    The above exception was the direct cause of the following exception:

    TypeError: inspect parameter 1

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `add_one`
    ",
    );
}

#[tokio::test]
async fn test_union_type_2_identical() {
    const CODE: &str = "
def add_one(x: int | str | int) -> int:
    return x + 1
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: only unions of form `T | None` are supported, but got a union of 2 distinct none-NULL types

    The above exception was the direct cause of the following exception:

    TypeError: inspect parameter 1

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `add_one`
    ",
    );
}

#[tokio::test]
async fn test_union_type_2_identical_and_none() {
    const CODE: &str = "
def add_one(x: int | None | str | int) -> int:
    return x + 1
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: only unions of form `T | None` are supported, but got a union of 2 distinct none-NULL types

    The above exception was the direct cause of the following exception:

    TypeError: inspect parameter 1

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `add_one`
    ",
    );
}

#[tokio::test]
async fn test_union_type_3() {
    const CODE: &str = "
def add_one(x: int | str | float) -> int:
    return x + 1
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r"
    scalar_udfs
    caused by
    Error during planning: TypeError: only unions of form `T | None` are supported, but got a union of 3 distinct none-NULL types

    The above exception was the direct cause of the following exception:

    TypeError: inspect parameter 1

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `add_one`
    ",
    );
}

#[tokio::test]
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
    Error during planning: TypeError: unknown annotation type: `1337` of type `int`

    The above exception was the direct cause of the following exception:

    TypeError: inspect parameter 1

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `add_one`
    ",
    );
}

#[tokio::test]
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
    Error during planning: TypeError: unknown annotation type: `list[int]` of type `GenericAlias`

    The above exception was the direct cause of the following exception:

    TypeError: inspect parameter 1

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `add_one`
    ",
    );
}

#[tokio::test]
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
    Error during planning: TypeError: unknown annotation type: `<class '__main__.C'>` of type `type`

    The above exception was the direct cause of the following exception:

    TypeError: inspect parameter 1

    The above exception was the direct cause of the following exception:

    TypeError: inspect type of `add_one`
    ",
    );
}

#[tokio::test]
async fn test_exception() {
    const CODE: &str = "
raise Exception('foo')
";

    insta::assert_snapshot!(
        err(CODE).await,
        @r#"
    scalar_udfs
    caused by
    Error during planning: Traceback (most recent call last):
      File "<string>", line 2, in <module>
    Exception: foo
    "#,
    );
}

async fn err(code: &str) -> DataFusionError {
    python_scalar_udfs(code).await.unwrap_err()
}
