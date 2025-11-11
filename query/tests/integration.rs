#![expect(
    // Docs are not strictly required for tests.
    missing_docs,
    // unused-crate-dependencies false positives
    unused_crate_dependencies,
)]

use std::collections::HashMap;

use datafusion::{
    assert_batches_eq,
    prelude::{DataFrame, SessionContext},
};
use datafusion_common::{Result as DataFusionResult, test_util::batches_to_string};
use datafusion_udf_wasm_host::WasmPermissions;
use datafusion_udf_wasm_query::{
    Lang, ParsedQuery, UdfQueryParser,
    format::{NoOpFormatter, StripIndentationFormatter},
};
use tokio::runtime::Handle;

mod integration_tests;
use datafusion_expr::async_udf::AsyncScalarUDF;

use crate::integration_tests::python::test_utils::python_component;

/// A helper struct for invoking UDF queries and validating their results.
struct UdfQueryInvocator;

impl UdfQueryInvocator {
    async fn invoke(
        ctx: &SessionContext,
        parsed_query: ParsedQuery,
    ) -> DataFusionResult<DataFrame> {
        for udf in parsed_query.udfs {
            let scalar_udf = AsyncScalarUDF::new(udf);
            ctx.register_udf(scalar_udf.into());
        }

        ctx.sql(&parsed_query.sql).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_basic() {
    let query = r#"
CREATE FUNCTION add_one()
LANGUAGE python
AS '
def add_one(x: int) -> int:
    return x + 1
';

SELECT add_one(1);
"#;

    let ctx = SessionContext::new();
    let component = python_component().await;
    let formatter = Box::new(NoOpFormatter);

    let parser = UdfQueryParser::new(HashMap::from_iter([(
        "python".to_string(),
        Lang {
            component,
            formatter,
        },
    )]));
    let parsed_query = parser
        .parse(
            query,
            &WasmPermissions::new(),
            Handle::current(),
            ctx.task_ctx().as_ref(),
        )
        .await
        .unwrap();

    let df = UdfQueryInvocator::invoke(&ctx, parsed_query).await.unwrap();
    let batch = df.collect().await.unwrap();

    assert_batches_eq!(
        [
            "+-------------------+",
            "| add_one(Int64(1)) |",
            "+-------------------+",
            "| 2                 |",
            "+-------------------+",
        ],
        &batch
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_functions() {
    let query = r#"
CREATE FUNCTION add_one()
LANGUAGE python
AS '
def add_one(x: int) -> int:
    return x + 1
';

CREATE FUNCTION multiply_two()
LANGUAGE python
AS '
def multiply_two(x: int) -> int:
    return x * 2
';

SELECT add_one(1), multiply_two(3);
"#;

    let ctx = SessionContext::new();
    let component = python_component().await;
    let formatter = Box::new(NoOpFormatter);

    let parser = UdfQueryParser::new(HashMap::from_iter([(
        "python".to_string(),
        Lang {
            component,
            formatter,
        },
    )]));
    let parsed_query = parser
        .parse(
            query,
            &WasmPermissions::new(),
            Handle::current(),
            ctx.task_ctx().as_ref(),
        )
        .await
        .unwrap();

    let df = UdfQueryInvocator::invoke(&ctx, parsed_query).await.unwrap();
    let batch = df.collect().await.unwrap();

    assert_batches_eq!(
        [
            "+-------------------+------------------------+",
            "| add_one(Int64(1)) | multiply_two(Int64(3)) |",
            "+-------------------+------------------------+",
            "| 2                 | 6                      |",
            "+-------------------+------------------------+",
        ],
        &batch
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_functions_single_statement() {
    let query = r#"
CREATE FUNCTION add_one()
LANGUAGE python
AS '
def add_one(x: int) -> int:
    return x + 1

def multiply_two(x: int) -> int:
    return x * 2
';

SELECT add_one(1), multiply_two(3);
"#;

    let ctx = SessionContext::new();
    let component = python_component().await;
    let formatter = Box::new(NoOpFormatter);

    let parser = UdfQueryParser::new(HashMap::from_iter([(
        "python".to_string(),
        Lang {
            component,
            formatter,
        },
    )]));
    let parsed_query = parser
        .parse(
            query,
            &WasmPermissions::new(),
            Handle::current(),
            ctx.task_ctx().as_ref(),
        )
        .await
        .unwrap();

    let df = UdfQueryInvocator::invoke(&ctx, parsed_query).await.unwrap();
    let batch = df.collect().await.unwrap();

    assert_batches_eq!(
        [
            "+-------------------+------------------------+",
            "| add_one(Int64(1)) | multiply_two(Int64(3)) |",
            "+-------------------+------------------------+",
            "| 2                 | 6                      |",
            "+-------------------+------------------------+",
        ],
        &batch
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_string() {
    let query = r#"
CREATE FUNCTION add_one()
LANGUAGE python
AS '';

SELECT add_one(1)
"#;

    let ctx = SessionContext::new();
    let component = python_component().await;
    let formatter = Box::new(NoOpFormatter);

    let parser = UdfQueryParser::new(HashMap::from_iter([(
        "python".to_string(),
        Lang {
            component,
            formatter,
        },
    )]));
    let parsed_query = parser
        .parse(
            query,
            &WasmPermissions::new(),
            Handle::current(),
            ctx.task_ctx().as_ref(),
        )
        .await
        .unwrap();

    let r = UdfQueryInvocator::invoke(&ctx, parsed_query).await;
    assert!(r.is_err());

    let err = r.err().unwrap();
    assert!(err.message().contains("Invalid function 'add_one'"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explain() {
    let query = r#"
CREATE FUNCTION add_one()
LANGUAGE python
AS '
def add_one(x: int) -> int:
    return x + 1
';

EXPLAIN SELECT add_one(1);
"#;

    let ctx = SessionContext::new();
    let component = python_component().await;
    let formatter = Box::new(NoOpFormatter);

    let parser = UdfQueryParser::new(HashMap::from_iter([(
        "python".to_string(),
        Lang {
            component,
            formatter,
        },
    )]));
    let parsed_query = parser
        .parse(
            query,
            &WasmPermissions::new(),
            Handle::current(),
            ctx.task_ctx().as_ref(),
        )
        .await
        .unwrap();

    let df = UdfQueryInvocator::invoke(&ctx, parsed_query).await.unwrap();
    let batch = df.collect().await.unwrap();

    insta::assert_snapshot!(
        batches_to_string(&batch),
        @r"
    │++---------------+------------------------------------------------------------------------------+
    │+| plan_type     | plan                                                                         |
    │++---------------+------------------------------------------------------------------------------+
    │+| logical_plan  | Projection: add_one(Int64(1))                                                |
    │+|               |   EmptyRelation                                                              |
    │+| physical_plan | ProjectionExec: expr=[__async_fn_0@0 as add_one(Int64(1))]                   |
    │+|               |   AsyncFuncExec: async_expr=[async_expr(name=__async_fn_0, expr=add_one(1))] |
    │+|               |     CoalesceBatchesExec: target_batch_size=8192                              |
    │+|               |       PlaceholderRowExec                                                     |
    │+|               |                                                                              |
    │++---------------+------------------------------------------------------------------------------+
    ");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_strip_indentation_everything_indented() {
    let query_lines = &[
        "  CREATE FUNCTION add_one()",
        "  LANGUAGE python",
        "  AS '",
        "  def add_one(x: int) -> int:",
        "    ",
        "    return x + 1",
        "  ';",
        "  ",
        "  SELECT add_one(1);",
    ];
    let query = query_lines.join("\n");

    let ctx = SessionContext::new();
    let component = python_component().await;
    let formatter = Box::new(StripIndentationFormatter);

    let parser = UdfQueryParser::new(HashMap::from_iter([(
        "python".to_string(),
        Lang {
            component,
            formatter,
        },
    )]));
    let parsed_query = parser
        .parse(
            &query,
            &WasmPermissions::new(),
            Handle::current(),
            ctx.task_ctx().as_ref(),
        )
        .await
        .unwrap();

    let df = UdfQueryInvocator::invoke(&ctx, parsed_query).await.unwrap();
    let batch = df.collect().await.unwrap();

    assert_batches_eq!(
        [
            "+-------------------+",
            "| add_one(Int64(1)) |",
            "+-------------------+",
            "| 2                 |",
            "+-------------------+",
        ],
        &batch
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_strip_indentation_empty_lines_not_indented() {
    let query_lines = &[
        "  CREATE FUNCTION add_one()",
        "  LANGUAGE python",
        "  AS '",
        "  def add_one(x: int) -> int:",
        "",
        "    return x + 1",
        "  ';",
        "",
        "  SELECT add_one(1);",
    ];
    let query = query_lines.join("\n");

    let ctx = SessionContext::new();
    let component = python_component().await;
    let formatter = Box::new(StripIndentationFormatter);

    let parser = UdfQueryParser::new(HashMap::from_iter([(
        "python".to_string(),
        Lang {
            component,
            formatter,
        },
    )]));
    let parsed_query = parser
        .parse(
            &query,
            &WasmPermissions::new(),
            Handle::current(),
            ctx.task_ctx().as_ref(),
        )
        .await
        .unwrap();

    let df = UdfQueryInvocator::invoke(&ctx, parsed_query).await.unwrap();
    let batch = df.collect().await.unwrap();

    assert_batches_eq!(
        [
            "+-------------------+",
            "| add_one(Int64(1)) |",
            "+-------------------+",
            "| 2                 |",
            "+-------------------+",
        ],
        &batch
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_strip_indentation_python_further_indented() {
    let query_lines = &[
        "  CREATE FUNCTION add_one()",
        "  LANGUAGE python",
        "  AS '",
        "    def add_one(x: int) -> int:",
        "      return x + 1",
        "    ';",
        "  ",
        "  SELECT add_one(1);",
    ];
    let query = query_lines.join("\n");

    let ctx = SessionContext::new();
    let component = python_component().await;
    let formatter = Box::new(StripIndentationFormatter);

    let parser = UdfQueryParser::new(HashMap::from_iter([(
        "python".to_string(),
        Lang {
            component,
            formatter,
        },
    )]));
    let parsed_query = parser
        .parse(
            &query,
            &WasmPermissions::new(),
            Handle::current(),
            ctx.task_ctx().as_ref(),
        )
        .await
        .unwrap();

    let df = UdfQueryInvocator::invoke(&ctx, parsed_query).await.unwrap();
    let batch = df.collect().await.unwrap();

    assert_batches_eq!(
        [
            "+-------------------+",
            "| add_one(Int64(1)) |",
            "+-------------------+",
            "| 2                 |",
            "+-------------------+",
        ],
        &batch
    );
}
