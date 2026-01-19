//! This benchmark should assess how expensive our Python wrapper is.
//!
//! In contrast to the `udf_overhead` benchmark in `datafusion-udf-wasm-host`, this benchmark can be executed natively
//! outside the WASM sandbox. This allows your to use certain profiling tools like [`samply`] more easily.
//!
//!
//! [`samply`]: https://github.com/mstange/samply
#![expect(
    // Docs are not strictly required for tests.
    clippy::missing_docs_in_private_items,
    // unused-crate-dependencies false positives
    unused_crate_dependencies,
)]

use std::{hint::black_box, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field},
};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use gungraun::{LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main};

struct Setup {
    udf: Arc<dyn ScalarUDFImpl>,
    batch_args: Vec<ScalarFunctionArgs>,
}

#[expect(dead_code)]
struct SetupLeftovers {
    udf: Arc<dyn ScalarUDFImpl>,
}

impl Setup {
    fn new(batch_size: usize, num_batches: usize) -> Self {
        let mut config_options = ConfigOptions::default();
        config_options.execution.batch_size = batch_size;
        let config_options = Arc::new(config_options);

        let udf = datafusion_udf_wasm_python::udfs(
            "
def add_one(a: int) -> int:
    return a + 1
        "
            .trim()
            .to_owned(),
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let mut input_gen = (0..).map(|x| (x % 2 == 0).then_some(x as i64));
        let arg_field = Arc::new(Field::new("a", DataType::Int64, true));
        let return_field = Arc::new(Field::new("r", DataType::Int64, true));
        let batch_args = (0..num_batches)
            .map(|_| ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter(
                    (&mut input_gen).take(batch_size),
                )))],
                arg_fields: vec![Arc::clone(&arg_field)],
                number_rows: batch_size,
                return_field: Arc::clone(&return_field),
                config_options: Arc::clone(&config_options),
            })
            .collect();

        Self { udf, batch_args }
    }

    fn run(self) -> SetupLeftovers {
        let Self { udf, batch_args } = self;

        #[expect(clippy::unit_arg)]
        black_box({
            for args in batch_args {
                udf.invoke_with_args(args).unwrap();
            }
        });

        SetupLeftovers { udf }
    }
}

mod actual_benchmark {
    use gungraun::{Callgrind, FlamegraphConfig};

    use super::*;

    #[library_benchmark(setup = Setup::new, teardown=drop)]
    #[bench::batchsize_24576_batches_3(24576, 3)]
    fn bench_python(setup: Setup) -> SetupLeftovers {
        setup.run()
    }

    library_benchmark_group!(
        name = add_one;
        compare_by_id = true;
        benchmarks = bench_python
    );

    main!(
        config = LibraryBenchmarkConfig::default()
                .tool(Callgrind::default()
                    .flamegraph(FlamegraphConfig::default())
                );
        library_benchmark_groups = add_one
    );

    // re-export `main`
    pub(crate) fn pub_main() {
        main();
    }
}

fn main() {
    // Running the actual benchmark under Valgrind is rather expensive, esp. in CI. Hence we just smoke tests.
    if std::env::args().into_iter().any(|arg| arg == "--test") {
        let batch_size = 1_000_000;
        let num_batches = 1;

        Setup::new(batch_size, num_batches).run();
    } else {
        actual_benchmark::pub_main();
    }
}
