//! Judge the overhead of the WASM & Python layers, compared to a native implementation.
//!
//! # Implementations
//! The following three implementations are tested:
//!
//! ## Native
//! Uses an ordinary [`AsyncScalarUDFImpl`] implemented using ordinary Rust, similar to how most DataFusion API
//! users[^api_users] would implement that trait.
//!
//! ## WASM
//! The same as [native](#native), but the UDF is compiled to WebAssembly and called through our UDF-WASM bridge. This
//! measures the raw overhead of running things as WASM + passing data back and forth from the host into the guest.
//!
//! ## Python
//! Our end-user interface that bundles CPython as a WebAssembly guest. This uses the same bridge as the [WASM](#wasm)
//! variant. It is compared to both [WASM](#wasm) to measure the additional overhead of the Python wrapper and Python
//! execution; as well as to [native](#native) to measure the end-to-end overhead of this solution.
//!
//! # What is Measured
//! We measure the cost (e.g. in cycles, cache MISS-es, RAM accesses) of calling
//! [`AsyncScalarUDFImpl::invoke_async_with_args`]. The creation of the [`AsyncScalarUDFImpl`] object is NOT measured,
//! since we assume that these cost are amortized in larger queries.
//!
//! # Benchmark Scaling Axis
//! There are two scaling axes to the benchmarks:
//!
//! ## Batch Size
//! Increasing DataFusion batch size. This is done to estimate the per-call overhead of
//! [`AsyncScalarUDFImpl::invoke_async_with_args`] and the per-row overhead. Assuming that the growth is approximately
//! linear, then we have per batch:
//!
//!     cost =
//!               cost_row * n_rows
//!            + cost_call
//!
//! or
//!
//!     cost =
//!               overhead_row *  cost_native_row * n_rows
//!            + overhead_call * cost_native_call
//!
//! ## Number of Batches
//! Increasing number of equally sized batches and -- apart from [`ScalarFunctionArgs::args`] -- identical arguments.
//! This is done to estimate if there is some form of caching effect, i.e. if calling the same UDF multiple times as
//! part of `AsyncFuncExec` would have some amortizing cost.
//!
//! This modifies the linear equation from [above](#batch-size) as follows, and is now measured for all batches:
//!
//!     cost =
//!              (
//!                   cost_row * n_rows
//!                + cost_call
//!              ) * n_batches
//!            + cost_cached
//!
//! or
//!
//!     cost =
//!              (
//!                   overhead_row *  cost_native_row * n_rows
//!                + overhead_call * cost_native_call
//!              ) * n_batches
//!            + overhead_cached
//!
//! Note that there is no `cost_native_cached` since the native implementation has no real caching effect. There may
//! only be some noise measured due to entering/exiting the tokio runtime, but we shall ignore that.
//!
//!
//! [^api_users]: One should differentiate between DataFusion "users". On one hand there are "API users", i.e.
//!               products that use the DataFusion library to build an end-user product. And then there are
//!               "end-users", e.g. customers that use products that are built on top of DataFusion.
#![expect(
    // Docs are not strictly required for tests.
    clippy::missing_docs_in_private_items,
    // unused-crate-dependencies false positives
    unused_crate_dependencies,
)]

use std::{hint::black_box, io::Write, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field},
};
use datafusion_common::{Result as DataFusionResult, cast::as_int64_array, config::ConfigOptions};
use datafusion_execution::memory_pool::UnboundedMemoryPool;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    async_udf::AsyncScalarUDFImpl,
};
use datafusion_udf_wasm_host::{WasmComponentPrecompiled, WasmScalarUdf};
use gungraun::{LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main};
use tokio::runtime::{Handle, Runtime};
use wasmtime_wasi::async_trait;

/// UDF that implements "add one".
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct AddOne {
    /// Signature of the UDF.
    ///
    /// We store this here because [`ScalarUDFImpl::signature`] requires us to return a reference.
    signature: Signature,
}

impl Default for AddOne {
    fn default() -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Int64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AddOne {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "add_one"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        unimplemented!()
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for AddOne {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> DataFusionResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            arg_fields: _,
            number_rows: _,
            return_field: _,
            config_options: _,
        } = args;

        let ColumnarValue::Array(array) = &args[0] else {
            unreachable!()
        };
        let array = as_int64_array(array)?;

        // perform calculation
        let array = array
            .iter()
            .map(|x| x.and_then(|x| x.checked_add(1)))
            .collect::<Int64Array>();

        // create output
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

/// Compile the WASM component outside of Valgrind, because otherwise the setup step takes like 3+min per benchmark.
fn build_wasm_module(binary: &[u8]) -> WasmComponentPrecompiled {
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_compile"))
        .arg("/dev/stdin")
        .arg("/dev/stdout")
        // Specify the target that is the same as the host.
        //
        // If we don't specify the target, this will compile for "native CPU", not for a generic CPU. And "native CPU"
        // may include AVX&Co instructions that Valgrind doesn't support.
        //
        // Also in production people are likely gonna use the generic CPU pre-compiled binaries, so I feel that's a
        // fairer comparison.
        .arg(target_lexicon::HOST.to_string())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();

    // Technically you also have to poll stdout/stderr while you feed data into stdin, otherwise we may deadlock.
    // However, the `compile` binary isn't streaming and stderr is hooked up to `/dev/null`, so we can feed the entire
    // data in before we read the output back. Just don't forget to close stdin (via `drop`) to send EOF.
    let mut stdin = child.stdin.take().expect("Failed to open stdin");
    stdin.write_all(binary).expect("Failed to write to stdin");
    stdin.flush().expect("Flush stdin");
    drop(stdin);

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());

    let elf = output.stdout;
    // SAFETY: we just compiled this data ourselves
    let res = unsafe { WasmComponentPrecompiled::load(elf) };
    res.unwrap()
}

enum Mode {
    Native,
    Wasm,
    Python,
}

struct Setup {
    udf: Arc<dyn AsyncScalarUDFImpl>,
    batch_args: Vec<ScalarFunctionArgs>,
    rt: Runtime,
}

#[expect(dead_code)]
struct SetupLeftovers {
    udf: Arc<dyn AsyncScalarUDFImpl>,
    rt: Runtime,
}

impl Setup {
    fn new(mode: Mode, batch_size: usize, num_batches: usize) -> Self {
        let mut config_options = ConfigOptions::default();
        config_options.execution.batch_size = batch_size;
        let config_options = Arc::new(config_options);

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let udf = match mode {
            Mode::Native => Arc::new(AddOne::default()) as Arc<dyn AsyncScalarUDFImpl>,
            Mode::Wasm => {
                let udf = rt.block_on(async {
                    let component =
                        build_wasm_module(datafusion_udf_wasm_bundle::BIN_EXAMPLE_ADD_ONE);

                    WasmScalarUdf::new(
                        &component,
                        &Default::default(),
                        Handle::current(),
                        &(Arc::new(UnboundedMemoryPool::default()) as _),
                        "".to_owned(),
                    )
                    .await
                    .unwrap()
                    .into_iter()
                    .next()
                    .unwrap()
                });
                Arc::new(udf)
            }
            Mode::Python => {
                let udf = rt.block_on(async {
                    let component = build_wasm_module(datafusion_udf_wasm_bundle::BIN_PYTHON);

                    WasmScalarUdf::new(
                        &component,
                        &Default::default(),
                        Handle::current(),
                        &(Arc::new(UnboundedMemoryPool::default()) as _),
                        "
def add_one(a: int) -> int:
    return a + 1
"
                        .to_owned(),
                    )
                    .await
                    .unwrap()
                    .into_iter()
                    .next()
                    .unwrap()
                });
                Arc::new(udf)
            }
        };

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

        Self {
            udf,
            batch_args,
            rt,
        }
    }

    fn run(self) -> SetupLeftovers {
        let Self {
            udf,
            batch_args,
            rt,
        } = self;

        #[expect(clippy::unit_arg)]
        black_box(rt.block_on(async {
            for args in batch_args {
                udf.invoke_async_with_args(args).await.unwrap();
            }
        }));

        SetupLeftovers { udf, rt }
    }
}

mod actual_benchmark {
    use super::*;

    /// Instantiate benchmarks for given mode.
    macro_rules! impl_benchmark {
        ($mode:ident, $bench_name:ident) => {
            #[library_benchmark(setup = Setup::new, teardown=drop)]
            #[bench::batchsize_0_batches_0(Mode::$mode, 0, 0)]
            #[bench::batchsize_0_batches_1(Mode::$mode, 0, 1)]
            #[bench::batchsize_0_batches_2(Mode::$mode, 0, 2)]
            #[bench::batchsize_0_batches_3(Mode::$mode, 0, 3)]
            #[bench::batchsize_8192_batches_0(Mode::$mode, 8192, 0)]
            #[bench::batchsize_8192_batches_1(Mode::$mode, 8192, 1)]
            #[bench::batchsize_8192_batches_2(Mode::$mode, 8192, 2)]
            #[bench::batchsize_8192_batches_3(Mode::$mode, 8192, 3)]
            #[bench::batchsize_16384_batches_0(Mode::$mode, 16384, 0)]
            #[bench::batchsize_16384_batches_1(Mode::$mode, 16384, 1)]
            #[bench::batchsize_16384_batches_2(Mode::$mode, 16384, 2)]
            #[bench::batchsize_16384_batches_3(Mode::$mode, 16384, 3)]
            #[bench::batchsize_24576_batches_0(Mode::$mode, 24576, 0)]
            #[bench::batchsize_24576_batches_1(Mode::$mode, 24576, 1)]
            #[bench::batchsize_24576_batches_2(Mode::$mode, 24576, 2)]
            #[bench::batchsize_24576_batches_3(Mode::$mode, 24576, 3)]
            fn $bench_name(setup: Setup) -> SetupLeftovers {
                setup.run()
            }
        };
    }

    impl_benchmark!(Native, bench_native);
    impl_benchmark!(Wasm, bench_wasm);
    impl_benchmark!(Python, bench_python);

    library_benchmark_group!(
        name = add_one;
        compare_by_id = true;
        benchmarks = bench_native, bench_wasm, bench_python
    );

    main!(
        config = LibraryBenchmarkConfig::default()
            .valgrind_args([
                // Ensure that `build_wasm_module` runs outside of valgrind for performance reasons. Otherwise, it can
                // easily take 3+min per benchmark to complete.
                "--trace-children=no",
            ]);
        ;
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
        let batch_size = 2;
        let num_batches = 1;

        println!("native");
        Setup::new(Mode::Native, batch_size, num_batches).run();

        println!("wasm");
        Setup::new(Mode::Wasm, batch_size, num_batches).run();

        println!("python");
        Setup::new(Mode::Python, batch_size, num_batches).run();
    } else {
        actual_benchmark::pub_main();
    }
}
