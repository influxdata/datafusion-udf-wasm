import argparse
import json
import re

from enum import Enum
from typing import Callable

from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

import numpy as np
import polars as pl


class Metric(Enum):
    """
    Valgrind metric that the comparison is based on.

    This uses human-readable names.
    """

    cycles = "cycles"
    instructions = "instructions"

    def __str__(self) -> str:
        return self.value

    def valgrind_name(self) -> str:
        """
        Valgrind-specific name for the metric.
        """
        match self:
            case Metric.cycles:
                return "EstimatedCycles"
            case Metric.instructions:
                return "Ir"
            case _:
                raise ValueError(f"invalid metric: {self}")


def parse_data(file: str, metric: Metric) -> pl.DataFrame:
    """
    Parse data from JSON file into a DataFrame.
    """
    fn_re = re.compile("bench_(?P<mode>[a-z]+)")
    id_re = re.compile("batchsize_(?P<batchsize>[0-9]+)_batches_(?P<batches>[0-9]+)")

    valgrind_metric = metric.valgrind_name()

    mode = []
    batchsize = []
    batches = []
    cost = []

    with open(file) as fp:
        for line in fp:
            data = json.loads(line)

            fn_parsed = fn_re.match(data["function_name"])
            assert fn_parsed
            mode.append(fn_parsed.group("mode"))

            id_parsed = id_re.match(data["id"])
            assert id_parsed
            batchsize.append(int(id_parsed.group("batchsize")))
            batches.append(int(id_parsed.group("batches")))

            [profile] = data["profiles"]
            cost.append(
                profile["summaries"]["total"]["summary"]["Callgrind"][valgrind_metric][
                    "metrics"
                ]["Left"]["Int"]
            )

    return pl.DataFrame(
        {"mode": mode, "batchsize": batchsize, "batches": batches, "cost": cost}
    )


def regression(df: pl.DataFrame) -> pl.DataFrame:
    """
    Run regression model on the DataFrame.
    """
    #  y = (c_row * batchsize + c_call) * batches + c_cached
    #    = c_row * batchsize * n_batches + c_call * batches + c_cached
    #    = c_row * totalrows + c_call * batches + c_cached
    df = df.with_columns((pl.col("batchsize") * pl.col("batches")).alias("totalrows"))

    mode = []
    cost_row = []
    cost_call = []
    cost_cached = []
    score = []

    for m in sorted(df["mode"].unique()):
        df_sub = df.filter(pl.col("mode") == m)
        X = df_sub.select("totalrows", "batches").to_numpy()
        y = df_sub["cost"].to_numpy()

        # Add a "fake" parameter (a constant column of ones) so we can treat the
        # intercept as just another coefficient. scikit-learn's LinearRegression
        # only enforces the positive=True constraint on coefficients, not on the
        # intercept, so we disable fit_intercept and instead learn this constant
        # term as a regular coefficient, then later scale it back and interpret it
        # as the intercept.
        X = np.hstack([X, np.ones((X.shape[0], 1))])

        # input dimensions have different scales
        scaler = StandardScaler(with_mean=False)
        X = scaler.fit_transform(X)

        reg = LinearRegression(positive=True, fit_intercept=False).fit(X, y)
        assert reg.intercept_ == 0

        # map coefficients and intercept back through the scaler
        coef = reg.coef_ / scaler.scale_

        # disentangle our fake parameter/intercept
        actual_coef = coef[:-1]
        actual_intercept = coef[-1]

        mode.append(m)
        cost_row.append(int(actual_coef[0]))
        cost_call.append(int(actual_coef[1]))
        cost_cached.append(int(actual_intercept))

        score.append(reg.score(X, y))

    return pl.DataFrame(
        {
            "mode": mode,
            "cost_row": cost_row,
            "cost_call": cost_call,
            "cost_cached": cost_cached,
            "score": score,
        }
    ).sort("cost_row")


def calc_delta(df: pl.DataFrame, op: Callable[[int, int], int | float]) -> pl.DataFrame:
    """
    Calculate delta between the regression models of the different modes.
    """
    baseline = []
    mode = []
    deltas = {col: [] for col in df.columns if col.startswith("cost_")}

    modes = df["mode"].unique(maintain_order=True)
    for m2 in modes:
        row2 = df.filter(pl.col("mode") == m2).row(0, named=True)
        for m1 in modes:
            row1 = df.filter(pl.col("mode") == m1).row(0, named=True)
            if row1["cost_row"] >= row2["cost_row"]:
                continue

            baseline.append(m1)
            mode.append(m2)
            for col, array in deltas.items():
                array.append(op(row1[col], row2[col]))

    out = {"mode": mode, "baseline": baseline}
    out.update(deltas)
    return pl.DataFrame(out)


def print_df(df: pl.DataFrame, title: str, descr: str | None = None) -> None:
    """
    Print DataFrame to stdout.
    """
    print(f"=== {title} ===")
    if descr:
        print(descr.strip())
    with pl.Config(
        float_precision=2,
        fmt_float="full",
        tbl_cell_numeric_alignment="RIGHT",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
        tbl_rows=100,
        thousands_separator="_",
    ):
        print(df)
    print()
    print()


def main() -> None:
    """
    Main entry point.
    """
    parser = argparse.ArgumentParser(
        description="""
Analyze data of the `udf_overhead` benchmark.

Data Collection
===============
Run the following command to generate a profile JSON file:

    $ cargo bench \\
        --features=all-arch --package=datafusion-udf-wasm-host --bench=udf_overhead \\
        -- --output-format=json --baseline=doesnotexist \\
        > perf.json

Note that we deliberately specify a non-existing baseline here
because otherwise the JSON file will look different and we
won't be able to parse that.
""",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--profile", help="profile JSON file", type=str, required=True)
    parser.add_argument(
        "--metric",
        help="valgrind metric to use as 'cost'",
        type=Metric,
        default=Metric.cycles,
        choices=list(Metric),
    )
    args = parser.parse_args()

    df = parse_data(args.profile, args.metric)
    print_df(df=df, title="input")

    df = regression(df)
    print_df(
        df=df,
        title="cost model",
        descr="""
y = cost_row * n_rows + cost_call * n_batches + cost_cached

   cost_row: Cost per input row/cell.
  cost_call: Cost per UDF call / record batch.
cost_cached: Cost amortized by calling the UDF multiple times.
      score: How well the linear model fits (0=not, 1=perfect).
""",
    )

    print_df(
        df=calc_delta(df, lambda a, b: b - a), title="delta abs", descr="delta = y - x"
    )
    print_df(
        df=calc_delta(df, lambda a, b: (float(b) - float(a)) / max(1, float(a))),
        title="delta rel1",
        descr="delta = (y - x) / max(x, 1)",
    )
    print_df(
        df=calc_delta(df, lambda a, b: float(b) / max(1, float(a))),
        title="delta rel2",
        descr="delta = y / max(x, 1)",
    )
