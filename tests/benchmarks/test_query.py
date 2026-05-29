"""Querying benchmarks."""

import datetime
from pathlib import Path
from typing import Callable

import polars as pl
import pytest
from pytest_benchmark.fixture import BenchmarkFixture

import bids2table as b2t2

SUBJECTS = ["01", "10"]
NUM_VOLS = 184
TARGET_TE = 0.00875
TARGET_TIME = datetime.time(10).strftime("%H:%M:%S.%f")


def _run_benchmark(
    benchmark: BenchmarkFixture,
    func: Callable,
    version: str,
    *args,
    **kwargs,
) -> None:
    benchmark.pedantic(func, args=args, kwargs=kwargs, iterations=1, rounds=11)
    benchmark.extra_info.update({"version": version or "Unknown"})


@pytest.mark.benchmark
class TestB2TQuery:
    """Benchmark different b2t queries."""

    @pytest.fixture
    def index(self) -> tuple:
        """Index dataset with b2t."""
        data_dir = Path("bids-examples/ds000117")
        table = b2t2.index_dataset(data_dir, show_progress=False)
        df = pl.from_arrow(table)
        df = df.with_columns(
            pl.format("{}/{}", pl.col("root"), pl.col("path")).alias("fpath")
        )
        df = df.with_columns(
            pl.col("fpath")
            .map_elements(b2t2.load_bids_metadata, return_dtype=pl.Object)
            .alias("json")
        )
        version = b2t2.__version__
        return df, version

    def test_subject_query(self, benchmark: BenchmarkFixture, index: tuple) -> None:
        """Benchmark subject queries."""
        table, version = index

        def query() -> None:
            table.get_column("sub").unique()

        _run_benchmark(benchmark, query, version=version)

    def test_bold_query(self, benchmark: BenchmarkFixture, index: tuple) -> None:
        """Benchmark queries for bold images."""
        table, version = index
        table = table.with_columns(
            [pl.col("ext").cast(pl.Categorical), pl.col("suffix").cast(pl.Categorical)]
        )

        def query() -> None:
            table.select(["ext", "suffix", "fpath"]).filter(
                (pl.col("ext") == ".nii.gz") & (pl.col("suffix") == "bold")
            ).get_column("fpath")

        _run_benchmark(benchmark, query, version=version)

    def test_metadata_query(self, benchmark: BenchmarkFixture, index: tuple) -> None:
        """Benchmark query via metadata."""
        table, version = index
        table = table.with_columns(
            pl.col("json")
            .map_elements(lambda x: x.get("EchoTime"), return_dtype=pl.Float64)
            .alias("echo_time")
        )

        def query() -> None:
            table.select(["sub", "echo_time", "fpath"]).filter(
                (pl.col("sub").is_in(SUBJECTS)) & (pl.col("echo_time") == TARGET_TE)
            ).get_column("fpath")

        _run_benchmark(benchmark, query, version=version)
