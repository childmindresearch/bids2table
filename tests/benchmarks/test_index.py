"""Indexing benchmarks."""

import os
import shutil
from pathlib import Path
from typing import Callable

import pyarrow.parquet as pq
import pytest
from pytest_benchmark.fixture import BenchmarkFixture

import bids2table as b2t2


def du(path: Path) -> float:
    """Compute directory size in mb."""
    total = 0
    stack = [path]
    while stack:
        for entry in os.scandir(stack.pop()):
            try:
                st = entry.stat(follow_symlinks=False)
                if entry.is_dir(follow_symlinks=False):
                    stack.append(Path(entry.path))
                else:
                    total += st.st_size
            except OSError:
                continue
    return total / 1_024**2


def _run_benchmark(
    benchmark: BenchmarkFixture,
    func: Callable,
    index_fpath: Path,
    version: str,
    workers: int,
    *args,
    **kwargs,
) -> None:
    sizes = []

    def _teardown(index_fpath: Path):
        size = du(index_fpath.parent)
        sizes.append(size)
        if index_fpath.exists():
            shutil.rmtree(index_fpath.parent)

    # Benchmark
    benchmark.pedantic(
        func,
        teardown=_teardown(index_fpath=index_fpath),
        args=args,
        kwargs=kwargs,
        iterations=1,
        rounds=11,  # Include an additional round for warmup
    )

    # Additional info
    benchmark.extra_info.update(
        {
            "size_mb": sizes,
            "version": version or "Unknown",
            "workers": workers or "Unknown",
        }
    )


@pytest.mark.benchmark
@pytest.mark.cloud
def test_openneuro(benchmark: BenchmarkFixture, tmp_path: Path) -> None:
    """Benchmark b2t2 with a subset of datasets on OpenNeuro."""
    workers = 4
    index_fpath = tmp_path / "index.parquet"

    def index() -> None:
        path = b2t2._pathlib.as_path("s3://openneuro.org/ds002*")
        paths = list(path.parent.glob(path.name))
        schema = b2t2.get_arrow_schema()
        assert len(paths) > 1, "1 or less datasets found...check the path provided"
        with pq.ParquetWriter(index_fpath, schema) as writer:
            for table in b2t2.batch_index_dataset(
                paths,  # type: ignore[arg-type]
                max_workers=workers,
                show_progress=False,
            ):
                writer.write_table(table)

    _run_benchmark(
        benchmark,
        index,
        index_fpath=index_fpath,
        version=b2t2.__version__,
        workers=workers,
    )


@pytest.mark.benchmark
@pytest.mark.parametrize("workers", (1, 4))
def test_local(benchmark: BenchmarkFixture, tmp_path: Path, workers: int) -> None:
    """Bids2Table v2 benchmarking on local dataset."""
    index_fpath = tmp_path / "index.parquet"
    data_dir = Path("bids-examples/ds000117")

    def index() -> None:
        table = b2t2.index_dataset(data_dir, max_workers=workers, show_progress=False)
        pq.write_table(table, index_fpath)

    _run_benchmark(
        benchmark,
        index,
        index_fpath=index_fpath,
        version=b2t2.__version__,
        workers=workers,
    )
