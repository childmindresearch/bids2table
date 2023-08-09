"""
A script to benchmark different methods for indexing BIDS datasets.
"""

import argparse
import json
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

try:
    import bids

    has_pybids = True
except ModuleNotFoundError:
    has_pybids = False

try:
    import ancpbids

    has_ancpbids = True
except ModuleNotFoundError:
    has_ancpbids = False

try:
    import bids2table as b2t

    has_bids2table = True
except ModuleNotFoundError:
    has_bids2table = False

METHODS = ("pybids", "ancpbids", "bids2table")


def du(path):
    size_kb = subprocess.check_output(["du", "-sk", path])
    size_kb = int(size_kb.split()[0].decode("utf-8"))
    size_mb = int(size_kb) / 1000
    return size_mb


def benchmark_pybids(root: Path, workers: int):
    assert has_pybids, "pybids not installed"
    assert workers == 1, "pybids doesn't use multiple workers"
    tic = time.monotonic()

    with tempfile.TemporaryDirectory() as tmpdir:
        indexer = bids.BIDSLayoutIndexer(
            validate=False,
            index_metadata=True,
        )
        bids.BIDSLayout(
            root=root,
            validate=False,
            absolute_paths=True,
            derivatives=True,
            database_path=Path(tmpdir) / "index.db",
            indexer=indexer,
        )
        size_mb = du(tmpdir)

    elapsed = time.monotonic() - tic

    result = {
        "version": bids.__version__,
        "elapsed": elapsed,
        "size_mb": size_mb,
    }
    return result


def benchmark_ancpbids(root: Path, workers: int):
    assert has_ancpbids, "ancpbids not installed"
    assert workers == 1, "ancpbids doesn't use multiple workers"
    tic = time.monotonic()

    ancpbids.load_dataset(root)
    # ancpbids is purely in-memory
    size_mb = float("nan")

    elapsed = time.monotonic() - tic

    result = {
        "version": ancpbids.__version__,
        "elapsed": elapsed,
        "size_mb": size_mb,
    }
    return result


def benchmark_bids2table(root: Path, workers: int):
    assert has_bids2table, "bids2table not installed"
    tic = time.monotonic()

    with tempfile.TemporaryDirectory() as tmpdir:
        b2t.bids2table(
            root,
            persistent=True,
            output=Path(tmpdir) / "index.b2t",
            workers=workers,
            return_df=False,
        )
        size_mb = du(tmpdir)

    elapsed = time.monotonic() - tic
    result = {
        "version": b2t.__version__,
        "elapsed": elapsed,
        "size_mb": size_mb,
    }
    return result


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "method",
        metavar="METHOD",
        type=str,
        choices=METHODS,
        help="Indexing method",
    )
    parser.add_argument(
        "root",
        metavar="ROOT",
        type=Path,
        help="Path to BIDS dataset",
    )
    parser.add_argument(
        "out",
        metavar="OUTPUT",
        type=Path,
        help="Path to JSON output",
    )
    parser.add_argument(
        "--workers",
        "-w",
        metavar="COUNT",
        type=int,
        help="Number of worker processes",
        default=1,
    )

    args = parser.parse_args()

    if args.method == "pybids":
        result = benchmark_pybids(args.root, args.workers)
    if args.method == "ancpbids":
        result = benchmark_ancpbids(args.root, args.workers)
    elif args.method == "bids2table":
        result = benchmark_bids2table(args.root, args.workers)
    else:
        raise NotImplementedError(f"Indexing method {args.method} not implemented")

    result = {
        "bids_dir": str(args.root.absolute()),
        "method": args.method,
        "workers": args.workers,
        "py_version": sys.version,
        "hostname": socket.gethostname(),
        **result,
    }

    with open(args.out, "w") as f:
        print(json.dumps(result), file=f)


if __name__ == "__main__":
    main()
