"""Main entry point of bids2table."""

import argparse
import concurrent.futures
import glob
import sys

import pyarrow.parquet as pq

import bids2table as b2t2
from bids2table._logging import setup_logger
from bids2table._pathlib import as_path

_logger = setup_logger(__package__)


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Find and index BIDS datasets.")
    subparsers = parser.add_subparsers(dest="subcommand")

    parser_index = subparsers.add_parser("index", help="Index BIDS dataset(s).")
    parser_index.add_argument(
        "--output",
        "-o",
        type=str,
        required=True,
        help="Path to output parquet file.",
    )
    parser_index.add_argument(
        "--subjects",
        metavar="SUB",
        type=str,
        nargs="+",
        default=None,
        help="List of subject names or glob patterns to only include in the index.  "
        "Only applies when indexing a single dataset.",
    )
    parser_index.add_argument(
        "--workers",
        "-j",
        type=int,
        help="Number of worker processes for dataset-level parallelism. Setting to -1 "
        "runs as many workers as there are cores available. Setting to 0 runs in the "
        "main process. (default: %(default)d)",
        default=0,
    )
    parser_index.add_argument(
        "--use-threads",
        action="store_true",
        help="Use threads instead of processes when workers > 0 (dataset-level "
        "parallelism only).",
    )
    parser_index.add_argument(
        "--no-progress", "-q", action="store_true", help="Disable the progress bar."
    )
    parser_index.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase logging. -v enables warnings. -vv enables even more logging.",
    )
    parser_index.add_argument(
        "root",
        metavar="ROOT",
        type=str,
        nargs="*",
        help="BIDS dataset(s) to index. Can be full paths or glob patterns. "
        "If no datasets provided, will attempt to read from stdin.",
    )
    parser_index.set_defaults(func=_index_command)

    parser_find = subparsers.add_parser("find", help="Find BIDS datasets.")
    parser_find.add_argument(
        "--maxdepth", type=int, help="Max search depth", default=None
    )
    parser_find.add_argument(
        "--exclude-dirs",
        metavar="DIR",
        type=str,
        nargs="+",
        default=None,
        help="List of directory names or glob patterns to exclude from search.",
    )
    parser_find.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase logging. -v enables warnings. -vv enables even more logging.",
    )
    parser_find.add_argument(
        "root", metavar="ROOT", type=str, help="Root directory to search."
    )
    parser_find.set_defaults(func=_find_command)

    args = parser.parse_args()

    if hasattr(args, "func"):
        log_level = ["ERROR", "WARNING", "INFO"][min(args.verbose, 2)]
        _logger.setLevel(log_level)

        args.func(args)
    else:
        parser.print_help()


def _index_command(args: argparse.Namespace) -> None:
    for path in args.root:
        _check_path(path)

    root = []
    for path in args.root:
        if glob.has_magic(path):
            path = as_path(path)
            paths = list(path.parent.glob(path.name))
            root.extend(paths)
        else:
            root.append(path)

    if len(root) == 1:
        table = b2t2.index_dataset(root[0], include_subjects=args.subjects)
        pq.write_table(table, args.output)
    else:
        # Logic to hand in piped in datasets / no datasets
        if len(root) == 0 and not sys.stdin.isatty():
            # read datasets from stdin, one per line
            root = (line.strip() for line in sys.stdin if line.strip())
        elif len(root) == 0:
            _logger.error("No datasets to index given; exiting.")
            sys.exit(1)

        # Set up for parallelism
        max_workers = None if args.workers == -1 else args.workers
        if args.use_threads:
            executor_cls = concurrent.futures.ThreadPoolExecutor
        else:
            executor_cls = concurrent.futures.ProcessPoolExecutor

        schema = b2t2.get_arrow_schema()
        with pq.ParquetWriter(args.output, schema=schema) as writer:
            for table in b2t2.batch_index_dataset(
                list(root),
                max_workers=max_workers,
                executor_cls=executor_cls,
                show_progress=not args.no_progress,
            ):
                writer.write_table(table)


def _find_command(args: argparse.Namespace) -> None:
    _check_path(args.root)

    for dataset in b2t2.find_bids_datasets(
        args.root,
        exclude=args.exclude_dirs,
        maxdepth=args.maxdepth,
    ):
        _logger.info(dataset)


def _check_path(path: str) -> None:
    if path.startswith(("s3://", "gs://")) and not b2t2.cloudpathlib_is_available():
        _logger.error(
            "Cloudpathlib is required to use cloud paths. "
            "Install with e.g. `pip install cloudpathlib[cloud]`."
        )
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
