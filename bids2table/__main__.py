"""Main entry point of bids2table."""

import argparse
import concurrent.futures
import glob
import sys
import warnings

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
        "Only applies when indexing a single dataset.  "
        "(Deprecated — use --filter sub=... instead.)",
    )
    parser_index.add_argument(
        "--filter",
        "-f",
        metavar="ENTITY=PATTERN",
        type=str,
        action="append",
        default=[],
        help="Filter files by entity key and value pattern. "
        "Syntax: ENTITY=PATTERN (e.g. --filter task=rest, --filter run=01). "
        "Repeat for multiple filters.",
    )
    parser_index.add_argument(
        "--schema",
        metavar="SCHEMA",
        type=str,
        default=None,
        help="Path to a directory containing BIDS schema YAML files or a YAML file. "
        "If not provided, uses the bundled default schema.",
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


def _parse_filters(filter_args: list[str]) -> dict[str, list[str]]:
    """Parse ``--filter`` arguments into a dict of key → [patterns].

    Each argument must be of the form ``KEY=PATTERN``. Malformed arguments
    are logged and skipped.
    """
    filters: dict[str, list[str]] = {}
    for arg in filter_args:
        if "=" not in arg:
            _logger.warning(
                "Skipping malformed filter argument: %r (expected ENTITY=PATTERN).", arg
            )
            continue
        key, _, pattern = arg.partition("=")
        filters.setdefault(key, []).append(pattern)
    return filters


def _normalize_filters(
    filters: dict[str, list[str]],
) -> dict[str, str | list[str]]:
    """Normalize a filter dict: single-item lists become bare strings."""
    return {k: v[0] if len(v) == 1 else v for k, v in filters.items()}


def _index_command(args: argparse.Namespace) -> None:
    """Handle the ``index`` subcommand: index one or more datasets to parquet."""
    filters = _parse_filters(args.filter)

    if args.subjects is not None:
        warnings.warn(
            "The --subjects flag is deprecated; use --filter sub=... instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        filters.setdefault("sub", []).extend(args.subjects)

    root = []
    for path in args.root:
        _check_path(path)
        if glob.has_magic(path):
            path = as_path(path)
            paths = list(path.parent.glob(path.name))
            root.extend(paths)
        else:
            root.append(path)

    if len(root) == 1:
        table = b2t2.index_dataset(
            root[0], filters=_normalize_filters(filters) or None, schema=args.schema
        )
        pq.write_table(table, args.output)
    else:
        if len(root) == 0 and not sys.stdin.isatty():
            root = (line.strip() for line in sys.stdin if line.strip())
        elif len(root) == 0:
            _logger.error("No datasets to index given; exiting.")
            sys.exit(1)

        max_workers = None if args.workers == -1 else args.workers
        if args.use_threads:
            executor_cls = concurrent.futures.ThreadPoolExecutor
        else:
            executor_cls = concurrent.futures.ProcessPoolExecutor

        schema = b2t2.get_arrow_schema(schema=args.schema)
        with pq.ParquetWriter(args.output, schema=schema) as writer:
            for table in b2t2.batch_index_dataset(
                list(root),
                max_workers=max_workers,
                executor_cls=executor_cls,
                filters=_normalize_filters(filters) or None,
                show_progress=not args.no_progress,
                schema=args.schema,
            ):
                writer.write_table(table)


def _find_command(args: argparse.Namespace) -> None:
    """Handle the ``find`` subcommand: log every BIDS dataset found under a root."""
    _check_path(args.root)

    for dataset in b2t2.find_bids_datasets(
        args.root,
        exclude=args.exclude_dirs,
        maxdepth=args.maxdepth,
    ):
        _logger.info(dataset)


def _check_path(path: str) -> None:
    """Exit early if ``path`` is a cloud path but ``cloudpathlib`` is unavailable."""
    if path.startswith(("s3://", "gs://")) and not b2t2.cloudpathlib_is_available():
        _logger.error(
            "Cloudpathlib is required to use cloud paths. "
            "Install with e.g. `pip install cloudpathlib[cloud]`."
        )
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
