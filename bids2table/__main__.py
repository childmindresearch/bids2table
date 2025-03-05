import argparse
import sys
from pathlib import Path

from bids2table import bids2table
from bids2table.logging import setup_logging


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("root", metavar="ROOT", type=Path, help="Path to BIDS dataset")
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Path to output parquet dataset directory (default: {ROOT}/index.b2t)",
        default=None,
    )
    parser.add_argument(
        "--incremental",
        "--inc",
        help="Update index incrementally with only new or changed files.",
        action="store_true",
    )
    parser.add_argument(
        "--overwrite",
        "-x",
        help="Overwrite previous index.",
        action="store_true",
    )
    parser.add_argument(
        "--workers",
        "-w",
        metavar="COUNT",
        type=int,
        help="Number of worker processes. Setting to -1 runs as many processes as "
        "there are cores available. (default: 1)",
        default=1,
    )
    parser.add_argument(
        "--worker_id",
        "--id",
        metavar="RANK",
        type=int,
        help="Optional worker ID to use when scheduling parallel tasks externally. "
        "Incompatible with --overwrite. (default: None)",
        default=None,
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase verbosity level.",
    )
    parser.add_argument(
        "--exclude",
        nargs="+",
        default=None,
        help="List of directory names or glob patterns to exclude from indexing.",
    )
    parser.add_argument(
        "--subject",
        "-sub",
        nargs="+",
        type=str,
        help="List of subject labels to index (default: None)",
        default=None,
    )
    args = parser.parse_args()

    log_level = ["ERROR", "WARNING", "INFO"][min(args.verbose, 2)]
    setup_logging(level=log_level)

    bids2table(
        root=args.root,
        persistent=True,
        index_path=args.output,
        incremental=args.incremental,
        overwrite=args.overwrite,
        workers=args.workers,
        worker_id=args.worker_id,
        exclude=args.exclude,
        subject=args.subject,
        return_table=False,
    )


if __name__ == "__main__":
    sys.exit(main())
