import argparse
import sys
from pathlib import Path

from elbow.utils import setup_logging

from bids2table import bids2table


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
    parser.add_argument("--verbose", "-v", help="Verbose logging.", action="store_true")

    args = parser.parse_args()

    setup_logging("INFO" if args.verbose else "ERROR")

    bids2table(
        root=args.root,
        persistent=True,
        output=args.output,
        incremental=args.incremental,
        overwrite=args.overwrite,
        workers=args.workers,
        worker_id=args.worker_id,
        return_df=False,
    )


if __name__ == "__main__":
    sys.exit(main())
