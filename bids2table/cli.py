import argparse
from pathlib import Path

import elbow.utils

from .loaders import load_bids_parquet


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i", "--input", type=Path, help="Path to BIDS dataset.", required=True
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Path to output parquet dataset directory",
        required=True,
    )
    parser.add_argument(
        "-u",
        "--incremental",
        help="Update dataset incrementally with only new or changed files.",
        action="store_true",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        help="Number of parallel processes. If `None` or 1, run in the main"
        "process. Setting to -1 runs in `os.cpu_count()` processes.",
        default=None,
    )
    parser.add_argument(
        "-v", "--verbose", help="Verbose logging.", action="store_true"
    )

    args = parser.parse_args()

    if not args.verbose:
        elbow.utils.setup_logging("ERROR")

    load_bids_parquet(
        path=args.input,
        where=args.output,
        incremental=args.incremental,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
