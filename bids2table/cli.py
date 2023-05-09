import argparse
from pathlib import Path

import elbow.utils

from .loaders import load_bids_parquet


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i", "--input",
        type=Path,
        help="Path to BIDS dataset",
        required=True
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Path to output parquet dataset directory",
        required=True
    )
    parser.add_argument(
        "-u", "--update",
        help="update dataset incrementally with only new or changed files.",
        action="store_true"
    )
    parser.add_argument(
        "-c", "--cores",
        type=int,
        help="number of parallel processes. If `None` or 1, run in the main"
             "process. Setting to -1 runs in `os.cpu_count()` processes.",
        default=None
    )
    parser.add_argument(
        "-s", "--suppress_warnings",
        help="suppress warnings",
        action="store_true"
    )

    args = parser.parse_args()

    if args.suppress_warnings:
        elbow.utils.setup_logging("ERROR")

    load_bids_parquet(
        path=args.input,
        where=args.output,
        incremental=args.update,
        workers=args.cores
    )


if __name__ == '__main__':
    main()
