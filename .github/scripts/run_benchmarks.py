#!/usr/bin/env python
"""Perform benchmarks across PR commit, main, and previous tag."""

import argparse

import pytest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", required=True, help="Output JSON file path")
    args = parser.parse_args()

    pytest.main(
        [
            "-m",
            "benchmark",
            "--benchmark-save-data",
            f"--benchmark-json={args.output}",
            "--benchmark-time-unit=ms",
            "--benchmark-warmup=on",
        ]
    )


if __name__ == "__main__":
    main()
