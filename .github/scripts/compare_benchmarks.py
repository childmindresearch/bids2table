#!/usr/bin/env python
"""Compare benchmark results across PR, main, and tag and output a markdown table."""

import json
import statistics
from pathlib import Path
from typing import Literal, NamedTuple


class BenchmarkResult(NamedTuple):
    fullname: str
    kind: Literal["index", "query"]
    locality: Literal["local", "remote"] | None = None
    workers: int | None = None
    median: float = 0.0
    mean: float = 0.0
    stddev: float = 0.0


def parse_file(path: Path) -> dict[str, BenchmarkResult]:
    data = json.loads(path.read_text())
    results = {}
    for benchmark in data["benchmarks"]:
        fullname: str = benchmark["fullname"]
        data_trimmed = benchmark["stats"]["data"][1:]
        median = statistics.median(data_trimmed)
        mean = statistics.mean(data_trimmed)
        stddev = statistics.stdev(data_trimmed)

        if "query" in fullname:
            result = BenchmarkResult(
                fullname=fullname, kind="query", median=median, mean=mean, stddev=stddev
            )
        else:
            locality: Literal["local", "remote"] = (
                "remote" if "openneuro" in fullname or "s3" in fullname else "local"
            )
            workers = benchmark["extra_info"].get("workers", "Unknown")
            result = BenchmarkResult(
                fullname=fullname,
                kind="index",
                locality=locality,
                workers=workers,
                median=median,
                mean=mean,
                stddev=stddev,
            )
        results[fullname] = result
    return results


def _scale(val: float) -> float:
    return val * 1000


def _fmt(res: BenchmarkResult) -> str:
    median = _scale(res.median)
    mean = _scale(res.mean)
    stddev = _scale(res.stddev)
    return f"{median:.3f} ({mean:.3f} ± {stddev:.3f}) ms"


def _delta(pr: BenchmarkResult, ref: BenchmarkResult) -> str:
    if ref == 0:
        return "N/A"
    diff = _scale(pr.median - ref.median)
    pct = (pr.median / ref.median - 1) * 100
    icon = "🔴" if pct > 5 else "🟢" if pct < -5 else "⚪"
    return f"{icon} {diff:+.3f} ms ({pct:+.1f}%)"


def _label(result: BenchmarkResult) -> str:
    if result.kind == "query":
        return (
            result.fullname.split("::")[-1]
            .replace("test_", "")
            .replace("_", " ")
            .capitalize()
        )
    return f"{result.locality.capitalize()} index ({result.workers} workers)"


def build_table(
    pr: dict[str, BenchmarkResult],
    main: dict[str, BenchmarkResult],
    tag: dict[str, BenchmarkResult],
    tag_name: str,
) -> str:
    all_keys = set(pr) | set(main) | set(tag)
    labels = [_label((pr.get(k) or main.get(k) or tag.get(k))) for k in all_keys]

    col_sep = " | "
    header = "| |" + col_sep.join(f" **{label}** " for label in labels) + " |"
    divider = "|-|" + "|".join("---" for _ in all_keys) + "|"

    def row(name: str, results: dict[str, BenchmarkResult]) -> str:
        cells = [_fmt(results[k]) if k in results else "—" for k in all_keys]
        return "| **" + name + "** |" + col_sep.join(f" {c} " for c in cells) + " |"

    def delta_row(label: str, ref: dict[str, BenchmarkResult]) -> str:
        cells = [
            _delta(pr[k], ref[k]) if k in pr and k in ref else "—" for k in all_keys
        ]
        return "| *" + label + "* |" + col_sep.join(f" {c} " for c in cells) + " |"

    lines = [
        "## Benchmark Results",
        "",
        header,
        divider,
        row("PR", pr),
        row("main", main),
        row(tag_name, tag),
        divider.replace("-", ""),
        delta_row("PR vs main", main),
        delta_row(f"PR vs {tag_name}", tag),
        "",
        "> `median (mean ± std)`",
        "> ",
        "🔴 >5% slower &nbsp; ⚪ within 5% &nbsp; 🟢 >5% faster",
    ]
    return "\n".join(lines)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pattern",
        default="benchmark-*.json",
        help="Glob pattern for benchmark JSON files",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output markdown filepath containing benchmark comparisons",
    )
    args = parser.parse_args()

    files = sorted(Path(".").glob(args.pattern))
    assert len(files) == 3, f"Expected 3 files, found {len(files)}: {files}"

    # Infer pr/main/tag from directory name
    parsed: dict[str, BenchmarkResult] = {}
    tag = None
    for f in files:
        stem = f.parent.name  # e.g. "benchmark-pr"
        key = stem.split("-")[-1]  # "pr", "main", tag
        if key not in ("pr", "main"):
            tag = key
        parsed[key] = parse_file(f)
    if tag is None:
        raise ValueError("Unknown tag")
    table = build_table(parsed["pr"], parsed["main"], parsed[tag], tag_name=tag)
    args.output.write_text(table)
    print(table)


if __name__ == "__main__":
    main()
