# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pytest>=9.1.1",
#     "pytest-benchmark>=5.2.3",
# ]
# ///
"""Perform benchmarking of bids2table against last tag, main and feature branches.

Run with:
    uv run scripts/benchmark.py \
        -b <feature_branch> [-o <output_dir>] [-f <output_file>] [-t <threshold>]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import statistics
import subprocess
import sys
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

import pytest

if TYPE_CHECKING:
    from types import GeneratorType

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger("bids2table.benchmark")

# Resolve git path once at import time
_git_path = shutil.which("git")
if _git_path is None:
    raise RuntimeError("git binary not found in PATH")
_GIT = _git_path


@contextmanager
def _suppress_log_exceptions() -> GeneratorType:
    """Temporarily disable logging exception raises.

    Suppression and resetting (after checkout) are necessary due to streaming
    of outputs during benchmark runs.
    """
    logging.raiseExceptions = False
    try:
        yield
    finally:
        logging.raiseExceptions = True


def _reset_logger() -> None:
    """Clear existing handlers and reconfigure the logger for a fresh checkout."""
    for h in _logger.handlers[:]:
        _logger.removeHandler(h)
        h.close()
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)


class Git:
    """Class to simplify git calls via subprocess."""

    def __init__(self) -> None:
        """Initialize the repository object, pulling in latest changes."""
        self.repo_path = self._root()
        self._head_ref = self._run("rev-parse", "--abbrev-ref", "HEAD")

    def __enter__(self) -> Git:
        """Verify clean working tree, pull latest, and update submodules."""
        if bool(self._run("status", "--porcelain")):
            _logger.error("Please stash or commit changes before benchmarking.")
            sys.exit(1)
        self.pull()
        self.submodule_update()
        return self

    def __exit__(self, *_: Any) -> None:  # noqa: ANN401
        """On context closure, checkout the HEAD ref."""
        self.checkout(self._head_ref)

    @staticmethod
    def _root() -> Path:
        """Return the top-level directory of the git repository."""
        result = subprocess.run(  # noqa: S603
            [_GIT, "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
        )
        return Path(result.stdout.strip())

    def _run(self, *args: str) -> str:
        """Execute a git command and return stripped stdout.

        Args:
            *args: Git subcommand and its arguments.

        Raises:
            SystemExit: If the git command returns a non-zero exit code.
        """
        result = subprocess.run(  # noqa: S603
            [_GIT, "-C", str(self.repo_path), *args],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            _logger.error(result.stderr.strip())
            sys.exit(result.returncode)
        return result.stdout.strip()

    def checkout(self, ref: str) -> None:
        """Checkout reference.

        Args:
            ref: Reference to checkout (e.g. branch, SHA, tag)
        """
        self._run("checkout", ref)

    def pull(self) -> None:
        """Pull from the remote repository."""
        self._run("pull")

    def submodule_update(self) -> None:
        """Update submodules of the repo, initializing if necessary."""
        self._run("submodule", "update", "--init", "--recursive")

    def last_tag(self) -> str:
        """Get last tag.

        Returns:
            A string value of the last tag
        """
        return self._run("describe", "--tags", "--abbrev=0")


class BenchmarkResult(NamedTuple):
    """Holds statistics for a single benchmark run.

    Attributes:
        fullname: Full pytest identifier for the benchmark.
        kind: Either "index" for indexing benchmarks or "query" for query benchmarks.
        locality: Data locality for index benchmarks ("local" or "remote");
            ``None`` for queries.
        workers: Number of parallel workers for index benchmarks; ``1`` for queries.
        median: Median execution time (seconds).
        mean: Mean execution time (seconds).
        stddev: Standard deviation of execution times (seconds).
    """

    fullname: str
    kind: Literal["index", "query"]
    locality: Literal["local", "remote"] | None = None
    workers: int = 1
    median: float = 0.0
    mean: float = 0.0
    stddev: float = 0.0


def parse_file(path: Path) -> dict[str, BenchmarkResult]:
    """Parse a pytest-benchmark JSON file into a dict of results.

    Args:
        path: Path to the JSON output produced by ``--benchmark-json``.

    Returns:
        A dict mapping benchmark fullnames to their ``BenchmarkResult`` entries.
    """
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


# Values are always provided in seconds in the JSON outputs.
# Need to scale appropriately (also noting factor and unit to pass along for
# formatting).
class Value(NamedTuple):
    """Scaled time value with its display unit.

    Attributes:
        value: The time value scaled to the chosen unit.
        factor: The multiplier used to scale from seconds (e.g., 1e3 for ms).
        unit: Human-readable unit label ("s", "ms", or "µs").
    """

    value: float
    factor: float
    unit: str


def _scale(val: float) -> Value:
    """Scale a time value (seconds) to the most appropriate unit."""
    if val >= 1.0:
        return Value(value=val, factor=1, unit="s")
    if val >= 1e-3:
        return Value(value=val * 1e3, factor=1e3, unit="ms")
    return Value(value=val * 1e6, factor=1e6, unit="µs")


def _fmt(res: BenchmarkResult) -> str:
    """Format a benchmark result as a human-readable string (median, mean ± std)."""
    median = _scale(res.median)
    mean = res.mean * median.factor
    stddev = res.stddev * median.factor
    return f"{median.value:.3f} ({mean:.3f} ± {stddev:.3f}) {median.unit}"


def _ratio(pr: BenchmarkResult, ref: BenchmarkResult, threshold: float) -> str:
    """Compute the median ratio between two results with a status icon.

    Returns a string like `"🔴 1.050"` (slower), `"⚪ 1.010"` (within threshold),
    or `"🟢 0.950"` (faster).
    """
    ratio = pr.median / ref.median
    if abs(1 - ratio) <= threshold:
        icon = "⚪"
    elif ratio > 1:
        icon = "🔴"
    else:
        icon = "🟢"
    return f"{icon} {ratio:.3f}"


def _label(result: BenchmarkResult) -> str:
    """Produce a human-readable label for a benchmark result."""
    if result.kind == "query":
        return (
            result.fullname.split("::")[-1]
            .replace("test_", "")
            .replace("_", " ")
            .capitalize()
        )
    if result.locality is None:
        raise ValueError("No result found for indexing")
    return f"{result.locality.capitalize()} index ({result.workers} workers)"


def build_table(
    threshold: float,
    branch_name: str,
    branch: dict[str, BenchmarkResult],
    main: dict[str, BenchmarkResult],
    tag: dict[str, BenchmarkResult] | None = None,
) -> str:
    """Build a Markdown table comparing benchmark results across branches.

    Args:
        threshold: Fractional threshold below which a ratio is considered unchanged.
        branch_name: Human-readable name of the feature branch.
        branch: Parsed benchmark results for the feature branch.
        main: Parsed benchmark results for ``main``.
        tag: Optional parsed benchmark results for the last tag.

    Returns:
        A Markdown string containing the comparison table.
    """
    tag = tag or {}
    all_keys = sorted(
        set(branch) | set(main) | set(tag),
        key=lambda x: (0 if "index" in x else 1 if "query" in x else 2, x),
    )
    labels = [_label(branch.get(k) or main.get(k) or tag.get(k)) for k in all_keys]  # ty: ignore[invalid-argument-type] - temporary until tag benchmark

    col_sep = " | "
    header = "| |" + col_sep.join(f" **{label}** " for label in labels) + " |"
    divider = "|-|" + "|".join("---" for _ in all_keys) + "|"

    def row(name: str, results: dict[str, BenchmarkResult]) -> str:
        cells = [_fmt(results[k]) if k in results else "—" for k in all_keys]
        return "| **" + name + "** |" + col_sep.join(f" {c} " for c in cells) + " |"

    def ratio_row(label: str, ref: dict[str, BenchmarkResult]) -> str:
        cells = [
            _ratio(branch[k], ref[k], threshold) if k in branch and k in ref else "—"
            for k in all_keys
        ]
        return "| *" + label + "* |" + col_sep.join(f" {c} " for c in cells) + " |"

    lines = [
        "## Benchmark Results",
        "",
        header,
        divider,
        row(branch_name, branch),
        row("main", main),
        divider.replace("-", ""),
        ratio_row(f"{branch_name} vs main ratio", main),
        "",
        "> `median (mean ± std)`",
        "> ",
        f"> 🔴 Slower &nbsp; ⚪ No change (<{threshold * 100:.0f} %) &nbsp; 🟢 Faster",
    ]
    return "\n".join(lines)


def _parser() -> argparse.Namespace:
    """Build and parse command-line arguments for the benchmark script."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--branch", required=True, help="PR branch to benchmark")
    parser.add_argument(
        "-o",
        "--output-dir",
        default="benchmarks",
        type=Path,
        help="Output directory to save benchmarks to",
    )
    parser.add_argument(
        "-f",
        "--output-file",
        required=False,
        type=str,
        help="Output file name",
    )
    parser.add_argument(
        "-t",
        "--threshold",
        default=0.05,
        type=float,
        help="Threshold for performance to be considered unchanged",
    )
    parser.add_argument(
        "-k",
        "--filter",
        default=None,
        help="pytest -k expression to filter which benchmarks to run",
    )
    return parser.parse_args()


def _sanitize(s: str) -> str:
    """Replace ``/`` with ``-`` to produce a filesystem-safe branch name."""
    return s.replace("/", "-")


def run_benchmark(
    git: Git, branch: str, out_dir: Path, filter_expr: str | None = None
) -> None:
    """Run pytest benchmarks for the given branch, ``main``, and last tag.

    Checks out each target, runs the benchmark suite, and saves JSON results
    to ``out_dir``.

    Args:
        git: Repository handle used to switch between branches.
        branch: Feature branch to benchmark.
        out_dir: Directory in which to write the JSON benchmark files.
        filter_expr: Optional ``pytest -k`` expression to limit which benchmarks run.
    """
    tag = git.last_tag()
    targets = {branch: branch, "main": "main", tag: None}

    with _suppress_log_exceptions():
        for name, ref in targets.items():
            # Skip if the reference is not provided
            if ref is None:
                continue
            git.checkout(ref)
            _reset_logger()
            _logger.info("Running benchmarks for '%s'", name)

            safe_name = _sanitize(name)
            fname = out_dir / f"benchmark-{safe_name}.json"
            if fname.exists():
                _logger.warning(
                    "Existing benchmarks found for %s. File will be overwritten.", fname
                )

            # Run benchmark
            pytest_args = [
                "-m",
                "benchmark",
                "--benchmark-save-data",
                f"--benchmark-json={fname}",
                "--benchmark-time-unit=ms",
                "--benchmark-warmup=on",
            ]
            if filter_expr:
                pytest_args.extend(["-k", filter_expr])
            pytest_args.append(f"{git.repo_path}/tests")
            pytest.main(pytest_args)


def generate_report(
    git: Git, branch: str, threshold: float, out_dir: Path, out_fname: str | None = None
) -> Path:
    """Generate markdown report from benchmarks.

    Args:
        git: Representation of current git repository for benchmarking
        branch: Feature branch benchmarked
        threshold: Threshold for performance to be considered unchanged
        out_dir: Directory benchmarks are saved to / output report to
        out_fname: Benchmark output file name

    Returns:
        Path to file containing benchmark comparison table

    Raises:
        AssertionError: if less than 2 benchmark files found
    """
    with _suppress_log_exceptions():
        git.checkout(branch)
        _reset_logger()
        _logger.info("Generating benchmark report")

        files = sorted(out_dir.glob("benchmark-*.json"))
        if len(files) < 2:
            raise AssertionError(
                "Expected 2 or more benchmark files to perform comparisons."
            )

        tag = git.last_tag()
        parsed: dict[str, dict[str, BenchmarkResult]] = {}
        for f in files:
            if not f.exists():
                _logger.warning("File %s does not exist - skipping", f)
                continue
            key = f.stem.split("-")[1]
            if key == tag:
                pass  # keep as tag name
            elif key != "main":
                key = branch
            parsed[key] = parse_file(f)

        if tag not in parsed:
            _logger.warning("Tag '%s' not found in benchmark files.", tag)

        report_contents = build_table(
            threshold,
            branch,
            parsed[branch],
            parsed["main"],
            None,  # parsed.get(tag)
        )
        if out_fname is None:
            dt = datetime.now(UTC).strftime("%Y%m%dT%H%M")
            out_fname = f"benchmark-{_sanitize(branch)}-{dt}.md"
        report_file = out_dir / out_fname
        report_file.write_text(report_contents)
        _logger.info("Report written to %s", report_file)

        return report_file


def main() -> None:
    """Entry point: parse args, run benchmarks, and generate the comparison report."""
    args = _parser()
    if abs(args.threshold) > 1:
        raise ValueError(f"Threshold should be between 0 and 1, got: {args.threshold}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    with Git() as git:
        run_benchmark(
            git=git,
            branch=args.branch,
            out_dir=args.output_dir,
            filter_expr=args.filter,
        )
        report_file = generate_report(
            git=git,
            branch=args.branch,
            threshold=args.threshold,
            out_dir=args.output_dir,
            out_fname=args.output_file,
        )

        if "GITHUB_OUTPUT" in os.environ:
            with Path(os.environ["GITHUB_OUTPUT"]).open("a") as f:
                f.write(f"report_file={report_file}\n")


if __name__ == "__main__":
    main()
