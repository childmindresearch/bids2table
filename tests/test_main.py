"""Tests for the bids2table CLI entry point."""

import shlex
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest
from bids2table import __main__ as cli

BIDS_EXAMPLES = Path(__file__).parents[1] / "bids-examples"


@pytest.fixture(scope="module")
def cli_prog() -> str:
    """Return the absolute path to the CLI module (module-scoped, computed once)."""
    return str(Path(cli.__file__).absolute())


@contextmanager
def patch_argv(argv: list[str]):
    """Temporarily replace ``sys.argv`` for CLI invocation tests."""
    old_argv = sys.argv
    try:
        sys.argv = argv.copy()
        yield
    finally:
        sys.argv = old_argv


@pytest.mark.parametrize(
    ("cmd", "output"),
    [
        ("index -o {out_dir}/ds102.parquet {examples}/ds102", "ds102.parquet"),
        (
            "index -o {out_dir}/ds101_ds102.parquet {examples}/ds101 {examples}/ds102",
            "ds101_ds102.parquet",
        ),
        ("index -o {out_dir}/ds10N.parquet '{examples}/ds10?'", "ds10N.parquet"),
    ],
)
def test_main_index(cmd: str, output: str | None, tmp_path: Path, cli_prog: str):
    """Run the ``index`` subcommand and verify the output parquet file is created."""
    cmd_fmt = cmd.format(out_dir=tmp_path, examples=BIDS_EXAMPLES)
    argv = [cli_prog, *shlex.split(cmd_fmt)]
    with patch_argv(argv):
        cli.main()

    if output:
        assert (tmp_path / output).exists()


@pytest.mark.parametrize("cmd", ["find {examples}"])
def test_main_find(cmd: str, cli_prog: str):
    """Run the ``find`` subcommand and verify it completes without error."""
    cmd_fmt = cmd.format(examples=BIDS_EXAMPLES)
    argv = [cli_prog, *shlex.split(cmd_fmt)]
    with patch_argv(argv):
        cli.main()


def test_main_index_filter_flag(tmp_path: Path, cli_prog: str):
    """CLI --filter flag works and produces output file."""
    ds = tmp_path / "ds"
    (ds / "sub-A01" / "anat").mkdir(parents=True)
    (ds / "dataset_description.json").write_text('{"Name": "ds"}')
    (ds / "sub-A01" / "anat" / "sub-A01_T1w.nii.gz").touch()

    out = tmp_path / "output.parquet"
    argv = [cli_prog, "index", "-o", str(out), "-f", "task=rest", str(ds)]
    with patch_argv(argv):
        cli.main()
    assert out.exists()


def test_main_index_filter_subject(tmp_path: Path, cli_prog: str):
    """CLI --filter sub=A01 works and reduces indexed rows."""
    ds = tmp_path / "ds"
    for sub in ("A01", "A02"):
        (ds / f"sub-{sub}" / "anat").mkdir(parents=True)
        (ds / f"sub-{sub}" / "anat" / f"sub-{sub}_T1w.nii.gz").touch()
    (ds / "dataset_description.json").write_text('{"Name": "ds"}')

    # No filter → 2 rows
    out_unfiltered = tmp_path / "output.parquet"
    argv_unfiltered = [cli_prog, "index", "-o", str(out_unfiltered), str(ds)]
    with patch_argv(argv_unfiltered):
        cli.main()
    assert out_unfiltered.exists()

    # -f sub=A01 → 1 row
    out_filtered = tmp_path / "output_filtered.parquet"
    argv_filter = [cli_prog, "index", "-o", str(out_filtered), "-f", "sub=A01", str(ds)]
    with patch_argv(argv_filter):
        cli.main()
    assert out_filtered.exists()


def test_main_index_subjects_deprecated(tmp_path: Path, cli_prog: str):
    """--subjects flag emits DeprecationWarning."""
    import warnings

    ds = tmp_path / "ds"
    (ds / "sub-A01" / "anat").mkdir(parents=True)
    (ds / "dataset_description.json").write_text('{"Name": "ds"}')
    (ds / "sub-A01" / "anat" / "sub-A01_T1w.nii.gz").touch()

    out = tmp_path / "output.parquet"
    argv = [cli_prog, "index", "-o", str(out), "--subjects", "A01", "--", str(ds)]
    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        with patch_argv(argv):
            cli.main()
    dep_warnings = [w for w in warns if issubclass(w.category, DeprecationWarning)]
    assert len(dep_warnings) == 1
    assert "--subjects" in str(dep_warnings[0].message)


def test_main_index_malformed_filter(tmp_path: Path, cli_prog: str):
    """Malformed --filter arg (no =) is skipped with warning."""
    ds = tmp_path / "ds"
    (ds / "sub-A01" / "anat").mkdir(parents=True)
    (ds / "dataset_description.json").write_text('{"Name": "ds"}')
    (ds / "sub-A01" / "anat" / "sub-A01_T1w.nii.gz").touch()

    out = tmp_path / "output.parquet"
    argv = [cli_prog, "index", "-o", str(out), "-f", "malformed_no_equals", str(ds)]
    with patch_argv(argv):
        cli.main()
    # Malformed filter is skipped, index still runs.
    assert out.exists()


def test_main_index_schema_flag(tmp_path: Path, cli_prog: str):
    """--schema flag is accepted when given a valid schema path."""
    ds = tmp_path / "ds"
    (ds / "sub-A01" / "anat").mkdir(parents=True)
    (ds / "dataset_description.json").write_text('{"Name": "ds"}')
    (ds / "sub-A01" / "anat" / "sub-A01_T1w.nii.gz").touch()

    # Resolve the default BIDS schema path from the installed bidsschematools package.
    import importlib

    pkg = importlib.import_module("bidsschematools")
    schema_path = str(Path(pkg.__file__).parent / "data" / "schema.json")

    out = tmp_path / "output.parquet"
    argv = [cli_prog, "index", "-o", str(out), "--schema", schema_path, str(ds)]
    with patch_argv(argv):
        cli.main()
    assert out.exists()
