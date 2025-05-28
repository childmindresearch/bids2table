import shlex
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import List

import pytest

from bids2table import __main__ as cli

BIDS_EXAMPLES = Path(__file__).parents[1] / "bids-examples"


@contextmanager
def patch_argv(argv: List[str]):
    old_argv = sys.argv
    try:
        sys.argv = argv.copy()
        yield
    finally:
        sys.argv = old_argv


@pytest.mark.parametrize(
    "cmd,output",
    [
        ("index -o {out_dir}/ds102.parquet {examples}/ds102", "ds102.parquet"),
        (
            "index -o {out_dir}/ds101_ds102.parquet {examples}/ds101 {examples}/ds102",
            "ds101_ds102.parquet",
        ),
        ("index -o {out_dir}/ds10N.parquet '{examples}/ds10?'", "ds10N.parquet"),
    ],
)
def test_main_index(cmd: str, output: str | None, tmp_path: Path):
    cmd_fmt = cmd.format(out_dir=tmp_path, examples=BIDS_EXAMPLES)
    prog = str(Path(cli.__file__).absolute())
    argv = [prog] + shlex.split(cmd_fmt)
    with patch_argv(argv):
        cli.main()

    if output:
        assert (tmp_path / output).exists()


@pytest.mark.parametrize("cmd", ["find {examples}"])
def test_main_find(cmd: str):
    cmd_fmt = cmd.format(examples=BIDS_EXAMPLES)
    prog = str(Path(cli.__file__).absolute())
    argv = [prog] + shlex.split(cmd_fmt)
    with patch_argv(argv):
        cli.main()
