import logging
from pathlib import Path

from bids2table.logging import setup_logging


def test_setup_logging(tmp_path: Path):
    log_path = tmp_path / "log.txt"
    logger = setup_logging(
        level=logging.INFO,
        log_path=tmp_path / "log.txt",
        max_repeats=5,
        overwrite=True,
    )

    for ii in range(0, 10):
        logger.info(f"log msg {ii}")

    log = log_path.read_text().strip().split("\n")
    assert len(log) == 6
