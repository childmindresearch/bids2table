"""Finding and indexing BIDS datasets.

Uses only `pathlib.Path` methods and string processing to find and filter the files.
Returns a dataset index as an Arrow table.
"""

import enum
import fnmatch
import importlib.metadata
import re
from concurrent.futures import Executor, ProcessPoolExecutor
from functools import partial
from typing import Any, Callable, Generator, Iterable, Sequence

import pyarrow as pa
from tqdm import tqdm

from ._entities import (
    _cache_parse_bids_entities,
    get_bids_entity_arrow_schema,
    validate_bids_entities,
)
from ._logging import setup_logger
from ._pathlib import Path

_BIDS_SUBJECT_DIR_PATTERN = re.compile(r"sub-[a-zA-Z0-9]+")

# Path names of BIDS dataset sub-directories that may contain nested BIDS datasets.
# Other candidates to consider including:
#   - sourcedata
#   - code
_BIDS_NESTED_PARENT_DIRNAMES = {
    "derivatives",
}

# Typically json files are reserved for sidecar metadata only. However there are some
# exceptions. One way to test whether a json file is sidecar or data is to check for any
# matching non-json files at the same level. But that is a lot of work to do for a few
# special cases. Rather, we just list the special case suffixes here. (Honestly, using
# plain json extension for data files should be discouraged.)
_BIDS_JSON_SIDECAR_EXCEPTION_SUFFIXES = {
    "coordsystem",
}

# Configs for index arrow fields to add to the entity schema (defined elsewhere).
_INDEX_ARROW_FIELDS = {
    "dataset": {
        "dtype": pa.string(),
        "metadata": {
            "name": "dataset",
            "display_name": "Dataset name",
            "description": (
                "BIDS dataset name. Nested datasets are represented as "
                "a partial path like 'ds000001/derivatives/fmriprep'."
            ),
        },
    },
    "root": {
        # NOTE: Trying out dictionary type to save memory on these repeated long
        # strings. Only question is compatibility with other libraries like pandas.
        "dtype": pa.dictionary(pa.int32(), pa.string()),
        "metadata": {
            "name": "root",
            "display_name": "Dataset root path",
            "description": "Absolute root path for the dataset.",
        },
    },
    "path": {
        "dtype": pa.string(),
        "metadata": {
            "name": "path",
            "display_name": "File path",
            "description": "BIDS file path relative to the dataset root.",
        },
    },
    "extra_entities": {
        "dtype": pa.map_(pa.string(), pa.string()),
        "metadata": {
            "name": "extra_entities",
            "display_name": "Extra BIDS entities",
            "description": "Map of extra BIDS entities not matching current schema.",
        },
    },
}

_logger = setup_logger(__package__)


def get_arrow_schema() -> pa.Schema:
    """Get Arrow schema of the BIDS dataset index."""
    entity_schema = get_bids_entity_arrow_schema()
    index_fields = {
        name: pa.field(name, cfg["dtype"], metadata=cfg["metadata"])
        for name, cfg in _INDEX_ARROW_FIELDS.items()
    }
    fields = [
        index_fields["dataset"],
        *entity_schema,
        index_fields["extra_entities"],
        index_fields["root"],
        index_fields["path"],
    ]

    metadata = {
        **entity_schema.metadata,
        "bids2table_version": importlib.metadata.version(__package__),
    }
    schema = pa.schema(fields, metadata=metadata)
    return schema


def get_column_names() -> enum.EnumType:
    """Get an enum of the BIDS index columns."""
    # TODO: It might be nice if the column names were statically available. One option
    # would be to generate a static _schema.py module at install time (similar to how
    # _version.py is generated) which defines the static default schema and column
    # names.
    schema = get_arrow_schema()
    items = []
    for f in schema:
        name = f.metadata["name".encode()].decode()
        items.append((name, name))

    BIDSColumn = enum.StrEnum("BIDSColumn", items)
    BIDSColumn.__doc__ = "Enum of BIDS index column names."
    return BIDSColumn


def find_bids_datasets(
    root: str | Path,
    exclude: str | list[str] | None = None,
    follow_symlinks: bool = True,
    log_frequency: int = 100,
) -> Generator[Path, None, None]:
    """Find all BIDS datasets under a root directory.

    Args:
        root: Root path to begin search.
        exclude: Glob pattern or list of patterns matching sub-directory names to
            exclude from the search.
        follow_symlinks: Search into symlinks that point to directories.

    Yields:
        Root paths of all BIDS datasets under `root`.
    """
    if isinstance(root, str):
        root = Path(root)

    dir_count = 0
    ds_count = 0

    # NOTE: Path.walk was introduced in 3.12. Otherwise, could use an older python.
    for dirpath, dirnames, _ in root.walk(follow_symlinks=follow_symlinks):
        dir_count += 1

        if _is_bids_dataset(dirpath):
            ds_count += 1
            yield dirpath

            # Only descend into specific sub-directories that are allowed to contain
            # sub-datasets.
            _filter_dirnames(dirnames, _BIDS_NESTED_PARENT_DIRNAMES)

        # Filter sub-directories to descend into.
        if exclude:
            matches = _filter_exclude(dirnames, exclude)
            _filter_dirnames(dirnames, matches)

        if log_frequency and dir_count % log_frequency == 0:
            _logger.info(
                "Searched %d directories; found %d BIDS datasets.", dir_count, ds_count
            )

    if log_frequency:
        _logger.info(
            "Searched %d directories; found %d BIDS datasets.", dir_count, ds_count
        )


def index_dataset(
    root: str | Path,
    include_subjects: str | list[str] | None = None,
    max_workers: int | None = 0,
    chunksize: int = 32,
    executor_cls: type[Executor] = ProcessPoolExecutor,
    show_progress: bool = False,
) -> pa.Table:
    """Index a BIDS dataset.

    Args:
        root: BIDS dataset root directory.
        include_subjects: Glob pattern or list of patterns for matching subjects to
            include in the index.
        max_workers: Number of indexing processes to run in parallel. Setting
            `max_workers=0` (the default) uses the main process only. Setting
            `max_workers=None` starts as many workers as there are available CPUs. See
            `concurrent.futures.ProcessPoolExecutor` for details.
        chunksize: Number of subjects per process task. Only used for
            `ProcessPoolExecutor` when `max_workers > 0`.
        executor_cls: Executor class to use for parallel indexing.
        show_progress: Show progress bar.

    Returns:
        An Arrow table index of the BIDS dataset.
    """
    if isinstance(root, str):
        root = Path(root)

    schema = get_arrow_schema()

    dataset, _ = _get_bids_dataset(root)
    if dataset is None:
        _logger.warning(f"Path {root} is not a valid BIDS dataset directory.")
        return pa.Table.from_pylist([], schema=schema)

    subject_dirs = _find_bids_subject_dirs(root, include_subjects)
    subject_dirs = sorted(subject_dirs, key=lambda p: p.name)
    if len(subject_dirs) == 0:
        _logger.warning(f"Path {root} contains no matching subject dirs.")
        return pa.Table.from_pylist([], schema=schema)

    func = partial(_index_bids_subject_dir, schema=schema, dataset=dataset)

    tables = []
    file_count = 0
    for sub, table in (
        pbar := tqdm(
            _pmap(func, subject_dirs, max_workers, chunksize, executor_cls),
            desc=dataset,
            total=len(subject_dirs),
            disable=not show_progress,
        )
    ):
        file_count += len(table)
        pbar.set_postfix(dict(sub=sub, N=_hfmt(file_count)), refresh=False)
        tables.append(table)

    # NOTE: concat_tables produces a table where each column is a ChunkedArray, with one
    # chunk per original subject table. Is it better to keep the original chunks (one
    # per subject) or merge using `combine_chunks`?
    table = pa.concat_tables(tables).combine_chunks()
    return table


def batch_index_dataset(
    roots: list[str | Path],
    max_workers: int | None = 0,
    executor_cls: type[Executor] = ProcessPoolExecutor,
    show_progress: bool = False,
) -> Generator[pa.Table, None, None]:
    """Index a batch of BIDS datasets.

    Args:
        roots: List of BIDS dataset root directories.
        max_workers: Number of indexing processes to run in parallel. Setting
            `max_workers=0` (the default) uses the main process only. Setting
            `max_workers=None` starts as many workers as there are available CPUs. See
            `concurrent.futures.ProcessPoolExecutor` for details.
        executor_cls: Executor class to use for parallel indexing.
        show_progress: Show progress bar.

    Yields:
        An Arrow table index for each BIDS dataset.
    """
    file_count = 0
    for dataset, table in (
        pbar := tqdm(
            _pmap(_batch_index_func, roots, max_workers, executor_cls=executor_cls),
            total=len(roots) if isinstance(roots, Sequence) else None,
            disable=show_progress not in {True, "dataset"},
        )
    ):
        file_count += len(table)
        pbar.set_postfix(dict(ds=dataset, N=_hfmt(file_count)), refresh=False)
        yield table


def _batch_index_func(root: str | Path) -> tuple[str, pa.Table]:
    dataset, _ = _get_bids_dataset(root)
    table = index_dataset(root, max_workers=0, show_progress=False)
    return dataset, table


def _get_bids_dataset(path: str | Path) -> tuple[str | None, Path | None]:
    """Get the BIDS dataset that the path belongs to, if any.

    Return the dataset directory name and the full dataset path. For nested derivatives
    datasets, a composite name of the form ``"ds000001/derivatives/fmriprep"`` is
    returned.

    Note that the name is extracted from the path, not the dataset description JSON.
    """
    if isinstance(path, str):
        path = Path(path)

    parent = path
    parts: list[str] = []
    scanning = False
    top_idx = None
    root = None

    while parent.name:
        if _is_bids_dataset(parent):
            scanning = True
            top_idx = len(parts)
            if root is None:
                root = parent

        if scanning:
            parts.append(parent.name)

        parent = parent.parent

    if len(parts) == 0:
        return None, None

    parts = parts[: top_idx + 1]
    dataset = "/".join(reversed(parts))
    return dataset, root


def _is_bids_dataset(path: Path) -> bool:
    """Test if path is a BIDS dataset root directory."""
    # Check if contains a dataset_description.json or any subject directories. Note,
    # it's common for ppl to forget the dataset description, so let's not be too strict.
    description_exists = (path / "dataset_description.json").exists()
    return description_exists or _contains_bids_subject_dirs(path)


def _contains_bids_subject_dirs(root: Path) -> bool:
    """Check if a path contains one or more BIDS subject dirs."""
    # Nb, this will return on the first matching path thanks to the generator.
    return any(_is_bids_subject_dir(path) for path in root.glob("sub-*"))


def _find_bids_subject_dirs(
    root: Path,
    include_subjects: str | list[str] | None = None,
) -> list[Path]:
    """Find all BIDS subject dirs contained in a root directory.

    Note, only looks one level down. Does not find nested subject directories, e.g. in
    derivatives datasets.
    """
    paths = [path for path in root.glob("sub-*") if _is_bids_subject_dir(path)]

    if include_subjects:
        filtered_names = _filter_include(
            set(path.name for path in paths), include_subjects
        )
        paths = [path for path in paths if path.name in filtered_names]
    return paths


def _is_bids_subject_dir(path: Path) -> bool:
    """Check if a path is a BIDS subject directory."""
    # NOTE: not checking if the path is in fact a directory.
    # This is a slow op, especially on cloud. Can assume that there are no files
    # matching the subject dir pattern, and even if there are, the rglob that happens
    # later will just return empty.
    return bool(re.fullmatch(_BIDS_SUBJECT_DIR_PATTERN, path.name))


def _index_bids_subject_dir(
    path: Path,
    schema: pa.Schema | None = None,
    dataset: str | None = None,
) -> tuple[str, pa.Table]:
    """Index a BIDS subject directory and return an Arrow table."""
    root = path.parent
    root_fmt = str(root.absolute())
    if dataset is None:
        dataset, _ = _get_bids_dataset(root)
    if schema is None:
        schema = get_arrow_schema()

    _, subject = path.name.split("-", maxsplit=1)

    records = []
    for p in path.rglob("sub-*"):
        if _is_bids_file(p):
            entities = _cache_parse_bids_entities(p)
            valid_entities, extra_entities = validate_bids_entities(entities)
            record = {
                "dataset": dataset,
                **valid_entities,
                "extra_entities": extra_entities,
                "root": root_fmt,
                "path": str(p.relative_to(root)),
            }
            records.append(record)

    table = pa.Table.from_pylist(records, schema=schema)
    return subject, table


def _is_bids_file(path: Path) -> bool:
    """Check if file is a BIDS file.

    Not very exact, but hopefully good enough.
    """
    # TODO: other checks?
    #   - skip files matching patterns in .bidsignore?

    # initial fast checks for missing extension or name that doesn't start with sub-
    if path.suffix == "" or not path.name.startswith("sub-"):
        return False

    entities = _cache_parse_bids_entities(path)
    # if not (entities.get("suffix") and entities.get("datatype")):
    if not (entities.get("suffix") and entities.get("ext")):
        return False

    if _is_bids_json_sidecar(path):
        return False

    # very special case for directories that are treated as bids "files"
    # e.g. microscopy .ome.zarr directories or MEG .ds directories.
    # A little annoying that we have to do this.
    if _is_bids_file(path.parent):
        return False
    return True


def _is_bids_json_sidecar(path: Path) -> bool:
    """Quick check if a file is a JSON sidecar."""
    # Quick check if path suffix is not json.
    if path.suffix != ".json":
        return False

    # Other checks require entities.
    entities = _cache_parse_bids_entities(path)

    # Second pass using full compound extension, in case of data files that use a
    # compound extension ending in .json.
    if entities.get("ext") != ".json":
        return False

    # Assume all JSON above the lowest level of hierarchy are sidecars.
    if entities.get("datatype") is None:
        return True

    # All sidecars must contain a suffix.
    # Also check if suffix matches special cases of data files with json extension.
    suffix = entities.get("suffix")
    if suffix is None or suffix in _BIDS_JSON_SIDECAR_EXCEPTION_SUFFIXES:
        return False
    return True


def _pmap(
    func: Callable,
    iterable: Iterable[Any],
    max_workers: int | None = 0,
    chunksize: int = 1,
    executor_cls: type[Executor] = ProcessPoolExecutor,
):
    if max_workers == 0:
        yield from map(func, iterable)
    else:
        with executor_cls(
            max_workers=max_workers,
            initializer=partial(setup_logger, name=__package__, level=_logger.level),
        ) as executor:
            yield from executor.map(func, iterable, chunksize=chunksize)


def _filter_include(
    names: Iterable[str],
    patterns: str | list[str],
) -> set[str]:
    """Filter names including those that match a glob pattern or list of patterns."""
    names = set(names)
    matching_names = _multi_pattern_filter(names, patterns)
    names.intersection_update(matching_names)
    return names


def _filter_exclude(
    names: Iterable[str],
    patterns: str | list[str],
) -> set[str]:
    """Filter names excluding those that match a glob pattern or list of patterns."""
    names = set(names)
    matching_names = _multi_pattern_filter(names, patterns)
    names.difference_update(matching_names)
    return names


def _multi_pattern_filter(names: list[str], patterns: str | list[str]) -> set[str]:
    """Filter names matching any of a list of patterns."""
    if isinstance(patterns, str):
        patterns = [patterns]
    matching_names = set()
    for pat in patterns:
        matching_names.update(fnmatch.filter(names, pat))
    return matching_names


def _filter_dirnames(dirnames: list[str], matches: set[str]) -> None:
    """Remove dirnames matching `matches` in place."""
    # Iterate in reversed order since we are modifying in place.
    n_names = len(dirnames)
    for ii, dirname in enumerate(reversed(dirnames)):
        if dirname not in matches:
            del dirnames[n_names - ii - 1]


def _hfmt(n: int) -> str:
    if n < 10_000:
        n_fmt = str(n)
    elif n < 1_000_000:
        n_fmt = f"{n / 1000:.0f}K"
    elif n < 10_000_000:
        n_fmt = f"{n / 1_000_000:.1f}M"
    else:
        n_fmt = f"{n / 1_000_000:.0f}M"
    return n_fmt
