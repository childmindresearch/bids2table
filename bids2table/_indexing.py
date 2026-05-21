"""Finding and indexing BIDS datasets.

Uses only `pathlib.Path` methods and string processing to find and filter the files.
Returns a dataset index as an Arrow table.
"""

import enum
import fnmatch
import importlib.metadata
import json
import re
import sys
from concurrent.futures import Executor, ProcessPoolExecutor
from functools import lru_cache, partial
from glob import glob
from typing import Any, Callable, Generator, Iterable, Sequence

import pyarrow as pa
from tqdm import tqdm

from ._entities import (
    _cache_parse_bids_entities,
    get_bids_entity_arrow_schema,
    get_entity_glob_pattern,
    get_entity_pattern,
    get_file_entity_prefixes,
    validate_bids_entities,
)
from ._logging import setup_logger
from ._pathlib import CloudPath, PathT, as_path, cloudpathlib_is_available

# Path names of BIDS dataset sub-directories that may contain nested BIDS datasets.
# Other candidates to consider including:
#   - sourcedata
#   - code
# TODO: Remove this and replace where it is invoked with reference from bidsschematools
_BIDS_NESTED_PARENT_DIRNAMES = {
    "derivatives",
}

# Typically json files are reserved for sidecar metadata only. However there are some
# exceptions. One way to test whether a json file is sidecar or data is to check for any
# matching non-json files at the same level. But that is a lot of work to do for a few
# special cases. Rather, we just list the special case suffixes here. (Honestly, using
# plain json extension for data files should be discouraged.)
# TODO: Remove this and replace where it is invoked with reference from bidsschematools
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


def get_column_names() -> enum.StrEnum:
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
    root: str | PathT,
    exclude: str | list[str] | None = None,
    maxdepth: int | None = None,
) -> Generator[PathT, None, None]:
    """Find all BIDS datasets under a root directory.

    Args:
        root: Root path to begin search.
        exclude: Glob pattern or list of patterns matching sub-directory names to
            exclude from the search.
        maxdepth: Maximum depth to search.

    Yields:
        Root paths of all BIDS datasets under `root`.
    """
    root = as_path(root)

    if isinstance(exclude, str):
        exclude = [exclude]
    elif exclude is None:
        exclude = []
    exclude = [re.compile(fnmatch.translate(pat)) for pat in exclude]

    entry_count = 1
    ds_count = 0

    if _is_bids_dataset(root):
        ds_count += 1
        yield root

    # Tuple of path, depth
    stack = [(root, 0)]

    while stack:
        top, depth = stack.pop()

        inside_bids = _is_bids_dataset(top)
        depth += 1

        for entry in top.iterdir():
            entry_count += 1

            if any(re.fullmatch(pat, entry.name) for pat in exclude):
                continue

            if _is_bids_dataset(entry):
                ds_count += 1
                yield entry

            # Checks if we should descend into this directory.
            # Check not reached final depth.
            descend = maxdepth is None or depth < maxdepth
            # Heuristic checks whether the filename looks like a (visible) directory.
            descend = descend and not (entry.suffix or entry.name.startswith("."))
            # Only descend into specific subdirectories of BIDS directories.
            descend = descend and (
                # TODO: Remove this and replace where it is invoked with reference from bidsschematools
                not inside_bids or entry.name in _BIDS_NESTED_PARENT_DIRNAMES
            )
            # Finally, check if actually a directory (which is slow so we want to
            # short-circuit as much as possible).
            if descend and entry.is_dir():
                stack.append((entry, depth))


def index_dataset(
    root: str | PathT,
    include_subjects: str | list[str] | None = None,
    show_progress: bool = False,
) -> pa.Table:
    """Index a BIDS dataset.

    Args:
        root: BIDS dataset root directory.
        include_subjects: Glob pattern or list of patterns for matching subjects to
            include in the index.
        show_progress: Show progress bar.

    Returns:
        An Arrow table index of the BIDS dataset.
    """
    root = as_path(root)

    schema = get_arrow_schema()

    dataset, _ = _get_bids_dataset(root)
    if dataset is None:
        _logger.warning(f"Path {root} is not a valid BIDS dataset directory.")
        return pa.Table.from_pylist([], schema=schema)

    subject_dirs = _find_bids_subject_dirs(root, include_subjects)
    template_dirs = _find_bids_entity_dirs(root, "template")
    entity_dirs = subject_dirs + template_dirs
    entity_dirs.sort(key=lambda p: p.name)
    if len(entity_dirs) == 0:
        _logger.warning(f"Path {root} contains no matching subject or template dirs.")
        return pa.Table.from_pylist([], schema=schema)

    tables = []
    file_count = 0
    for entity_dir in entity_dirs:
        entity_type = "template" if entity_dir in template_dirs else "subject"
        _, table = _index_bids_entity_dir(
            entity_dir, entity_type, schema=schema, dataset=dataset
        )
        tables.append(table)
        file_count += len(table)
    table = pa.concat_tables(tables).combine_chunks()
    return table


def batch_index_dataset(
    roots: list[str | PathT],
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


def _batch_index_func(root: str | PathT) -> tuple[str | None, pa.Table]:
    dataset, _ = _get_bids_dataset(root)
    table = index_dataset(root, show_progress=False)
    return dataset, table


@lru_cache()
def _get_bids_dataset(path: str | PathT) -> tuple[str | None, PathT | None]:
    """Get the BIDS dataset that the path belongs to, if any.

    Return the dataset directory name and the full dataset path. For nested derivatives
    datasets, a composite name of the form ``"ds000001/derivatives/fmriprep"`` is
    returned.

    Note that the name is extracted from the path, not the dataset description JSON.
    """
    parent = as_path(path)
    parts: list[str] = []
    scanning = False
    top_idx = 0
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


@lru_cache()
def _is_bids_dataset(path: PathT) -> bool:
    """Test if path is a BIDS dataset root directory."""
    # Quick heuristic checks.
    # BIDS datasets should not contain a file extension.
    if path.suffix:
        return False
    # Path should not be hidden.
    if path.name.startswith("."):
        return False
    # Subject dirs are not datasets.
    if _is_bids_subject_dir(path):
        return False

    # Check if contains a dataset_description.json or is a derivatives directory
    description_exists = (path / "dataset_description.json").exists()

    if description_exists:
        try:
            with open(path / "dataset_description.json") as f:
                desc = json.load(f)
            dataset_type = desc.get("DatasetType", "raw")
            if dataset_type == "raw":
                return True
            elif dataset_type == "derivative":
                return _contains_bids_entity_dirs(path, ["subject", "template"]) or any(
                    p.is_dir()
                    for p in path.iterdir()
                    # TODO: Pull these valid paths from bidsschematools
                    if p.name in {"derivatives", "code", "logs"}
                )
        except (json.JSONDecodeError, OSError):
            pass

    return False


def _contains_bids_subject_dirs(root: PathT) -> bool:
    """Check if a path contains one or more BIDS subject dirs."""
    return _contains_bids_entity_dirs(root, ["subject"])


def _find_bids_subject_dirs(
    root: PathT,
    include_subjects: str | list[str] | None = None,
) -> list[PathT]:
    """Find all BIDS subject dirs contained in a root directory.

    Note, only looks one level down. Does not find nested subject directories, e.g. in
    derivatives datasets.
    """
    return _find_bids_entity_dirs(root, "subject", include_subjects)


def _find_bids_entity_dirs(
    root: PathT,
    entity_type: str,
    include_pattern: str | list[str] | None = None,
) -> list[PathT]:
    """Find all BIDS entity dirs of a given type in a root directory."""
    paths = [path for path in root.iterdir() if _is_bids_entity_dir(path, entity_type)]

    if include_pattern:
        filtered_names = _filter_include(
            set(path.name for path in paths), include_pattern
        )
        paths = [path for path in paths if path.name in filtered_names]
    return paths


def _is_bids_entity_dir(path: PathT, entity_type: str) -> bool:
    """Check if a path is a BIDS entity directory (e.g. sub-*, tpl-*, ses-*)."""
    pattern = get_entity_pattern(entity_type)
    if not pattern:
        return False
    return bool(re.fullmatch(pattern, path.name))


def _contains_bids_entity_dirs(root: PathT, entity_types: list[str]) -> bool:
    """Check if a path contains directories matching any of the given entity types."""
    if not root.is_dir():
        return False
    return any(
        _is_bids_entity_dir(path, et) for path in root.iterdir() for et in entity_types
    )


def _is_bids_subject_dir(path: PathT) -> bool:
    """Check if a path is a BIDS subject directory."""
    return _is_bids_entity_dir(path, "subject")


def _index_bids_subject_dir(
    path: PathT,
    schema: pa.Schema | None = None,
    dataset: str | None = None,
) -> tuple[str, pa.Table]:
    """Index a BIDS subject directory and return an Arrow table."""
    return _index_bids_entity_dir(path, "subject", schema, dataset)


def _index_bids_entity_dir(
    path: PathT,
    entity_type: str = "subject",
    schema: pa.Schema | None = None,
    dataset: str | None = None,
) -> tuple[str, pa.Table]:
    """Index a BIDS entity directory and return an Arrow table."""
    root = path.parent
    root_fmt = str(root.absolute())
    if dataset is None:
        dataset, _ = _get_bids_dataset(root)
    if schema is None:
        schema = get_arrow_schema()

    _, entity_id = path.name.split("-", maxsplit=1)

    glob_pattern = get_entity_glob_pattern(entity_type)

    records = []
    # Use built-in rglob methods for CloudPath and py3.13+
    if cloudpathlib_is_available() and isinstance(path, CloudPath):
        paths = map(as_path, path.rglob(glob_pattern))
    elif sys.version_info >= (3, 13):
        paths = map(as_path, path.rglob(glob_pattern, recurse_symlinks=True))
    else:
        # Fall back to glob.glob for <py3.13
        paths = map(as_path, glob(f"{path}/**/{glob_pattern}", recursive=True))

    for p in paths:
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
    return entity_id, table


def _is_bids_file(path: PathT) -> bool:
    """Check if file is a BIDS file.

    Not very exact, but hopefully good enough.
    """
    # TODO: other checks?
    #   - skip files matching patterns in .bidsignore?

    # initial fast checks for missing extension or name without entity prefix
    if path.suffix == "" or not path.name.startswith(get_file_entity_prefixes()):
        return False

    entities = _cache_parse_bids_entities(path)
    # If we want to exclude metadata files like *_scans.tsv, we can also check for
    # datatype.
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


def _is_bids_json_sidecar(path: PathT) -> bool:
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
    # TODO: Remove this and replace where it is invoked with reference from bidsschematools
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
