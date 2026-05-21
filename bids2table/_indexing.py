"""Finding and indexing BIDS datasets.

Uses only `pathlib.Path` methods and string processing to find and filter the files.
Returns a dataset index as an Arrow table.
"""

import enum
import fnmatch
import importlib.metadata
import json
import os
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
    get_all_dataset_types,
    get_bids_entity_arrow_schema,
    get_entity_child_dirs,
    get_entity_glob_pattern,
    get_entity_name,
    get_entity_pattern,
    get_file_entity_prefixes,
    validate_bids_entities,
)
from ._logging import setup_logger
from ._pathlib import CloudPath, PathT, as_path, cloudpathlib_is_available

_IS_WINDOWS = os.name == "nt"

# Path names of BIDS dataset sub-directories that may contain nested BIDS datasets.
# Only "derivatives" is defined by the BIDS spec for this purpose; it is not
# derivable from the bidsschematools schema (no schema flag for "contains nested
# datasets"), so it remains hardcoded here.
_BIDS_NESTED_PARENT_DIRNAMES = {
    "derivatives",
}

# Typically json files are reserved for sidecar metadata only. However there are some
# exceptions. One way to test whether a json file is sidecar or data is to check for any
# matching non-json files at the same level. But that is a lot of work to do for a few
# special cases. Rather, we just list the special case suffixes here. (Honestly, using
# plain json extension for data files should be discouraged.)
# These are BIDS extension suffixes not present in the core schema, so they remain
# hardcoded here.
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
    "dataset_name": {
        "dtype": pa.string(),
        "metadata": {
            "name": "dataset_name",
            "display_name": "Dataset name",
            "description": "Name of the BIDS dataset from dataset_description.json.",
        },
    },
    "dataset_type": {
        "dtype": pa.string(),
        "metadata": {
            "name": "dataset_type",
            "display_name": "Dataset type",
            "description": "BIDS dataset type (e.g. 'raw', 'derivative').",
        },
    },
    "bids_version": {
        "dtype": pa.string(),
        "metadata": {
            "name": "bids_version",
            "display_name": "BIDS version",
            "description": "BIDS version from dataset_description.json.",
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
        index_fields["dataset_name"],
        index_fields["dataset_type"],
        index_fields["bids_version"],
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
                # Only descend into subdirectories that may contain nested BIDS datasets.
                # This is not derivable from the schema, so it remains hardcoded.
                not inside_bids or entry.name in _BIDS_NESTED_PARENT_DIRNAMES
            )
            # Finally, check if actually a directory (which is slow so we want to
            # short-circuit as much as possible).
            if descend and entry.is_dir():
                stack.append((entry, depth))


def _get_dataset_type(root: PathT) -> str:
    """Read the BIDS dataset type from dataset_description.json. Defaults to 'raw'."""
    desc_path = root / "dataset_description.json"
    if desc_path.exists():
        try:
            with open(desc_path) as f:
                desc = json.load(f)
            return desc.get("DatasetType", "raw")
        except (json.JSONDecodeError, OSError):
            pass
    return "raw"


def _match_filters(
    entities: dict[str, Any], filters: dict[str, str | list[str]] | None
) -> bool:
    """Check if parsed entities match all filter patterns.

    Returns True if no filters are set or if every filter key matches the
    corresponding entity value. A missing entity key means the file does not
    match (e.g. filtering by ``ses`` on a file without a session).

    Both entity values (``"03"``) and compound forms (``"sub-03"``) are
    accepted for entity keys, so filters work identically at directory and
    file levels.
    """
    if not filters:
        return True
    for key, pattern in filters.items():
        value = entities.get(key)
        if value is None:
            return False
        if isinstance(pattern, list):
            matches = any(_match_single(str(value), key, p) for p in pattern)
        else:
            matches = _match_single(str(value), key, pattern)
        if not matches:
            return False
    return True


def _match_single(value: str, key: str, pattern: str) -> bool:
    """Match a single entity value against a pattern, trying both the raw
    value and the compound form (``{key}-{value}``)."""
    if fnmatch.fnmatch(value, pattern):
        return True
    compound = f"{key}-{value}"
    return fnmatch.fnmatch(compound, pattern)


def index_dataset(
    root: str | PathT,
    filters: dict[str, str | list[str]] | None = None,
    include_subjects: str | list[str] | None = None,
    show_progress: bool = False,
) -> pa.Table:
    """Index a BIDS dataset.

    Args:
        root: BIDS dataset root directory.
        filters: Dict mapping entity names to glob patterns to filter indexed
            files. Keys reference directory-level entities (``sub``, ``ses``,
            ``cohort``, ``tpl``) to filter entity directories, and any parsed
            entity to filter individual files. Examples::

                {"sub": "sub-0*"}
                {"sub": ["sub-01", "sub-02"], "task": "rest"}
                {"ses": "ses-1"}

        include_subjects: Deprecated. Use ``filters={"sub": ...}`` instead.
        show_progress: Show progress bar.

    Returns:
        An Arrow table index of the BIDS dataset.
    """
    root = as_path(root)

    # Normalise filters: merge include_subjects for backward compat
    filters = dict(filters or {})
    if include_subjects is not None:
        filters["sub"] = include_subjects

    schema = get_arrow_schema()

    dataset, _ = _get_bids_dataset(root)
    if dataset is None:
        _logger.warning(f"Path {root} is not a valid BIDS dataset directory.")
        return pa.Table.from_pylist([], schema=schema)

    dataset_type = _get_dataset_type(root)

    # Read dataset metadata from dataset_description.json
    desc_path = root / "dataset_description.json"
    dataset_name = ""
    bids_version = ""
    if desc_path.exists():
        try:
            with open(desc_path) as f:
                desc = json.load(f)
            dataset_name = desc.get("Name", "")
            bids_version = desc.get("BIDSVersion", "")
        except (json.JSONDecodeError, OSError):
            pass

    record_extras = {
        "dataset_name": dataset_name,
        "dataset_type": dataset_type,
        "bids_version": bids_version,
    }

    # Discover entity dirs at the dataset root.
    root_entity_types = get_entity_child_dirs(dataset_type, "root")
    entity_dirs = _discover_entity_dirs(root, root_entity_types, filters)

    # Fallback: if no entity dirs match the detected type, try all dataset types.
    if not entity_dirs:
        seen = set(root_entity_types)
        for dtype in get_all_dataset_types():
            if dtype == dataset_type:
                continue
            for et in get_entity_child_dirs(dtype, "root"):
                if et not in seen:
                    seen.add(et)
                    dirs = _discover_entity_dirs(root, [et], filters)
                    entity_dirs.extend(dirs)
        entity_dirs.sort(key=lambda p: p.name)

    if not entity_dirs:
        _logger.warning(
            f"Path {root} contains no matching entity dirs for dataset type '{dataset_type}'."
        )
        return pa.Table.from_pylist([], schema=schema)

    # Collect all possible root entity types for dir detection
    all_entity_types = list(root_entity_types)
    for dtype in get_all_dataset_types():
        for et in get_entity_child_dirs(dtype, "root"):
            if et not in all_entity_types:
                all_entity_types.append(et)

    tables = []
    file_count = 0

    # Build the scheduling list: (dir, entity_type) pairs, expanding child
    # entity dirs when appropriate.
    schedule: list[tuple[PathT, str]] = []
    for entity_dir in entity_dirs:
        entity_type = all_entity_types[0]
        for et in all_entity_types:
            if _is_bids_entity_dir(entity_dir, et):
                entity_type = et
                break
        schedule.extend(
            _expand_entity_dir(entity_dir, entity_type, dataset_type, filters)
        )

    for entity_dir, entity_type in schedule:
        _, table = _index_bids_entity_dir(
            entity_dir,
            entity_type,
            schema=schema,
            dataset=dataset,
            filters=filters,
            record_extras=record_extras,
        )
        tables.append(table)
        file_count += len(table)

    table = pa.concat_tables(tables).combine_chunks()
    return table


def _discover_entity_dirs(
    root: PathT,
    entity_types: list[str],
    filters: dict[str, str | list[str]] | None,
) -> list[PathT]:
    """Find all entity directories under root matching the given entity types.

    Applies directory-level filters where applicable.
    """
    dirs: list[PathT] = []
    for et in entity_types:
        entity_name = get_entity_name(et)
        filter_pattern = filters.get(entity_name) if filters else None
        found = _find_bids_entity_dirs(root, et, filter_pattern)
        dirs.extend(found)
    dirs.sort(key=lambda p: p.name)
    return dirs


def _expand_entity_dir(
    entity_dir: PathT,
    entity_type: str,
    dataset_type: str,
    filters: dict[str, str | list[str]] | None,
) -> list[tuple[PathT, str]]:
    """Return (dir, entity_type) pairs to index for a root entity dir.

    Child entity dirs (e.g. session under subject) are indexed via flat rglob
    from the parent using the top-level entity prefix, so we always index the
    entity dir itself.
    """
    return [(entity_dir, entity_type)]


def batch_index_dataset(
    roots: list[str | PathT],
    max_workers: int | None = 0,
    executor_cls: type[Executor] = ProcessPoolExecutor,
    show_progress: bool = False,
    filters: dict[str, str | list[str]] | None = None,
) -> Generator[pa.Table, None, None]:
    """Index a batch of BIDS datasets.

    Args:
        roots: List of BIDS dataset root directories.
        max_workers: Number of indexing processes to run in parallel. Setting
            ``max_workers=0`` (the default) uses the main process only. Setting
            ``max_workers=None`` starts as many workers as there are available CPUs.
        executor_cls: Executor class to use for parallel indexing.
        show_progress: Show progress bar.
        filters: Dict mapping entity names to glob patterns to filter indexed
            files (see :func:`index_dataset` for details).

    Yields:
        An Arrow table index for each BIDS dataset.
    """
    file_count = 0
    for dataset, table in (
        pbar := tqdm(
            _pmap(
                partial(_batch_index_func, filters=filters),
                roots,
                max_workers,
                executor_cls=executor_cls,
            ),
            total=len(roots) if isinstance(roots, Sequence) else None,
            disable=show_progress not in {True, "dataset"},
        )
    ):
        file_count += len(table)
        pbar.set_postfix(dict(ds=dataset, N=_hfmt(file_count)), refresh=False)
        yield table


def _batch_index_func(
    root: str | PathT,
    filters: dict[str, str | list[str]] | None = None,
) -> tuple[str | None, pa.Table]:
    dataset, _ = _get_bids_dataset(root)
    table = index_dataset(root, show_progress=False, filters=filters)
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
            if dataset_type in get_all_dataset_types():
                if dataset_type == "raw":
                    return True
                entity_types = get_entity_child_dirs(dataset_type, "root")
                if entity_types:
                    return _contains_bids_entity_dirs(path, entity_types)
                return True
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
    record_extras: dict[str, Any] | None = None,
) -> tuple[str, pa.Table]:
    """Index a BIDS subject directory and return an Arrow table."""
    return _index_bids_entity_dir(path, "subject", schema, dataset, record_extras)


def _index_bids_entity_dir(
    path: PathT,
    entity_type: str = "subject",
    schema: pa.Schema | None = None,
    dataset: str | None = None,
    record_extras: dict[str, Any] | None = None,
    filters: dict[str, str | list[str]] | None = None,
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
        if _is_bids_file(p) and not _is_bidsignored(p):
            entities = _cache_parse_bids_entities(p)
            if not _match_filters(entities, filters):
                continue
            valid_entities, extra_entities = validate_bids_entities(entities)
            record = {
                "dataset": dataset,
                **valid_entities,
                "extra_entities": extra_entities,
                **(record_extras or {}),
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


def _is_bidsignored(path: PathT) -> bool:
    """Check if path matches any pattern in a .bidsignore file.

    Searches up from the file's directory to the dataset root for a .bidsignore
    file and checks if the relative path matches any of its glob patterns.
    """
    bidsignore = _find_bidsignore(path.parent)
    if bidsignore is None:
        return False
    patterns = _load_bidsignore_patterns(bidsignore)
    if not patterns:
        return False
    rel = path.relative_to(bidsignore.parent)
    return any(fnmatch.fnmatch(str(rel), pat) for pat in patterns)


@lru_cache(maxsize=None)
def _find_bidsignore(start: PathT) -> PathT | None:
    """Walk up from start looking for a .bidsignore file."""
    current = start
    root = as_path("/") if not _IS_WINDOWS else None
    while current != root:
        candidate = current / ".bidsignore"
        if candidate.exists():
            return candidate
        parent = current.parent
        if parent == current:
            break
        current = parent
    return None


@lru_cache(maxsize=None)
def _load_bidsignore_patterns(path: PathT) -> tuple[str, ...]:
    """Load .bidsignore patterns from a file."""
    try:
        text = path.read_text()
        patterns = tuple(
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.strip().startswith("#")
        )
        return patterns
    except OSError:
        return ()


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
    # These are BIDS extension suffixes not present in the core schema, so they remain
    # hardcoded here.
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
