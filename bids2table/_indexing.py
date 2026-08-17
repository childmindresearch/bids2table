"""Finding and indexing BIDS datasets.

Uses only `pathlib.Path` methods and string processing to find and filter the files.
Returns a dataset index as an Arrow table.
"""

import enum
import fnmatch
import json
import re
import sys
import warnings
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import cache, lru_cache, partial
from glob import glob
from typing import Any

import pyarrow as pa
from tqdm import tqdm

from bids2table._entities import (
    _build_datatype_pattern,
    _cache_parse_bids_entities,
    _pyarrow_validate_entities,
    get_file_entity_prefixes,
    get_root_entity_types,
)
from bids2table._logging import setup_logger
from bids2table._pathlib import CloudPath, PathT, as_path, cloudpathlib_is_available
from bids2table._schema import (
    BIDSSchemaAdapter,
    SchemaSpec,
    entity_arrow_schema,
    get_entity_directory_order,
    get_json_data_suffixes,
    load_bids_schema,
)
from bids2table._version import version

# Path names of BIDS dataset sub-directories that may contain nested BIDS datasets.
# Other candidates to consider including:
#   - sourcedata
#   - code
_BIDS_NESTED_PARENT_DIRNAMES = {
    "derivatives",
}


def _compile_entity_dir_pattern(
    prefixes: tuple[str, ...], adapter: BIDSSchemaAdapter
) -> re.Pattern[str]:
    """Compile a regex matching any entity directory name in the given prefixes.

    Args:
        prefixes: Entity prefix strings (e.g., ``("sub", "tpl")``).
        adapter: A ``BIDSSchemaAdapter`` with format patterns.

    Returns:
        A compiled regex matching ``prefix-value`` for any of the prefixes.
    """
    alternates = []
    for prefix in prefixes:
        for cfg in adapter.entity_schema.values():
            if cfg.get("name") == prefix:
                fmt = cfg.get("format", "special")
                char_class = adapter.format_patterns.get(
                    fmt, adapter.format_patterns["special"]
                )
                alternates.append(f"{prefix}-{char_class}")
                break
        else:
            alternates.append(f"{prefix}-[a-zA-Z0-9]+")
    return re.compile("|".join(f"({a})" for a in alternates))


# Configs for index arrow fields to add to the entity schema (defined elsewhere).
_DESC_FIELD_MAP: dict[str, str] = {
    "dataset_name": "Name",
    "dataset_type": "DatasetType",
    "bids_version": "BIDSVersion",
}
_INDEX_ARROW_FIELDS = {
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
            "description": "Dataset type (e.g. 'raw', 'derivative', 'study').",
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


def clear_schema_caches() -> None:
    """Clear LRU caches that depend on the BIDS schema.

    Call after :func:`bids2table.set_bids_schema` to avoid stale results from
    previously cached schema-dependent function calls.
    """
    _get_bids_dataset.cache_clear()
    _is_bids_dataset.cache_clear()
    _build_datatype_pattern.cache_clear()
    _cache_parse_bids_entities.cache_clear()


def get_arrow_schema(*, schema: SchemaSpec | BIDSSchemaAdapter = None) -> pa.Schema:
    """Get Arrow schema of the BIDS dataset index."""
    adapter = (
        schema if isinstance(schema, BIDSSchemaAdapter) else load_bids_schema(schema)
    )
    entity_schema = entity_arrow_schema(adapter)
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
        b"bids2table_version": version.encode(),
    }
    return pa.schema(fields, metadata=metadata)


def get_column_names(*, schema: SchemaSpec = None) -> type[enum.StrEnum]:
    """Get an enum of the BIDS index columns."""
    # TODO: It might be nice if the column names were statically available. One option
    # would be to generate a static _schema.py module at install time (similar to how
    # _version.py is generated) which defines the static default schema and column
    # names.
    arrow_schema = get_arrow_schema(schema=schema)
    items = []
    for f in arrow_schema:
        name = f.metadata[b"name"].decode()
        items.append((name, name))

    BIDSColumn = enum.StrEnum("BIDSColumn", items)  # noqa: N806 - class type
    BIDSColumn.__doc__ = "Enum of BIDS index column names."
    return BIDSColumn


def find_bids_datasets(
    root: str | PathT,
    exclude: str | list[str] | None = None,
    maxdepth: int | None = None,
    *,
    schema: SchemaSpec = None,
) -> Generator[PathT, None, None]:
    """Find all BIDS datasets under a root directory.

    Args:
        root: Root path to begin search.
        exclude: Glob pattern or list of patterns matching sub-directory names to
            exclude from the search.
        maxdepth: Maximum depth to search.
        schema: BIDS schema specification to use. If ``None``, uses the bundled
            default schema.

    Yields:
        Root paths of all BIDS datasets under `root`.
    """
    root = as_path(root)

    if isinstance(exclude, str):
        exclude = [exclude]
    elif exclude is None:
        exclude = []
    exclude_patterns = [re.compile(fnmatch.translate(pat)) for pat in exclude]

    entry_count = 1
    ds_count = 0

    if _is_bids_dataset(root, schema):
        ds_count += 1
        yield root

    # Tuple of path, depth
    stack = [(root, 0)]

    while stack:
        top, depth = stack.pop()

        inside_bids = _is_bids_dataset(top, schema)
        depth += 1

        for entry in top.iterdir():
            entry_count += 1

            if any(re.fullmatch(pat, entry.name) for pat in exclude_patterns):
                continue

            if _is_bids_dataset(entry, schema):
                ds_count += 1
                yield entry

            # Checks if we should descend into this directory.
            # Check not reached final depth.
            descend = maxdepth is None or depth < maxdepth
            # Heuristic checks whether the filename looks like a (visible) directory.
            descend = descend and not (entry.suffix or entry.name.startswith("."))
            # Only descend into specific subdirectories of BIDS directories.
            descend = descend and (
                not inside_bids or entry.name in _BIDS_NESTED_PARENT_DIRNAMES
            )
            # Finally, check if actually a directory (which is slow so we want to
            # short-circuit as much as possible).
            if descend and entry.is_dir():
                stack.append((entry, depth))


def index_dataset(
    root: str | PathT,
    include_subjects: str | list[str] | None = None,
    *,
    filters: dict[str, str | list[str]] | None = None,
    schema: SchemaSpec = None,
) -> pa.Table:
    """Index a BIDS dataset.

    Args:
        root: BIDS dataset root directory.
        include_subjects: Glob pattern or list of patterns for matching subjects to
            include in the index. .. deprecated:: Use ``filters={'sub': ...}`` instead.
        filters: Dict mapping entity keys to glob patterns or lists of patterns.
        schema: BIDS schema specification to use. If ``None``, uses the bundled
            default schema.

    Returns:
        An Arrow table index of the BIDS dataset.
    """
    root = as_path(root)

    if include_subjects is not None:
        warnings.warn(
            "include_subjects is deprecated; use filters={'sub': ...} instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if filters is None:
            filters = {}
        filters["sub"] = include_subjects

    adapter = load_bids_schema(schema)
    arrow_schema = get_arrow_schema(schema=adapter)

    dataset, _ = _get_bids_dataset(root)
    if dataset is None:
        _logger.warning(f"Path {root} is not a valid BIDS dataset directory.")
        return pa.Table.from_pylist([], schema=arrow_schema)

    entity_dirs = _resolve_entity_dirs(root, adapter=adapter, filters=filters)
    entity_dirs = sorted(entity_dirs, key=lambda p: p.name)
    if len(entity_dirs) == 0:
        _logger.warning(f"Path {root} contains no matching entity dirs.")
        return pa.Table.from_pylist([], schema=arrow_schema)

    tables = []
    file_count = 0
    for entity_dir in entity_dirs:
        prefix = entity_dir.name.split("-")[0]
        _, table = _index_bids_entity_dir(
            entity_dir, prefix, adapter, arrow_schema, dataset, filters
        )
        tables.append(table)
        file_count += len(table)
    return pa.concat_tables(tables).combine_chunks()


def batch_index_dataset(
    roots: Sequence[str | PathT],
    max_workers: int | None = 0,
    executor_cls: type[ProcessPoolExecutor | ThreadPoolExecutor] = ProcessPoolExecutor,
    *,
    filters: dict[str, str | list[str]] | None = None,
    show_progress: bool = False,
    schema: SchemaSpec = None,
) -> Generator[pa.Table, None, None]:
    """Index a batch of BIDS datasets.

    Args:
        roots: List of BIDS dataset root directories.
        max_workers: Number of indexing processes to run in parallel. Setting
            `max_workers=0` (the default) uses the main process only. Setting
            `max_workers=None` starts as many workers as there are available CPUs. See
            `concurrent.futures.ProcessPoolExecutor` for details.
        executor_cls: Executor class to use for parallel indexing.
        filters: Dict mapping entity keys to glob patterns or lists of patterns.
        show_progress: Show progress bar.
        schema: Optional `SchemaSpec`. `None` uses the default BIDS schema.

    Yields:
        An Arrow table index for each BIDS dataset.
    """
    func = partial(_batch_index_func, filters=filters, schema=schema)
    file_count = 0
    for dataset, table in (
        pbar := tqdm(
            _pmap(func, roots, max_workers, executor_cls=executor_cls),
            total=len(roots) if isinstance(roots, Sequence) else None,
            disable=show_progress not in {True, "dataset"},
        )
    ):
        file_count += len(table)
        pbar.set_postfix({"ds": dataset, "N": _hfmt(file_count)}, refresh=False)
        yield table


def _batch_index_func(
    root: str | PathT,
    *,
    filters: dict[str, str | list[str]] | None = None,
    schema: SchemaSpec = None,
) -> tuple[str | None, pa.Table]:
    dataset, _ = _get_bids_dataset(root)
    table = index_dataset(root, filters=filters, schema=schema)
    return dataset, table


@lru_cache
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


@cache
def _read_dataset_description(path: PathT) -> dict[str, Any]:
    """Read and parse ``dataset_description.json`` from a dataset root.

    Returns an empty dict if the file does not exist or cannot be parsed.
    Cached keyed by the absolute path.
    """
    desc_path = as_path(path).absolute() / "dataset_description.json"
    if desc_path.exists():
        try:
            with desc_path.open() as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _get_dataset_type(root: PathT) -> str:
    """Determine the dataset type for a root directory.

    Reads ``DatasetType`` from ``dataset_description.json`` if present.
    Falls back to ``"derivative"`` if the root sits inside a nested parent
    directory (e.g. ``derivatives/``). Defaults to ``"raw"``.

    Args:
        root: BIDS dataset root directory.

    Returns:
        A dataset type string (``"raw"``, ``"derivative"``, ``"study"``, etc.).
    """
    desc = _read_dataset_description(root)
    if "DatasetType" in desc:
        return desc["DatasetType"]

    # Check if inside a nested parent directory.
    for ancestor in root.parents:
        if ancestor.name in _BIDS_NESTED_PARENT_DIRNAMES:
            return "derivative"

    return "raw"


@lru_cache
def _is_bids_dataset(path: PathT, schema: SchemaSpec = None) -> bool:
    """Test if path is a BIDS dataset root directory.

    Args:
        path: Path to check.
        schema: Optional schema specification. Defaults to the bundled schema.

    Uses the default BIDS schema for heuristic detection when ``schema=None``.
    ``load_bids_schema()`` is ``@lru_cache``'d so the cost is negligible after
    the first call.
    """
    # Quick heuristic checks.
    # BIDS datasets should not contain a file extension.
    if path.suffix:
        return False
    # Path should not be hidden.
    if path.name.startswith("."):
        return False

    # Derive entity info from the schema.
    adapter = load_bids_schema(schema)
    root_prefixes = get_root_entity_types(adapter)
    pattern = _compile_entity_dir_pattern(root_prefixes, adapter)

    # Entity dirs (sub-*, tpl-*, etc.) are not datasets themselves.
    if pattern.fullmatch(path.name):
        return False

    # Check for dataset_description.json or any entity directories.
    description_exists = (path / "dataset_description.json").exists()
    return description_exists or _contains_bids_entity_dirs(
        path, root_prefixes, pattern
    )


def _contains_bids_entity_dirs(
    root: PathT,
    prefixes: tuple[str, ...],
    pattern: re.Pattern[str],
) -> bool:
    """Check if a path contains one or more BIDS entity dirs.

    Args:
        root: Directory to check.
        prefixes: Entity prefixes to look for (e.g., ``("sub", "tpl")``).
        pattern: Compiled regex to validate directory names.

    Returns:
        ``True`` if any matching entity directory is found.
    """
    for prefix in prefixes:
        for path in root.glob(f"{prefix}-*"):
            if pattern.fullmatch(path.name):
                return True
    return False


def _find_bids_entity_dirs(
    root: PathT,
    prefixes: tuple[str, ...],
    pattern: re.Pattern[str],
    include_pattern: str | list[str] | None = None,
) -> list[PathT]:
    """Find all BIDS entity dirs contained in a root directory.

    Args:
        root: Dataset root directory.
        prefixes: Entity prefixes to search for (e.g., ``("sub", "tpl")``).
        pattern: Compiled regex to validate directory names.
        include_pattern: Glob pattern or list of patterns to filter results by.

    Returns:
        List of matching entity directory paths.
    """
    paths = [
        path
        for prefix in prefixes
        for path in root.glob(f"{prefix}-*")
        if pattern.fullmatch(path.name)
    ]

    if include_pattern:
        if isinstance(include_pattern, str):
            include_pattern = [include_pattern]
        entity_names = {path.name for path in paths}
        filtered_names = {
            name
            for name in entity_names
            if any(_match_entity_name(name, pat) for pat in include_pattern)
        }
        paths = [path for path in paths if path.name in filtered_names]
    return paths


def _resolve_entity_dirs(
    root: PathT,
    *,
    adapter: BIDSSchemaAdapter,
    filters: dict[str, str | list[str]] | None = None,
) -> list[PathT]:
    """Resolve entity directories for a BIDS dataset root.

    Tries the primary root entity prefixes first (e.g., ``"sub"``, ``"tpl"``).
    If none are found, falls back to searching all known entity prefixes.

    Args:
        root: Dataset root directory.
        adapter: BIDS schema adapter to derive entity prefixes and patterns from.
        filters: Optional entity filter dict.

    Returns:
        List of matching entity directory paths.
    """
    # Derive entity prefixes and pattern from the adapter.
    root_prefixes = get_root_entity_types(adapter)
    entity_prefixes = tuple(
        frozenset(get_entity_directory_order(adapter))
        | frozenset(get_file_entity_prefixes(adapter))
    )
    pattern = _compile_entity_dir_pattern(root_prefixes, adapter)

    # Extract include pattern for the primary entity key from filters.
    include_pattern = None
    if filters:
        for prefix in root_prefixes:
            if prefix in filters:
                include_pattern = filters[prefix]
                break

    # Try primary root entity prefixes.
    dirs = _find_bids_entity_dirs(root, root_prefixes, pattern, include_pattern)
    if dirs:
        return dirs

    # Fallback: try all known entity prefixes.
    return _find_bids_entity_dirs(root, entity_prefixes, pattern, include_pattern)


def _match_entity_name(name: str, pattern: str) -> bool:
    """Match an entity directory name against a glob pattern.

    Tries both the bare value (e.g. ``"01"``) and the compound form
    (e.g. ``"sub-01"``) so that users can specify either style.

    Args:
        name: Entity directory name (e.g., ``"sub-01"``).
        pattern: Glob pattern to match against.

    Returns:
        ``True`` if the name matches the pattern.
    """
    _, _, value = name.partition("-")
    return fnmatch.fnmatch(value, pattern) or fnmatch.fnmatch(name, pattern)


def _is_bids_entity_dir(path: PathT, pattern: re.Pattern[str]) -> bool:
    """Check if a path is a BIDS entity directory.

    Args:
        path: Path to check.
        pattern: Compiled regex to validate directory names.

    Returns:
        ``True`` if the path name matches the entity directory pattern.
    """
    # Fast: only check the name, not whether it's actually a directory.
    return bool(pattern.fullmatch(path.name))


def _index_bids_entity_dir(
    path: PathT,
    entity_prefix: str,
    adapter: BIDSSchemaAdapter,
    schema: pa.Schema | None = None,
    dataset: str | None = None,
    filters: dict[str, str | list[str]] | None = None,
) -> tuple[str, pa.Table]:
    """Index a BIDS entity directory and return an Arrow table.

    Args:
        path: Entity directory path (e.g., ``sub-01`` or ``tpl-MNI152``).
        entity_prefix: The entity prefix (e.g., ``"sub"``).
        adapter: BIDS schema adapter for entity prefix validation.
        schema: Arrow schema for the index table.
        dataset: Dataset name string.
        filters: Dict mapping entity keys to glob patterns.

    Returns:
        Tuple of (entity value, Arrow table).
    """
    root = path.parent
    root_fmt = str(root.absolute())
    if dataset is None:
        dataset, _ = _get_bids_dataset(root)
    if schema is None:
        schema = get_arrow_schema()

    _, entity_value = path.name.split("-", maxsplit=1)

    # Read dataset description for new index columns.
    desc = _read_dataset_description(root)

    # Build filter dict excluding the directory entity (handled at directory level).
    file_filters = {k: v for k, v in (filters or {}).items() if k != entity_prefix}

    records = []
    # Use built-in rglob methods for CloudPath and py3.13+
    if cloudpathlib_is_available() and isinstance(path, CloudPath):
        paths = map(as_path, path.rglob(f"{entity_prefix}-*"))
    elif sys.version_info >= (3, 13):
        paths = map(as_path, path.rglob(f"{entity_prefix}-*", recurse_symlinks=True))
    else:
        # Fall back to glob.glob for <py3.13
        paths = map(
            as_path,
            glob(f"{path}/**/{entity_prefix}-*", recursive=True),  # noqa: PTH207
        )

    for p in paths:
        if not _is_bids_file(p, adapter=adapter) or _is_bidsignored(p, root):
            continue
        entities = _cache_parse_bids_entities(p, adapter)
        valid_entities, extra_entities = _pyarrow_validate_entities(
            entities, pa_schema=schema
        )
        # Skip files that don't match filters.
        if file_filters and not _match_filters(valid_entities, file_filters):
            continue
        record = {
            "dataset": dataset,
            **{k: desc[v] for k, v in _DESC_FIELD_MAP.items() if v in desc},
            **valid_entities,
            "extra_entities": extra_entities,
            "root": root_fmt,
            "path": str(
                p.relative_to(root)  # ty:ignore[invalid-argument-type]
            ),
        }
        records.append(record)

    table = pa.Table.from_pylist(records, schema=schema)
    return entity_value, table


@lru_cache
def _load_bidsignore_patterns(root: str) -> tuple[str, ...]:
    """Load glob patterns from ``.bidsignore`` at the dataset root.

    Note:
        Only the root-level ``.bidsignore`` is read; nested ``.bidsignore``
        files in subdirectories are not searched for.

    Args:
        root: Dataset root directory (as a string for cache-key stability).

    Returns:
        A tuple of glob patterns. Empty tuple if ``.bidsignore`` is absent.
    """
    bidsignore = as_path(root) / ".bidsignore"
    if not bidsignore.is_file():
        return ()
    patterns = []
    for line in bidsignore.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        patterns.append(line)
    return tuple(patterns)


def _is_bidsignored(path: PathT, root: PathT) -> bool:
    """Check if a path matches any ``.bidsignore`` pattern.

    Patterns are matched against both the full relative path and the bare
    filename, so patterns like ``sub-A01_*bold*`` work even when the file
    is nested in a datatype directory.

    Args:
        path: File path to check.
        root: Dataset root directory.

    Returns:
        True if the path should be ignored, False otherwise.
    """
    patterns = _load_bidsignore_patterns(str(root))
    if not patterns:
        return False
    rel_path = str(path.relative_to(root))  # ty:ignore[invalid-argument-type]
    filename = path.name
    return any(
        fnmatch.fnmatch(rel_path, pattern) or fnmatch.fnmatch(filename, pattern)
        for pattern in patterns
    )


def _is_bids_file(path: PathT, adapter: BIDSSchemaAdapter) -> bool:
    """Check if file is a BIDS file.

    Args:
        path: File path to check.
        adapter: BIDS schema adapter for entity prefix and sidecar validation.

    Not very exact, but hopefully good enough.
    """
    # Initial fast checks.
    if path.suffix == "":
        return False

    # File name must start with a known BIDS entity prefix.
    prefixes = tuple(
        frozenset(get_entity_directory_order(adapter))
        | frozenset(get_file_entity_prefixes(adapter))
    )
    if not path.name.startswith(tuple(f"{p}-" for p in prefixes)):
        return False

    entities = _cache_parse_bids_entities(path, adapter)
    # If we want to exclude metadata files like *_scans.tsv, we can also check for
    # datatype.
    if not (entities.get("suffix") and entities.get("ext")):
        return False

    if _is_bids_json_sidecar(path, adapter):
        return False

    # Very special case for directories treated as BIDS "files"
    # (e.g. .ds or .ome.zarr).
    return not _is_bids_file(path.parent, adapter)


def _is_bids_json_sidecar(path: PathT, adapter: BIDSSchemaAdapter) -> bool:
    """Quick check if a file is a JSON sidecar.

    Args:
        path: File path to check.
        adapter: BIDS schema adapter for exception suffix validation.
    """
    # Quick check if path suffix is not json.
    if path.suffix != ".json":
        return False

    # Other checks require entities.
    entities = _cache_parse_bids_entities(path, adapter)

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
    exception_suffixes = get_json_data_suffixes(adapter)
    return not (suffix is None or suffix in exception_suffixes)


def _pmap(
    func: Callable,
    iterable: Iterable[Any],
    max_workers: int | None = 0,
    chunksize: int = 1,
    executor_cls: type[ProcessPoolExecutor | ThreadPoolExecutor] = ProcessPoolExecutor,
) -> Iterator[Any]:
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
    patterns: str | Iterable[str],
) -> set[str]:
    """Filter names including those that match a glob pattern or list of patterns."""
    names = set(names)
    matching_names = _multi_pattern_filter(names, patterns)
    names.intersection_update(matching_names)
    return names


def _filter_exclude(
    names: Iterable[str],
    patterns: str | Iterable[str],
) -> set[str]:
    """Filter names excluding those that match a glob pattern or list of patterns."""
    names = set(names)
    matching_names = _multi_pattern_filter(names, patterns)
    names.difference_update(matching_names)
    return names


def _multi_pattern_filter(
    names: Iterable[str], patterns: str | Iterable[str]
) -> set[str]:
    """Filter names matching any of a list of patterns."""
    if isinstance(patterns, str):
        patterns = [patterns]
    matching_names = set()
    for pat in patterns:
        matching_names.update(fnmatch.filter(names, pat))
    return matching_names


def _match_single(value: str | int, key: str, pattern: str) -> bool:
    """Match a single entity value against a glob pattern.

    Tries both the bare value (e.g. ``"01"``) and the compound form
    (e.g. ``"sub-01"``) against the pattern so that users can specify
    either style in ``--filter``.
    """
    str_value = str(value)
    return fnmatch.fnmatch(str_value, pattern) or fnmatch.fnmatch(
        f"{key}-{str_value}", pattern
    )


def _match_filters(
    entities: dict[str, str | int], filters: dict[str, str | list[str]]
) -> bool:
    """Check if an entities dict matches all filters.

    Returns ``True`` if every filter key has a matching value in
    ``entities``. A filter value can be a single glob pattern or a list
    of patterns — matching any one is sufficient per key. Missing keys
    in ``entities`` cause the filter to fail.
    """
    for key, patterns in filters.items():
        if key not in entities:
            return False
        value = entities[key]
        if isinstance(patterns, str):
            patterns = [patterns]
        if not any(_match_single(value, key, p) for p in patterns):
            return False
    return True


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
