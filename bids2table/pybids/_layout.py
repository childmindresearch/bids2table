"""BIDSLayout compatibility wrapper around bids2table.

Provides a PyBIDS-compatible interface for querying BIDS datasets
while leveraging bids2table's superior performance.
"""

import warnings
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from bids2table._indexing import index_dataset
from bids2table._metadata import load_bids_metadata

from ._bidsfile import BIDSFile
from ._utils import Query


class BIDSLayout:
    """PyBIDS-compatible wrapper around bids2table.

    Provides a familiar BIDSLayout interface while using bids2table's
    fast indexing and efficient querying under the hood.

    Example:
        >>> from bids2table_compat import BIDSLayout
        >>> layout = BIDSLayout('/path/to/dataset', validate=False)
        >>> subjects = layout.get_subjects()
        >>> files = layout.get(subject='01', suffix='T1w')

    Args:
        root: Path to BIDS dataset root
        derivatives: Path(s) to derivative datasets to include
        cache_path: Path to parquet cache file
            (default: {root}/.bids2table_cache.parquet)
        database_path: Legacy parameter (ignored, use cache_path instead)
        reset_database: If True, ignore cache and force re-indexing
            (useful for benchmarking)
        **kwargs: Additional arguments (currently ignored)

    Attributes:
        root: Dataset root path
        df: Pandas DataFrame with indexed BIDS files
    """

    def __init__(
        self,
        root: str | Path,
        derivatives: str | Path | list[str | Path] | None = None,
        cache_path: Path | None = None,
        database_path: Path | None = None,
        *,
        reset_database: bool = False,
        **kwargs: dict[str, Any],  # noqa: ARG002 - allow for other kwargs to be passed
    ) -> None:
        # Initialize BIDSLayout with dataset indexing.
        self.root = Path(root).absolute()
        self.reset_database = reset_database

        # Handle legacy database_path parameter
        if database_path is not None and cache_path is None:
            warnings.warn(
                "database_path is deprecated, use cache_path instead. "
                "Note: cache uses parquet format, not SQLite.",
                DeprecationWarning,
                stacklevel=2,
            )

        # Set cache path
        if cache_path is None:
            self.cache_path = self.root / ".bids2table_cache.parquet"
        else:
            self.cache_path = Path(cache_path)

        # Load or create index
        self._tab = self._load_or_create_index()

        # Handle derivatives
        if derivatives is not None:
            self._add_derivatives(derivatives)

        # Load the entity schema and create a LUT
        entity_schema = self._tab.schema
        self._entity_map = {}
        for entity in entity_schema:
            # Pull the name (used by B2T) and entity (used by pyBIDS) labels
            name = entity.metadata[b"name"]
            dname = entity.metadata.get(b"entity", name)
            # Decode them from bytestrings into real strings, and store
            # so that either the entity or shortname will return appropriately
            self._entity_map[dname.decode("utf-8")] = name.decode("utf-8")
            self._entity_map[name.decode("utf-8")] = name.decode("utf-8")

        # Flatten extra entities after
        self._flatten_extra_entities()

        # Convert to pandas DataFrame for querying
        self.df = self._tab.to_pandas(types_mapper=pd.ArrowDtype)

    def _flatten_extra_entities(self) -> None:
        """Flatten extra entities in the table."""
        if "extra_entities" not in self._tab.column_names:
            return

        idx = self._tab.schema.get_field_index("extra_entities")
        dicts = [
            dict(r) if r else {} for r in self._tab.column("extra_entities").to_pylist()
        ]
        all_keys = set().union(*dicts)

        self._tab = self._tab.remove_column(idx)
        if all_keys:
            for k in all_keys:
                self._tab = self._tab.append_column(
                    pa.field(k, pa.string()), pa.array([d.get(k) for d in dicts])
                )
                self._entity_map[k] = k

        cols = [c for c in self._tab.column_names if c not in ("root", "path")] + [
            "root",
            "path",
        ]
        self._tab = self._tab.select(cols)

    def _load_or_create_index(self) -> pa.Table:
        """Load cached index or create new one.

        Returns:
            PyArrow table with indexed BIDS files
        """
        # If reset_database is True, skip cache and force re-index
        if not self.reset_database and self.cache_path.exists():
            # Check if cache is stale (optional - could be expensive)
            # For now, trust the cache exists means it's valid
            try:
                return pq.read_table(self.cache_path)
            except Exception as e:
                warnings.warn(
                    f"Failed to load cache from {self.cache_path}: {e}. "
                    "Re-indexing dataset.",
                    UserWarning,
                    stacklevel=3,
                )

        # Create new index
        tab = index_dataset(str(self.root))

        # Save cache (unless reset_database is True, which implies benchmarking)
        if not self.reset_database:
            try:
                self.cache_path.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(tab, self.cache_path)
            except Exception as e:
                warnings.warn(
                    f"Failed to save cache to {self.cache_path}: {e}",
                    UserWarning,
                    stacklevel=3,
                )

        return tab

    def _add_derivatives(self, derivatives: str | Path | list[str | Path]) -> None:
        """Add derivative datasets to the index.

        Args:
            derivatives: Path or list of paths to derivative datasets
        """
        # Normalize to list
        if not isinstance(derivatives, list):
            derivatives = [derivatives]

        # Index each derivative dataset
        deriv_tabs = []
        for deriv_path in derivatives:
            deriv_path = Path(deriv_path)
            if not deriv_path.exists():
                warnings.warn(
                    f"Derivative path does not exist: {deriv_path}",
                    UserWarning,
                    stacklevel=3,
                )
                continue

            deriv_tab = index_dataset(str(deriv_path))
            deriv_tabs.append(deriv_tab)

        # Concatenate with main table
        if deriv_tabs:
            self._tab = pa.concat_tables([self._tab, *deriv_tabs])

    def get(
        self, return_type: str = "file", **entities: dict[str, Any]
    ) -> list[str | BIDSFile]:
        """Query files by BIDS entities.

        Args:
            return_type: 'file', 'filename', 'id', or 'dir'
            **entities: BIDS entity filters (e.g., subject='01', suffix='T1w')

        Returns:
            List of files matching query (type depends on return_type)
        """
        result_df = self._filter_df(entities)
        return self._format_results(result_df, return_type)

    def _filter_df(self, entities: dict[str, Any]) -> pd.DataFrame:
        """Apply entity filters to dataframe."""
        result_df = self.df
        for key, value in entities.items():
            key = self._entity_map.get(key, key)
            if key not in result_df.columns:
                warnings.warn(
                    f"Unknown entity '{key}' (not in dataset columns)",
                    UserWarning,
                    stacklevel=2,
                )
                continue
            result_df = self._apply_filter(result_df, key, value)
        return result_df

    def _apply_filter(
        self,
        df: pd.DataFrame,
        key: str,
        value: Any,  # noqa: ANN401
    ) -> pd.DataFrame:
        """Apply single entity filter, handling Query sentinels."""
        if value in (Query.OPTIONAL, Query.ANY):
            return df
        if value is Query.NONE:
            return df[df[key].isna()]
        if isinstance(value, list):
            return df[df[key].isin(value)]
        return df[df[key] == value]

    def _format_results(
        self, df: pd.DataFrame, return_type: str
    ) -> list[str | BIDSFile]:
        """Convert filtered dataframe to requested return type."""
        formatters = {
            "filename": lambda d: d["path"].tolist(),
            "file": lambda d: [BIDSFile(p) for p in d["path"].tolist()],
            "id": lambda d: d.index.tolist(),
            "dir": lambda d: sorted(
                d["path"].apply(lambda p: str(Path(p).parent)).unique().tolist()
            ),
        }
        if return_type not in formatters:
            raise ValueError(
                f"Unknown return_type: {return_type}. "
                "Valid options: 'file', 'filename', 'id', 'dir'"
            )
        return formatters[return_type](df)

    def get_subjects(self, **filters: dict[str, str | int]) -> list[str]:
        """Get list of unique subject IDs.

        Args:
            **filters: Optional entity filters to apply before extracting subjects

        Returns:
            Sorted list of subject IDs (without 'sub-' prefix)

        Example:
            >>> layout.get_subjects()
            ['01', '02', '03']
            >>> layout.get_subjects(suffix='bold')
            ['01', '02']  # Only subjects with BOLD data
        """
        if filters:
            # Apply filters first
            filtered_df = self.df.copy()
            for key, value in filters.items():
                key = self._entity_map.get(key, key)
                if key in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df[key] == value]
            subjects = filtered_df["sub"].dropna().unique()
        else:
            subjects = self.df["sub"].dropna().unique()

        return sorted(subjects.tolist())

    def get_sessions(
        self, subject: str | None = None, **filters: dict[str, str | int]
    ) -> list[str]:
        """Get list of unique session IDs.

        Args:
            subject: Optional subject ID to filter by
            **filters: Optional entity filters

        Returns:
            Sorted list of session IDs (without 'ses-' prefix)

        Example:
            >>> layout.get_sessions()
            ['01', '02']
            >>> layout.get_sessions(subject='01')
            ['01', '02']
        """
        result_df = self.df.copy()

        # Filter by subject if provided
        if subject is not None:
            result_df = result_df[result_df["sub"] == subject]

        # Apply additional filters
        for key, value in filters.items():
            key = self._map_entity_key(key)
            if key in result_df.columns:
                result_df = result_df[result_df[key] == value]

        sessions = result_df["ses"].dropna().unique()
        return sorted(sessions.tolist())

    def get_metadata(self, path: str) -> dict[str, Any]:
        """Load metadata from JSON sidecar(s) for a given file.

        Uses BIDS inheritance principle to merge metadata from
        dataset, subject, and session levels.

        Args:
            path: Path to BIDS file (absolute or relative to dataset root)

        Returns:
            Dictionary of metadata fields

        Example:
            >>> metadata = layout.get_metadata('sub-01/func/sub-01_task-rest_bold.nii.gz')
            >>> metadata['RepetitionTime']
            2.0
        """  # noqa: E501
        # Convert to absolute path if relative
        if not Path(path).is_absolute():
            path = str(self.root / path)

        return load_bids_metadata(path)

    def get_file(self, path: str) -> BIDSFile:
        """Get BIDSFile object for a given path.

        Args:
            path: Path to file

        Returns:
            BIDSFile object wrapping the path

        Example:
            >>> bids_file = layout.get_file('sub-01/anat/sub-01_T1w.nii.gz')
            >>> entities = bids_file.get_entities()
        """
        return BIDSFile(path)

    def get_entities(self, **filters: dict[str, str | int]) -> dict[str, list[str]]:
        """Get dictionary of all entities and their unique values.

        Args:
            **filters: Optional entity filters to apply before extracting entities

        Returns:
            Dictionary where keys are entity names and values are lists of unique values

        Example:
            >>> entities = layout.get_entities()
            >>> entities['task']
            ['rest', 'nback', 'faces']
            >>> # With filters
            >>> entities = layout.get_entities(suffix='bold')
            >>> entities['sub']
            ['01', '02']  # Only subjects with BOLD data
        """
        # Apply filters if provided
        if filters:
            filtered_df = self.df.copy()
            for key, value in filters.items():
                key = self._map_entity_key(key)
                if key in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df[key] == value]
        else:
            filtered_df = self.df

        # Extract unique values for each entity column
        entities = {}
        for evalue in self._entity_map.values():
            if evalue in filtered_df.columns:
                unique_vals = filtered_df[evalue].dropna().unique().tolist()
                if unique_vals:  # Only include if not empty
                    entities[evalue] = sorted(unique_vals)

        return entities

    def add_custom_entity(
        self,
        name: str,
        values: list[Any] | dict[str, Any] | Any,  # noqa: ANN401 - custom entities
        *,
        overwrite: bool = False,
    ) -> None:
        """Add a custom entity column to the layout.

        This is a convenience method for adding custom metadata that can be
        queried like standard BIDS entities.

        Args:
            name: Name of the custom entity (will become a column)
            values: Values to assign. Can be:
                - Single value: Applied to all rows
                - List/array: Must match length of df
                - Dict: Maps from subject/file to value
                - Function: Applied to each row via df.apply()
            overwrite: If True, overwrite existing column (default: False)

        Raises:
            ValueError: If column exists and overwrite=False

        Example:
            >>> layout = BIDSLayout('/data')
            >>> # Add constant
            >>> layout.add_custom_entity('processing_status', 'pending')
            >>> # Add from dict
            >>> qc = {'01': 'pass', '02': 'fail'}
            >>> layout.add_custom_entity('qc_grade', qc)
            >>> # Add from function
            >>> layout.add_custom_entity('modality', lambda row:
            ...     'anat' if row['datatype'] == 'anat' else 'func')
            >>> # Query it
            >>> passed = layout.get(qc_grade='pass')
        """
        if name in self.df.columns and not overwrite:
            raise ValueError(
                f"Entity '{name}' already exists. Use overwrite=True to replace."
            )

        if callable(values):
            self.df[name] = self.df.apply(values, axis=1)
        elif isinstance(values, dict):
            if values:
                first_key = next(iter(values))
                sub_col = self.df["sub"]
                map_col = (
                    sub_col if first_key in sub_col.to_numpy() else self.df["path"]
                )
            else:
                map_col = self.df["path"]
            self.df[name] = map_col.map(values)
        else:
            self.df[name] = values

        self._entity_map[name] = name

    def __repr__(self) -> str:
        """String representation of layout."""
        n_subjects = self.df["sub"].nunique()
        n_sessions = self.df["ses"].nunique()
        n_files = len(self.df)

        return (
            f"BIDSLayout(root='{self.root}', "
            f"subjects={n_subjects}, sessions={n_sessions}, files={n_files})"
        )

    def to_df(self) -> pd.DataFrame:
        """Explicit method to return converted dataframe, mirroring pybids."""
        return self.df
