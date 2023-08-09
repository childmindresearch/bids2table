from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import pandas as pd

from bids2table.entities import ENTITY_NAMES_TO_KEYS, BIDSEntities


class BIDSTable(pd.DataFrame):
    """
    A table representing one or more BIDS datasets.

    Each row in the table corresponds to a BIDS data file. The table is organized with
    several groups of columns:

    - **dataset** (`ds`): dataset name, relative dataset path, and the JSON dataset description
    - **entities** (`ent`): All [valid BIDS entities](https://bids-specification.readthedocs.io/en/stable/appendices/entities.html) plus an `extra_entities` dict containing any extra entities
    - **metadata** (`meta`): BIDS JSON metadata
    - **file info** (`finfo`): General file info including the full file path and last modified time

    It's recommended to create a `BIDSTable` using the main `bids2table.bids2table`
    function or use one of the constructor methods:

    - `BIDSTable.from_df`
    - `BIDSTable.from_parquet`

    ### Example

    ```python
        tab = BIDSTable.from_parquet("dataset/index.b2t")
        tab = tab.sort_entities(["dataset", "sub", "ses", "task", "run"])
        tab = (
            tab
            .filter("dataset", "ds001")
            .filter("sub", items=["04", "06"])
            .filter("RepetitionTime", 2.0)
        )
        # Get list of BIDSFiles
        files = tab.files
    ```
    """

    @cached_property
    def nested(self) -> pd.DataFrame:
        """
        A copy of the table with column labels organized in a nested
        [`MultiIndex`](https://pandas.pydata.org/docs/user_guide/advanced.html#hierarchical-indexing-multiindex).
        """
        # Cast back to the base class since we no longer have the full BIDS table
        # structure.
        return pd.DataFrame(flat_to_multi_columns(self))

    @cached_property
    def ds(self) -> pd.DataFrame:
        """
        The dataset (`ds`) subtable.
        """
        return self.nested["ds"]

    @cached_property
    def ent(self) -> pd.DataFrame:
        """
        The entities (`ent`) subtable.
        """
        return self.nested["ent"]

    @cached_property
    def meta(self) -> pd.DataFrame:
        """
        The metadata (`meta`) subtable.
        """
        return self.nested["meta"]

    @cached_property
    def finfo(self) -> pd.DataFrame:
        """
        The file info (`finfo`) subtable.
        """
        return self.nested["finfo"]

    @cached_property
    def flat(self) -> pd.DataFrame:
        """
        A copy of the table with subtable prefixes e.g. `ds__`, `ent__` removed.
        """
        return self.nested.droplevel(0, axis=1)

    @cached_property
    def flat_meta(self) -> pd.DataFrame:
        """
        A table of flattened JSON metadata where each metadata field is converted to its
        own column, with nested levels separated by `'.'`.

        See also:

        - [`pd.json_normalize`](https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html):
        more general function in pandas.
        """
        # Need to replace None with empty dict for max_level=0 to work.
        metadata = pd.json_normalize(
            self["meta__json"].map(lambda v: v or {}), max_level=0
        )
        metadata.index = self.index
        return metadata

    @cached_property
    def files(self) -> List["BIDSFile"]:
        """
        Convert the table to a list of structured `BIDSFile`s.
        """

        def to_dict(val):
            if pd.isna(val):
                return {}
            return dict(val)

        return [
            BIDSFile(
                dataset=row["ds"]["dataset"],
                root=Path(row["ds"]["dataset_path"]),
                path=Path(row["finfo"]["file_path"]),
                entities=BIDSEntities.from_dict(row["ent"]),
                metadata=to_dict(row["meta"]["json"]),
            )
            for _, row in self.nested.iterrows()
        ]

    @cached_property
    def datatypes(self) -> List[str]:
        """
        Get all datatypes present in the table.
        """
        return self.ent["datatype"].unique().tolist()

    @cached_property
    def modalities(self) -> List[str]:
        """
        Get all modalities present in the table.
        """
        # TODO: Is this the right way to get the modality
        return self.ent["mod"].unique().tolist()

    @cached_property
    def subjects(self) -> List[str]:
        """
        Get all unique subjects in the table.
        """
        return self.ent["sub"].unique().tolist()

    @cached_property
    def entities(self) -> List[str]:
        """
        Get all entity keys with at least one non-NA entry in the table.
        """
        entities = self.ent.dropna(axis=1, how="all").columns.tolist()
        special = set(BIDSEntities.special())
        return [key for key in entities if key not in special]

    def filter(
        self,
        key: str,
        value: Optional[Any] = None,
        *,
        items: Optional[Iterable[Any]] = None,
        contains: Optional[str] = None,
        regex: Optional[str] = None,
        func: Optional[Callable[[Any], bool]] = None,
    ) -> "BIDSTable":
        """
        Filter the rows of the table.

        Args:
            key: Column to filter. Can be a metadata field, BIDS entity name, or any
                unprefixed column label in the `flat` table.
            value: Keep rows with this exact value.
            items: Keep rows whose value is in `items`.
            contains: Keep rows whose value contains `contains` (string only).
            regex: Keep rows whose value matches `regex` (string only).
            func: Apply an arbitrary function and keep values that evaluate to `True`.

        Returns:
            A filtered BIDS table.

        Example::
            filtered = (
                tab
                .filter("dataset", "ds001")
                .filter("sub", items=["04", "06"])
                .filter("RepetitionTime", 2.0)
            )
        """
        # NOTE: Should be careful about reinventing a new style of query API. There are
        # some obvious things this can't do:
        #   - comparison operators <, >, <=, >=
        #   - negation
        #   - combining filters with 'or' instead of 'and'
        # At the bottom of this rabbit hole are more general query interfaces like those
        # already implemented in pandas, duckdb, polars. The goal should be not to
        # create a new one, but to make the 95% of use cases as easy as possible, and
        # empower users to interact with the underlying table using their more powerful
        # tool of choice if necessary.
        if sum(k is not None for k in [value, items, contains, regex, func]) != 1:
            raise ValueError(
                "Exactly one of value, items, contains, regex, or func must not be None"
            )

        try:
            # JSON metadata field
            # NOTE: Assuming all JSON metadata fields are uppercase.
            if key[:1].isupper():
                col = self.flat_meta[key]
            # Long name entity
            elif key in ENTITY_NAMES_TO_KEYS:
                col = self.ent[ENTITY_NAMES_TO_KEYS[key]]
            # Any other unprefixed column
            else:
                col = self.flat[key]
        except KeyError as exc:
            raise KeyError(
                f"Invalid key {key}; expected a valid BIDS entity or metadata field "
                "present in the dataset"
            ) from exc

        if value is not None:
            mask = col == value
        elif items is not None:
            mask = col.isin(items)
        elif contains is not None:
            mask = col.str.contains(contains)
        elif regex is not None:
            mask = col.str.match(regex)
        else:
            mask = col.apply(func)
        mask = mask.fillna(False).astype(bool)

        return self.loc[mask]

    def filter_multi(self, **filters) -> "BIDSTable":
        """
        Apply multiple filters to the table sequentially.

        Args:
            filters: A mapping of column labels to queries. Each query can either be
                a single value for an exact equality check or a `dict` for a more
                complex query, e.g. `{"items": [1, 2, 3]}`, that's passed through to
                `filter`.

        Returns:
            A filtered BIDS table.

        Example::
            filtered = tab.filter_multi(
                dataset="ds001"
                sub={"items": ["04", "06"]},
                RepetitionTime=2.5,
            )
        """
        tab = self.copy(deep=False)

        for k, query in filters.items():
            if not isinstance(query, dict):
                query = {"value": query}
            tab = tab.filter(k, **query)
        return tab

    def sort_entities(
        self, by: Union[str, List[str]], inplace: bool = False
    ) -> "BIDSTable":
        """
        Sort the values of the table by entities.

        Args:
            by: label or list of labels. Can be `"dataset"` or a short or long entity
                name.
            inplace: sort the table in place

        Returns:
            A sorted BIDS table.
        """
        if isinstance(by, str):
            by = [by]

        # TODO: what about sorting by other columns, e.g. file_path?
        def add_prefix(k: str):
            if k == "dataset":
                k = f"ds__{k}"
            elif k in ENTITY_NAMES_TO_KEYS:
                k = f"ent__{ENTITY_NAMES_TO_KEYS[k]}"
            else:
                k = f"ent__{k}"
            return k

        by = [add_prefix(k) for k in by]
        out = self.sort_values(by, inplace=inplace)
        if inplace:
            return self
        return out

    @classmethod
    def from_df(cls, df: pd.DataFrame) -> "BIDSTable":
        """
        Create a BIDS table from a pandas `DataFrame` generated by `bids2table`.
        """
        return cls(df)

    @classmethod
    def from_parquet(cls, path: Path) -> "BIDSTable":
        """
        Read a BIDS table from a Parquet file or dataset directory generated by
        `bids2table`.
        """
        df = pd.read_parquet(path)
        return cls.from_df(df)

    @property
    def _constructor(self):
        # Makes sure that dataframe slices return a subclass instance
        # https://pandas.pydata.org/docs/development/extending.html#override-constructor-properties
        return BIDSTable


@dataclass
class BIDSFile:
    """
    A structured BIDS file.
    """

    dataset: str
    """Parent BIDS dataset."""
    root: Path
    """Path to parent dataset."""
    path: Path
    """File path."""
    entities: BIDSEntities
    """BIDS entities."""
    metadata: Dict[str, Any] = field(default_factory=dict)
    """BIDS JSON metadata."""

    @property
    def relative_path(self) -> Path:
        """
        The file path relative to the dataset root.
        """
        return self.path.relative_to(self.root)


def flat_to_multi_columns(df: pd.DataFrame, sep: str = "__") -> pd.DataFrame:
    """
    Convert a flat column index to a MultiIndex by splitting on `sep`.
    """
    # Do nothing if already a MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        return df

    # Do nothing for empty df
    # TODO: It would probably be better if the header was initialized even if there are
    # no records.
    if len(df.columns) == 0:
        return df

    split_columns = [col.split(sep) for col in df.columns]
    num_levels = max(map(len, split_columns))

    def _pad_col(col):
        return tuple((num_levels - len(col)) * [None] + col)

    df = df.copy(deep=False)
    df.columns = pd.MultiIndex.from_tuples(map(_pad_col, split_columns))
    return df


def multi_to_flat_columns(df: pd.DataFrame, sep: str = "__") -> pd.DataFrame:
    """
    Convert a column MultiIndex to a flat index by joining on `sep`.
    """
    # Do nothing if already flat
    if not isinstance(df.columns, pd.MultiIndex):
        return df

    columns = df.columns.to_flat_index()
    join_columns = [sep.join(col) for col in columns]

    df = df.copy(deep=False)
    df.columns = pd.Index(join_columns)
    return df


def join_bids_path(
    row: Union[pd.Series, Dict[str, Any]],
    prefix: Optional[Union[str, Path]] = None,
    valid_only: bool = True,
) -> Path:
    """
    Reconstruct a BIDS path from a table row or entities dict.

    Args:
        row: row from a `BIDSTable` or `BIDSTable.ent` subtable.
        prefix: output file prefix path.
        valid_only: only include valid BIDS entities.

    Example::

        tab = BIDSTable.from_parquet("dataset/index.b2t")
        paths = tab.apply(join_bids_path, axis=1)
    """
    # Filter in case input is a row from the raw dataframe and not the entities group.
    row = _filter_row(row, group="ent")
    entities = BIDSEntities.from_dict(row, valid_only=valid_only)
    path = entities.to_path(prefix=prefix, valid_only=valid_only)
    return path


def _filter_row(
    row: Union[pd.Series, Dict[str, Any]], group: str, sep: str = "__"
) -> Dict[str, Any]:
    """
    Filter a table row for fields from a particular group. Keeps all fields without a
    group prefix.
    """
    prefix = f"{group}{sep}"
    return {
        _removeprefix(k, prefix): v
        for k, v in row.items()
        if k.startswith(prefix) or sep not in k
    }


def _removeprefix(s: str, prefix: str) -> str:
    # same as str.removeprefix(), which was introduced in 3.9
    if s.startswith(prefix):
        s = s[len(prefix) :]
    return s
