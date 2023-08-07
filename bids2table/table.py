from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

import pandas as pd

from bids2table.entities import ENTITY_NAMES_TO_KEYS, BIDSEntities
from bids2table.helpers import flat_to_multi_columns


@dataclass
class BIDSFile:
    dataset: str
    root: Path
    path: Path
    entities: BIDSEntities
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def relative_path(self) -> Path:
        return self.path.relative_to(self.root)


class BIDSTable(pd.DataFrame):
    """
    A BIDS table.
    """

    @cached_property
    def nested(self) -> pd.DataFrame:
        """
        A copy of the table with nested columns.
        """
        # Cast back to the base class since we no longer have the full BIDS table
        # structure.
        return pd.DataFrame(flat_to_multi_columns(self))

    @cached_property
    def flat(self) -> pd.DataFrame:
        """
        A copy of the table with subtable prefixes removed.
        """
        return self.nested.droplevel(0, axis=1)

    @cached_property
    def ds(self) -> pd.DataFrame:
        """
        The dataset info subtable.
        """
        return self.nested["ds"]

    @cached_property
    def ent(self) -> pd.DataFrame:
        """
        The entities subtable.
        """
        return self.nested["ent"]

    @cached_property
    def meta(self) -> pd.DataFrame:
        """
        The metadata subtable
        """
        return self.nested["meta"]

    @cached_property
    def finfo(self) -> pd.DataFrame:
        """
        The file info subtable
        """
        return self.nested["finfo"]

    @cached_property
    def flat_metadata(self) -> pd.DataFrame:
        """
        A table of flattened JSON metadata.
        """
        metadata = pd.json_normalize(self["meta__json"])
        metadata.index = self.index
        return metadata

    @cached_property
    def files(self) -> List[BIDSFile]:
        """
        Get a list of `BIDSFile`s contained in the table.
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
        Get all unique subjects in the table
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
        like: Optional[str] = None,
        regex: Optional[str] = None,
    ) -> "BIDSTable":
        """
        Filter the rows of the table.

        Args:
            key: Column to filter. Can be `"dataset"`, a BIDS entity short or long name,
                or metadata field that's present in the table.
            value: Keep rows with this exact value.
            items: Keep rows whose value is in `items`.
            like: Keep rows whose value contains `like` (string only).
            regex: Keep rows whose value matches `regex` (string only).
        """
        if sum(k is not None for k in [value, items, like, regex]) != 1:
            raise ValueError(
                "Exactly one of value, items, like, or regex must not be None"
            )

        try:
            if key == "dataset":
                col = self["ds__dataset"]
            # JSON metadata field
            # NOTE: Assuming all JSON metadata fields are uppercase. Is this good
            # enough? I believe metadata fields are supposed to be CamelCase, whereas
            # entities are lowercase.
            elif key[:1].isupper():
                col = self.flat_metadata[key]
            # Long name entity
            elif key in ENTITY_NAMES_TO_KEYS:
                col = self.ent[ENTITY_NAMES_TO_KEYS[key]]
            # Short key entity
            else:
                col = self.ent[key]
        except KeyError as exc:
            raise KeyError(
                f"Invalid key {key}; expected a valid BIDS entity or metadata field "
                "present in the dataset"
            ) from exc

        if value is not None:
            mask = col == value
        elif items is not None:
            mask = col.isin(items)
        elif like is not None:
            mask = col.str.contains(like)
        else:
            mask = col.str.match(regex)
        mask = mask.fillna(False)

        return self.loc[mask]

    def sort_entities(
        self, by: Union[str, List[str]], inplace: bool = False
    ) -> "BIDSTable":
        """
        Sort the values of the table by entities.

        Args:
            by: label or list of labels. Can be `"dataset"` or a short or long entity
                name.
            inplace: sort the table in place
        """
        if isinstance(by, str):
            by = [by]

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
    def from_parquet(cls, path: Path):
        """
        Read a BIDS table from a Parquet file or dataset directory.
        """
        df = pd.read_parquet(path)
        return cls(df)

    @property
    def _constructor(self):
        # Makes sure that dataframe slices return a subclass instance
        # https://pandas.pydata.org/docs/development/extending.html#override-constructor-properties
        return BIDSTable
