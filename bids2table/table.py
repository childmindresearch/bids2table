from functools import cached_property
from pathlib import Path

import pandas as pd

from bids2table.helpers import flat_to_multi_columns


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
    def dataset(self) -> pd.DataFrame:
        """
        The dataset info subtable.
        """
        return self.nested["ds"]

    @cached_property
    def entities(self) -> pd.DataFrame:
        """
        The entities subtable.
        """
        return self.nested["ent"]

    @cached_property
    def metadata(self) -> pd.DataFrame:
        """
        The metadata subtable
        """
        return self.nested["meta"]

    @cached_property
    def file(self) -> pd.DataFrame:
        """
        The file info subtable
        """
        return self.nested["file"]

    @cached_property
    def flat_metadata(self) -> pd.DataFrame:
        """
        A table of flattened JSON metadata.
        """
        return pd.json_normalize(self["meta__json"])

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
