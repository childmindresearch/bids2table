r"""
[bids2table](https://github.com/cmi-dair/bids2table) is a library for efficiently
indexing and querying large-scale BIDS neuroimaging datasets and derivatives. It aims to
improve upon the efficiency of [PyBIDS](https://github.com/bids-standard/pybids) by
leveraging modern data science tools.

bids2table represents a BIDS dataset index as a single table with columns for BIDS
entities and file metadata. The index is constructed using
[Arrow](https://arrow.apache.org/) and stored in [Parquet](https://parquet.apache.org/)
format, a binary tabular file format optimized for efficient storage and retrieval.

## Installation

A pre-release version of bids2table can be installed with

```sh
pip install bids2table
```

The latest development version can be installed with

```sh
pip install git+https://github.com/cmi-dair/bids2table.git
```

## Quickstart

The main entrypoint to the library is the `bids2table.bids2table` function, which builds
the index.

```python
tab = bids2table("path/to/dataset")
```

You can also build the index in parallel

```python
tab = bids2table("path/to/dataset", workers=8)
```

To save the index to disk as a [Parquet](https://parquet.apache.org/) dataset for later
reuse, run

```python
tab = bids2table("path/to/dataset", persistent=True)
```

By default this saves the index to an `index.b2t` directory under the dataset root
directory. To change the output destination, use the `index_path` argument.

To generate and save an index from the command line, you can use the `bids2table` CLI.

```sh
usage: bids2table [-h] [--output OUTPUT] [--incremental] [--overwrite] [--workers COUNT]
                  [--worker_id RANK] [--verbose]
                  ROOT
```

See `bids2table --help` for more information.

## Table representation

The generated index is represented as a `bids2table.BIDSTable`, which is just a subclass
of a `pandas.DataFrame`. Each row in the table corresponds to a BIDS data file, and the
columns are organized into groups:

- dataset (`BIDSTable.ds`): dataset name, relative dataset path, and the JSON dataset
description
- entities (`BIDSTable.ent`): All [valid BIDS
entities](https://bids-specification.readthedocs.io/en/stable/appendices/entities.html)
plus an `extra_entities` dict containing any extra entities
- metadata (`BIDSTable.meta`): BIDS JSON metadata
- file info (`BIDSTable.finfo`): General file info including the full file path and last
modified time

The `BIDSTable` also makes it easy to access some of the key characteristics of your
dataset. The BIDS datatypes, modalities, subjects, and entities present in the dataset
are each accessible as properties of the table.

In addition, the associated JSON metadata for each file can be conveniently accessed via
the `BIDSTable.flat_meta` property.

### Filtering

To filter the table for files matching certain criteria, you can use the
`BIDSTable.filter` method which selects for rows based on whether a specified column
meets a condition.

```python
filtered = (
    tab
    .filter("task", "rest")
    .filter("sub", items=["04", "06"])
    .filter("RepetitionTime", 2.5)
)
```

To apply multiple filters at once, you can also use `BIDSTable.filter_multi`

```python

filtered = tab.filter_multi(
    task="rest"
    sub={"items": ["04", "06"]},
    RepetitionTime=2.5,
)
```

This is similar to the
[`BIDSLayout.get()`](https://bids-standard.github.io/pybids/generated/bids.layout.BIDSLayout.html#bids.layout.BIDSLayout)
method in [`PyBIDS`](https://bids-standard.github.io/pybids/), where each `key=value`
pair specifies the column to filter on and the condition to apply.

### Advanced usage

For more advanced usage that goes beyond what's supported in this higher-level
interface, you can also interact directly with the underlying
[`DataFrame`](https://pandas.pydata.org/docs/user_guide/index.html).

The column labels of the raw table indicate the group as a prefix, e.g. `ent__*` for
BIDS entities. However, you may find one of the alternative views of the table more
useful:

- `BIDSTable.nested`: Columns organized as a nested pandas
[`MultiIndex`](https://pandas.pydata.org/docs/user_guide/advanced.html#hierarchical-indexing-multiindex).
- `BIDSTable.flat`: Flattened columns without any nesting or group prefix.

.. warning::
    You should avoid manipulating the table in place if possible, as this may interfere
    with the higher-level accessors. If you must manipulate in place, consider
    converting the `BIDSTable` to a plain `DataFrame` first.

    ```python
    df = pd.DataFrame(tab)
    ```
"""

# Register elbow extension types
import elbow.dtypes  # noqa

from ._b2t import bids2table
from ._version import __version__, __version_tuple__  # noqa
from .entities import BIDSEntities, parse_bids_entities
from .table import BIDSFile, BIDSTable, join_bids_path

__all__ = [
    "bids2table",
    "BIDSTable",
    "BIDSFile",
    "BIDSEntities",
    "parse_bids_entities",
    "join_bids_path",
]
