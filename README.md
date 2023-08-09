# bids2table

bids2table is a library for efficiently indexing and querying large-scale BIDS neuroimaging datasets and derivatives. It aims to improve upon the efficiency of [PyBIDS](https://github.com/bids-standard/pybids) by leveraging modern data science tools.

bids2table represents a BIDS dataset index as a single table with columns for BIDS entities and file metadata. The index is constructed using [Arrow](https://arrow.apache.org/) and stored in [Parquet](https://parquet.apache.org/) format, a binary tabular file format optimized for efficient storage and retrieval.

## Installation

A pre-release version of bids2table can be installed with

```sh
pip install bids2table
```

The latest development version can be installed with

```sh
pip install git+https://github.com/cmi-dair/bids2table.git
```

## Documentation

Our documentation is [here](https://cmi-dair.github.io/bids2table/).

## Example

```python
import pandas as pd

from bids2table import bids2table

# Load in memory as pandas dataframe
df = bids2table("/path/to/dataset")

# Load in parallel and stream to disk as a Parquet dataset
df = bids2table("/path/to/dataset", persistent=True, workers=8)
```

See [here](example/example.ipynb) for a more complete example.

## Performance

bids2table significantly outperforms both [PyBIDS](https://github.com/bids-standard/pybids) and [ancpBIDS](https://github.com/ANCPLabOldenburg/ancp-bids) in terms of indexing run time, index size on disk, and query run time.

### Indexing performance

Indexing run time and index size on disk for the [NKI Rockland Sample](https://fcon_1000.projects.nitrc.org/indi/pro/nki.html) dataset. See the [indexing benchmark](benchmark/indexing) for more details.

| Index | Num workers | Run time (s) | Index size (MB) |
| -- | -- | -- | -- |
| PyBIDS | 1 | 1618 | 448 |
| ancpBIDS | 1 | 465 | -- |
| bids2table | 1 | 402 | 4.02 |
| bids2table | 8 | 53.2 | **3.84** |
| bids2table | 64 | **10.7** | 4.82 |


### Query performance

Query run times for the [Chinese Color Nest Project](http://deepneuro.bnu.edu.cn/?p=163) dataset. See the [query benchmark](benchmark/query) for more details.

| Index | Get subjects (ms) | Get BOLD (ms) | Query metadata (ms) | Get morning scans (ms) |
| -- | -- | -- | -- | -- |
| PyBIDS | 1350 | 12.3 | 6.53 | 34.3 |
| ancpBIDS | 30.6 | 19.2 | -- | -- |
| bids2table | **0.046** | **0.346** | **0.312** | **0.352** |
