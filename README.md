# bids2table
[![CI](https://github.com/childmindresearch/bids2table/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/childmindresearch/bids2table/actions/workflows/ci.yaml?query=branch%3Amain)
[![Docs](https://github.com/childmindresearch/bids2table/actions/workflows/docs.yaml/badge.svg?branch=main)](https://childmindresearch.github.io/bids2table/bids2table)
[![codecov](https://codecov.io/gh/childmindresearch/bids2table/branch/main/graph/badge.svg?token=22HWWFWPW5)](https://codecov.io/gh/childmindresearch/bids2table)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
![Python3](https://img.shields.io/badge/python->=3.11-blue.svg)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Index [BIDS](https://bids-specification.readthedocs.io/en/stable/) datasets fast, locally or in the cloud.

## Installation

To install the latest release from pypi, you can run

```sh
pip install bids2table
```

To install with S3 support, include the `s3` extra

```sh
pip install bids2table[s3]
```

The latest development version can be installed with

```sh
pip install "bids2table[s3] @ git+https://github.com/childmindresearch/bids2table.git"
```

## Usage

To run these examples, you will need to clone the [bids-examples](https://github.com/bids-standard/bids-examples) repo.

```sh
git clone -b 1.9.0 https://github.com/bids-standard/bids-examples.git
```

### Finding BIDS datasets

You can search a directory for valid BIDS datasets using `b2t2 find`

```
(bids2table) clane$ b2t2 find bids-examples | head -n 10
bids-examples/asl002
bids-examples/ds002
bids-examples/ds005
bids-examples/asl005
bids-examples/ds051
bids-examples/eeg_rishikesh
bids-examples/asl004
bids-examples/asl003
bids-examples/ds003
bids-examples/eeg_cbm
```

### Indexing datasets from the command line

Indexing datasets is done with `b2t2 index`. Here we index a single example dataset, saving the output as a parquet file.

```
(bids2table) clane$ b2t2 index -o ds102.parquet bids-examples/ds102
ds102: 100%|███████████████████████████████████████| 26/26 [00:00<00:00, 154.12it/s, sub=26, N=130]
```

You can also index a list of datasets. Note that each iteration in the progress bar represents one dataset.

```
(bids2table) clane$ b2t2 index -o bids-examples.parquet bids-examples/*
100%|████████████████████████████████████████████| 87/87 [00:00<00:00, 113.59it/s, ds=None, N=9727]
```

You can pipe the output of `b2t2 find` to `b2t2 index` to create an index of all datasets under a root directory.

```
(bids2table) clane$ b2t2 find bids-examples | b2t2 index -o bids-examples.parquet
97it [00:01, 96.05it/s, ds=ieeg_filtered_speech, N=10K]
```

The resulting index will include both top-level datasets (as in the previous command) as well nested derivatives datasets.

### Indexing datasets hosted on S3

bids2table supports indexing datasets hosted on S3 via [cloudpathlib](https://github.com/drivendataorg/cloudpathlib). To use this functionality, make sure to install bids2table with the `s3` extra. Or you can also just install cloudpathlib directly

```sh
pip install cloudpathlib[s3]
```

As an example, here we index all datasets on [OpenNeuro](https://openneuro.org/)

```
(bids2table) clane$ b2t2 index -o openneuro.parquet \
  -j 8 --use-threads s3://openneuro.org/ds*
100%|█████████████████████████████████████| 1408/1408 [12:25<00:00,  1.89it/s, ds=ds006193, N=1.2M]
```

Using 8 threads, we can index all ~1400 OpenNeuro datasets (1.2M files) in less than 15 minutes.


### Indexing datasets from python

You can also index datasets using the Python API.

```python
import bids2table as b2t2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Index a single dataset.
tab = b2t2.index_dataset("bids-examples/ds102")

# Find and index a batch of datasets.
tabs = b2t2.batch_index_dataset(
    b2t2.find_bids_datasets("bids-examples"),
)
tab = pa.concat_tables(tabs)

# Index a dataset on S3.
tab = b2t2.index_dataset("s3://openneuro.org/ds000224")

# Save as parquet.
pq.write_table(tab, "ds000224.parquet")

# Convert to a pandas dataframe.
df = tab.to_pandas(types_mapper=pd.ArrowDtype)
```
