# Query benchmark

In this benchmark we compare the query performance for [PyBIDS](https://github.com/bids-standard/pybids), [ancpBIDS](https://github.com/ANCPLabOldenburg/ancp-bids), and [bids2table](https://github.com/cmi-dair/bids2table). The queries are modeled after the [PyBIDS `BIDSLayout` tutorial](https://bids-standard.github.io/pybids/examples/pybids_tutorial.html#querying-the-bidslayout).

For this benchmark, we use raw data from the [Chinese Color Nest Project](http://deepneuro.bnu.edu.cn/?p=163) (195 subjects, 2 resting state sessions per subject).

## Setup environment

We create a single environment that includes all three main libraries using the [setup_environments.sh](setup_environments.sh) script. The main libraries are pinned to the following versions.

- PyBIDS: 0.16.0
- ancpBIDS: 0.2.2
- bids2table: 0.1.dev29+gb7b1658

## Run benchmark

The [query_benchmark.ipynb](query_benchmark.ipynb) notebook runs the benchmark. We ran the benchmark on the [PSC bridges2](https://www.psc.edu/resources/bridges-2/) HPC system.

## Results

We compared the performance of the different indices on four queries:

- Get subjects: Get a list of all unique subjects
- Get BOLD: Get a list of all BOLD Nifti image files
- Query Metadata: Find scans with a specific value for a sidecar metadata field
- Get morning scans: Find scans that were acquired before 10 AM

Below is a summary table of the query run times in milliseconds. We find that bids2table is >20x faster than PyBIDS and ancpBIDS.

| Index | Get subjects (ms) | Get BOLD (ms) | Query metadata (ms) | Get morning scans (ms) |
| -- | -- | -- | -- | -- |
| PyBIDS | 1350 | 12.3 | 6.53 | 34.3 |
| ancpBIDS | 30.6 | 19.2 | -- | -- |
| bids2table | **0.046** | **0.346** | **0.312** | **0.352** |

Note that ancpBIDS is missing values for the two queries that require accessing the sidecar metadata. It's possible that ancpBIDS supports these queries, but [looking at the documentation](https://ancpbids.readthedocs.io/en/latest/advancedQueries.html), it wasn't obvious to me how.
