# Migrating from pybids to bids2table

`bids2table.pybids` implements pybids' `BIDSLayout` / `BIDSFile` querying API
on top of bids2table's indexer. Most querying code migrates with an import
swap, and then you need to address the differences below.

## 1. Install the `pybids` extra

```sh
pip install bids2table[pybids]
```

## 2. Swap the import

```diff
- from bids import BIDSLayout
+ from bids2table.pybids import BIDSLayout
```

Your querying code should work unchanged:

```python
layout = BIDSLayout("/data/ds001", derivatives=["/data/derivs"])

files = layout.get(subject="01", suffix="bold")             # [BIDSFile, ...]
names = layout.get(suffix="bold", return_type="filename")   # [str, ...]
layout.get_subjects()
layout.get_sessions(subject="01")
layout.get_metadata(names[0])   # sidecar JSON, merged per BIDS inheritance
files[0].get_entities()         # {'sub': '01', 'suffix': 'bold', ...}
```

The index is cached to `{root}/.bids2table_cache.parquet` on first
construction; override with `cache_path=`, force re-indexing with
`reset_database=True`. (`database_path=` is accepted but deprecated.)

## 3. Expect these behavioral differences

| pybids | bids2table |
| --- | --- |
| `BIDSFile.path` is absolute | paths are relative to the root — `layout.root / name` |
| `layout.get_file(missing)` returns `None` | returns a `BIDSFile` for any path |
| `run=1` and `run="1"` are equivalent | exact match — `run` is an `int` |
| unknown filter raises (`invalid_filters='error'`) | warns and drops the filter |
| `regex_search=True`, `validate=False` | accepted but ignored (they have no effect) |
| results are naturally sorted (by path) | index order — wrap in `sorted()` if needed |
| `return_type='object'`, `Query.REQUIRED` | `return_type='file'`, `Query.ANY` |
| `layout.get(target='task', ...)` | `layout.get_entities(**filters)['task']` (returns observed values, not schema definitions) |

## 4. Substitute for unimplemented features

| pybids feature | Substitute |
| --- | --- |
| `get_tasks()` and other dynamic `get_<entity>()` getters | `layout.get_entities(**filters)` or `layout.df` |
| `BIDSFile.get_dict()` / `.get_metadata()` / `.get_associations()` | `layout.get_metadata(path)` |
| `layout.load()` / `.save()` | the automatic parquet cache (step 2) |

No substitute: `get_fieldmap()`, `get_tr()`, `get_bval()` /
`get_bvec()`, `get_dataset_description()`, `build_path()`, `copy_files()`,
`write_to_file()`, derivatives scoping (`scope=`, `add_derivatives()`,
`layout.derivatives`), indexer options (`ignore`, `extensions`, `config`,
`index_metadata`), `BIDSImageFile`, `BIDSJSONFile`, `BIDSDataFile`.

## 5. Still missing something?

[Open an issue](https://github.com/childmindresearch/bids2table/issues)
describing what you're trying to do.
