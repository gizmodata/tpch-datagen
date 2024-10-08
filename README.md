# tpch-datagen
A utility to generate TPC-H data in parallel using [DuckDB](https://duckdb.org) and multi-processing

[<img src="https://img.shields.io/badge/GitHub-gizmodata%2Ftpch--datagen-blue.svg?logo=Github">](https://github.com/gizmodata/tpch-datagen)
[![tpch-datagen-ci](https://github.com/gizmodata/tpch-datagen/actions/workflows/ci.yml/badge.svg)](https://github.com/gizmodata/tpch-datagen/actions/workflows/ci.yml)
[![Supported Python Versions](https://img.shields.io/pypi/pyversions/tpch-datagen)](https://pypi.org/project/tpch-datagen/)
[![PyPI version](https://badge.fury.io/py/tpch-datagen.svg)](https://badge.fury.io/py/tpch-datagen)
[![PyPI Downloads](https://img.shields.io/pypi/dm/tpch-datagen.svg)](https://pypi.org/project/tpch-datagen/)

# Why?
Because generating TPC-H data can be time-consuming and resource-intensive.  This project provides a way to generate TPC-H data in parallel using DuckDB and multi-processing.

# Setup (to run locally)

## Install Python package
You can install `tpch-datagen` from PyPi or from source.

### Option 1 - from PyPi
```shell
# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

pip install tpch-datagen
```

### Option 2 - from source - for development
```shell
git clone https://github.com/gizmodata/tpch-datagen

cd tpch-datagen

# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

# Upgrade pip, setuptools, and wheel
pip install --upgrade pip setuptools wheel

# Install TPC-H Datagen - in editable mode with client and dev dependencies
pip install --editable .[dev]
```

### Note
For the following commands - if you running from source and using `--editable` mode (for development purposes) - you will need to set the PYTHONPATH environment variable as follows:
```shell
export PYTHONPATH=$(pwd)/src
```

### Usage
Here are the options for the `tpch-datagen` command:

```shell
tpch-datagen --help
Usage: tpch-datagen [OPTIONS]

Options:
  --version / --no-version        Prints the TPC-H Datagen package version and
                                  exits.  [required]
  --scale-factor INTEGER          The TPC-H Scale Factor to use for data
                                  generation.
  --data-directory TEXT           The target output data directory to put the
                                  files into  [default: data; required]
  --work-directory TEXT           The work directory to use for data
                                  generation.  [default: /tmp; required]
  --overwrite / --no-overwrite    Can we overwrite the target directory if it
                                  already exists...  [default: no-overwrite;
                                  required]
  --num-chunks INTEGER            The number of chunks that will be generated
                                  - more chunks equals smaller memory
                                  requirements, but more files generated.
                                  [default: 10; required]
  --num-processes INTEGER         The maximum number of processes for the
                                  multi-processing pool to use for data
                                  generation.  [default: 10; required]
  --duckdb-threads INTEGER        The number of DuckDB threads to use for data
                                  generation (within each job process).
                                  [default: 1; required]
  --per-thread-output / --no-per-thread-output
                                  Controls whether to write the output to a
                                  single file or multiple files (for each
                                  process).  [default: per-thread-output;
                                  required]
  --compression-method [none|snappy|gzip|zstd]
                                  The compression method to use for the
                                  parquet files generated.  [default: zstd;
                                  required]
  --file-size-bytes TEXT          The target file size for the parquet files
                                  generated.  [default: 100m; required]
  --help                          Show this message and exit.
```

> [!NOTE]   
> Default values may change depending on the number of CPU cores you have, etc.

### Handy development commands

#### Version management

##### Bump the version of the application - (you must have installed from source with the [dev] extras)
```bash
bumpver update --patch
```
