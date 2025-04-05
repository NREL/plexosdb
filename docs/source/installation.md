# Installation

This guide explains different methods to install plexosdb.

## Using pip

The simplest way to install plexosdb is using pip:

```bash
pip install plexosdb
```

## Installing a specific version

To install a specific version of plexosdb, specify the version number:

```bash
pip install plexosdb==1.2.3
```

## Development version

To install the latest development version directly from GitHub:

```bash
pip install git+https://github.com/NREL/plexosdb.git
```

## Using uv

[uv](https://github.com/astral-sh/uv) is a new, fast Python package installer and resolver. To install plexosdb using uv:

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install plexosdb using uv
uv pip install plexosdb

# Install a specific version
uv pip install plexosdb==1.2.3
```

## Requirements

plexosdb requires:

- Python 3.7 or higher
- SQLite 3.30 or higher (usually comes with Python)

## Verification

After installation, you can verify that plexosdb was installed correctly by running:

```python
import plexosdb
print(plexosdb.__version__)
```
