# PlexosDB Documentation

PlexosDB is a Python library for working with PLEXOS energy market simulation models.

```{toctree}
:maxdepth: 2
:hidden:

installation
tutorial
howtos/index
api/index
CHANGELOG
```

## About PlexosDB

PlexosDB provides a Python interface for working with PLEXOS energy market simulation models. The library converts PLEXOS XML files into SQLite databases and offers a comprehensive API for creating, querying, and manipulating energy system models.

### Key Features

PlexosDB offers the following capabilities:

- Complete support for PLEXOS model components including generators, regions, lines, and transmission networks
- Optimized SQLite backend with transaction support and bulk operations for handling large datasets efficiently
- Seamless bidirectional conversion between PLEXOS XML format and database representation
- Scenario management system for creating and comparing different model configurations
- Memory-efficient iterators and chunked processing for working with large models

## Getting Started

To begin using PlexosDB, start with the [installation guide](installation) and then follow the step-by-step [tutorial](tutorial).

## How-to Guides

Task-oriented guides for specific workflows can be found in the [How-to Guides](howtos/index) section.

## Reference

Complete API documentation is available in the [API Reference](api/index) section.

## Release Notes

Track changes and updates in the [Release Notes](CHANGELOG).

## Indices and Tables

* {ref}`genindex`
* {ref}`modindex`
* {ref}`search`
