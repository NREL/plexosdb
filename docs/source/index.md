# plexosdb Documentation

plexosdb is a Python library for working with PLEXOS energy market simulation models.

```{toctree}
:maxdepth: 2
:hidden:

installation
howtos/index
api/index
CHANGELOG
```

## Features

- Complete API for creating and manipulating energy system models with support
for generators, regions, lines, and other PLEXOS components
- Optimized SQLite backend with transaction support, bulk operations, and
memory-efficient iterators for large datasets
- Seamless conversion between PLEXOS XML format and database representation for
compatibility with PLEXOS
- Built-in support for creating multiple scenarios and analyzing differences
between model configurations

## Quick Start

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum

# Create a database from an XML file
db = PlexosDB.from_xml("/path/to/plexos_model.xml")

# List all generators in the model
generators = db.list_objects_by_class(ClassEnum.Generator)
print(f"Found {len(generators)} generators")

# Get properties for a specific generator
gen_props = db.get_object_properties(ClassEnum.Generator, generators[0])
for prop in gen_props:
    print(f"{prop['property']}: {prop['value']} {prop['unit'] or ''}")
```



## Indices and Tables

* {ref}`genindex`
* {ref}`modindex`
* {ref}`search`
