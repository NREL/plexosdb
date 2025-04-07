# Creating a database from an existing XML file

PlexosDB allows you to create a database from an existing XML file using a few simple steps.

## Basic Usage

```python
from plexosdb import PlexosDB

# Create a new database
db = PlexosDB.from_xml("/path/to/xml")
db.create_schema()
```
