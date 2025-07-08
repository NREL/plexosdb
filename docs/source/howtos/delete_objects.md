# Deleting Objects from the Database

## Basic Object Creation

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum

# Initialize database
db = PlexosDB()
db.create_schema()

# Add a test generator object
db.add_object(
    ClassEnum.Generator,
    name="TestGenerator",
    description="Example generator",
    category="Thermal"
)

# Delete object
db.delete_object(object_class, name=object_name)
```
