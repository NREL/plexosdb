# Adding Attributes to the objects

Objects in PlexosDB can have attributes that are saved on the `t_attribute_data`
table.

## Listing available attributes per `ClassEnum`

To see the list of available attributes per `ClassEnum` use:
```python
from plexosdb import PlexosDB, ClassEnum
db = PlexosDB.from_xml("/path/to/your/xml")

attributes = db.list_attributes(ClassEnum.Generator)
print(attributes)
```

## Adding an attribute to an existing object

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum, CollectionEnum

# Initialize database
db = PlexosDB()
db.create_schema()

# Create a generator object if it doesn't exist
if not db.check_object_exists(ClassEnum.Generator, "Generator1"):
    db.add_object(ClassEnum.Generator, "Generator1")

# Add a property to the generator
attribute_name = "Latitude"
db.add_attribute(
    ClassEnum.Generator,
    object_name="Generator1",
    attribute_name=attribute_name,
    attribute_value=100.0
)
```

## Extracting an attribute from an object

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum, CollectionEnum

# Initialize database
db = PlexosDB()
db.create_schema()

# Create a generator object if it doesn't exist
if not db.check_object_exists(ClassEnum.Generator, "Generator1"):
    db.add_object(ClassEnum.Generator, "Generator1")

# Add a property to the generator
attribute_name = "Latitude"
db.add_attribute(
    ClassEnum.Generator,
    object_name="Generator1",
    attribute_name=attribute_name,
    attribute_value=19.8
)

db.get_attribute(ClassEnum.Generator, object_name="Generator1",
                 attribute_name=attribute_name)
```
