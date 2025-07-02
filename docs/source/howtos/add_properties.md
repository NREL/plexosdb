# Adding Properties to Objects

Properties define attributes of objects in your PLEXOS model, such as a generator's capacity or a node's location.

## Basic Property Addition

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
db.add_property(
    ClassEnum.Generator,
    object_name="Generator1",
    name="Max Capacity",
    value=100.0
)

# Add another property
db.add_property(
    ClassEnum.Generator,
    object_name="Generator1",
    name="Min Stable Level",
    value=20.0
)
```

## Adding Properties with Scenarios

Properties can be associated with specific scenarios:

```python
# Add a property with a scenario
db.add_property(
    ClassEnum.Generator,
    object_name="Generator1",
    name="Max Capacity",
    value=120.0,
    scenario="High Demand"
)
```

## Adding Properties with Bands

For properties that have band data:

```python
# Add a property with a band
db.add_property(
    ClassEnum.Generator,
    object_name="Generator1",
    name="Heat Rate",
    value=10.5,
    band=1
)
```

## Adding Text Data to Properties

Properties can include additional text information:

```python
from plexosdb.enums import ClassEnum

# Add a property with text data
db.add_property(
    ClassEnum.Generator,
    object_name="Generator1",
    name="Max Capacity",  # Use a valid property name
    value="Main unit",
    text={ClassEnum.Generator: "Primary generation unit"}
)
```

## Bulk Adding Properties

For efficiency when adding many properties at once:

```python
# Prepare property records
property_records = [
    {"name": "Generator1", "Max Capacity": 100.0, "Min Stable Level": 20.0},
    {"name": "Generator2", "Max Capacity": 150.0, "Min Stable Level": 30.0},
    {"name": "Generator3", "Max Capacity": 250.0, "Min Stable Level": 10.0},
]

# Bulk add properties
db.add_properties_from_records(
    property_records,
    object_class=ClassEnum.Generator,
    parent_class=ClassEnum.System,
    collection=CollectionEnum.Generators,
    scenario="Base Case",
)
```

## Checking Valid Properties

Before adding properties, you can check if they are valid for a collection:

```python
# Check if properties are valid
valid_props = db.list_valid_properties(
    CollectionEnum.Generators,
    parent_class_enum=ClassEnum.System,
    child_class_enum=ClassEnum.Generator
)
print(f"Valid generator properties: {valid_props}")
```

```{warning}
Adding an invalid property will raise a NameError. Always check if properties are valid for your collection.
```
