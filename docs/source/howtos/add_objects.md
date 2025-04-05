# Adding Objects to the Database

Objects in PlexosDB represent entities in your PLEXOS model like generators, regions, and nodes.

## Basic Object Creation

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum

# Initialize database
db = PlexosDB()
db.create_schema()

# Add a generator object
db.add_object(
    ClassEnum.Generator,
    name="Generator1",
    description="Example generator",
    category="Thermal"
)

# Add a node object
db.add_object(
    ClassEnum.Node,
    name="Node1"
)

# Add a region object
db.add_object(
    ClassEnum.Region,
    name="Region1"
)
```

## Adding Objects with Custom Categories

You can organize your objects by adding them to specific categories:

```python
# Add generator with custom category
db.add_object(
    ClassEnum.Generator,
    name="Wind_Farm1",
    category="Renewable",
    description="Offshore wind farm"
)
```

## Checking if Objects Exist

Before adding objects, you can check if they already exist:

```python
if not db.check_object_exists(ClassEnum.Generator, "Generator1"):
    db.add_object(ClassEnum.Generator, "Generator1")
else:
    print("Generator already exists")
```

## Listing Objects by Class

You can list all objects of a specific class:

```python
# List all generators
generators = db.list_objects_by_class(ClassEnum.Generator)
print(f"Found {len(generators)} generators: {generators}")

# List all generators in a specific category
thermal_generators = db.list_objects_by_class(ClassEnum.Generator, category="Thermal")
print(f"Found {len(thermal_generators)} thermal generators")
```

## Working with Multiple Objects

For bulk operations, you can create multiple objects efficiently:

```python
# Create multiple generators
for i in range(1, 101):
    db.add_object(
        ClassEnum.Generator,
        name=f"Generator{i}",
        category="Thermal" if i % 2 == 0 else "Renewable"
    )
```

```{note}
When creating objects, PlexosDB automatically creates a system membership for the object.
```
