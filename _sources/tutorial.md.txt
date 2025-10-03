# Tutorial

This tutorial provides a step-by-step introduction to PlexosDB, guiding you through the essential concepts and operations for working with PLEXOS energy market simulation models.

## Prerequisites

Before starting this tutorial, ensure you have:

- Python 3.7 or higher installed
- PlexosDB installed (see [Installation](installation.md))
- Basic understanding of energy market modeling concepts

## Learning Objectives

By the end of this tutorial, you will be able to:

- Create and initialize a PlexosDB database
- Import data from PLEXOS XML files
- Add objects and properties to your model
- Query the database for information
- Work with scenarios and model configurations

## Creating Your First Database

Let's start by creating a new PlexosDB database:

```python
from plexosdb import PlexosDB

# Create a new in-memory database
db = PlexosDB()

# Initialize the database schema
db.create_schema()
```

This creates an empty database with the PLEXOS schema structure ready for data.

## Working with Objects

Objects represent entities in your energy model such as generators, nodes, and regions.

### Adding Objects

```python
from plexosdb.enums import ClassEnum

# Add a generator to the database
db.add_object(ClassEnum.Generator, "Generator1")

# Add a node with additional details
db.add_object(ClassEnum.Node, "Node1", description="Main transmission node")

# Verify objects were added
generators = db.list_objects_by_class(ClassEnum.Generator)
print(f"Generators: {generators}")
```

### Adding Properties

Properties define characteristics of objects like capacity, cost, or operational parameters:

```python
# Add capacity property to the generator
db.add_property(
    ClassEnum.Generator,
    "Generator1",
    "Max Capacity",
    500.0  # MW
)

# Add multiple properties
db.add_property(ClassEnum.Generator, "Generator1", "Min Stable Level", 100.0)
db.add_property(ClassEnum.Generator, "Generator1", "Heat Rate", 9500.0)
```

### Querying Properties

Retrieve property information for specific objects:

```python
# Get all properties for Generator1
properties = db.get_object_properties(ClassEnum.Generator, "Generator1")

for prop in properties:
    print(f"{prop['property']}: {prop['value']} {prop['unit'] or ''}")
```

## Working with XML Files

PlexosDB can import existing PLEXOS XML files and export databases back to XML format.

### Importing from XML

```python
# Create database from existing XML file
db = PlexosDB.from_xml("/path/to/your/plexos_model.xml")

# List all generators in the imported model
generators = db.list_objects_by_class(ClassEnum.Generator)
print(f"Found {len(generators)} generators in the model")
```

### Exporting to XML

```python
# Export your database to XML format
db.to_xml("/path/to/output_model.xml")
```

## Working with Scenarios

Scenarios allow you to model different operational conditions or future projections:

```python
# Add a scenario
scenario_id = db.add_scenario("High Demand")

# Add property with scenario context
db.add_property(
    ClassEnum.Generator,
    "Generator1",
    "Max Capacity",
    750.0,  # Increased capacity for high demand scenario
    scenario="High Demand"
)

# List all scenarios
scenarios = db.list_scenarios()
print(f"Available scenarios: {scenarios}")
```

## Working with Collections and Memberships

Collections define relationships between objects in your model:

```python
from plexosdb.enums import CollectionEnum

# Add a region
db.add_object(ClassEnum.Region, "RegionA")

# Create membership: Generator1 belongs to RegionA
db.add_membership(
    parent_class_enum=ClassEnum.Region,
    child_class_enum=ClassEnum.Generator,
    parent_object_name="RegionA",
    child_object_name="Generator1",
    collection_enum=CollectionEnum.Generators
)

# Query memberships
memberships = db.list_object_memberships(ClassEnum.Generator, "Generator1")
for member in memberships:
    print(f"Generator1 belongs to {member['parent_name']} ({member['parent_class_name']})")
```

## Bulk Operations

For large models, PlexosDB provides efficient bulk operations:

```python
# Add multiple objects at once
generator_names = ["Gen1", "Gen2", "Gen3", "Gen4", "Gen5"]
db.add_objects(*generator_names, class_enum=ClassEnum.Generator)

# Bulk property addition using records
property_records = [
    {"name": "Gen1", "Max Capacity": 100.0, "Heat Rate": 9000.0},
    {"name": "Gen2", "Max Capacity": 150.0, "Heat Rate": 9200.0},
    {"name": "Gen3", "Max Capacity": 200.0, "Heat Rate": 8800.0},
]

db.add_properties_from_records(
    property_records,
    object_class=ClassEnum.Generator,
    collection=CollectionEnum.Generators,
    scenario="Base Case"
)
```

## Next Steps

Now that you have completed the tutorial, you can:

1. Explore the [How-to Guides](howtos/index.md) for specific tasks
2. Consult the [API Reference](api/index.md) for detailed method documentation
3. Review examples in the source code test files
