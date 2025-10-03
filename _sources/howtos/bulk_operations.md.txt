# Bulk Operations with PlexosDB

This guide demonstrates how to efficiently perform bulk operations using PlexosDB, which can significantly improve performance when working with large datasets.

## Bulk Inserting Properties

When you need to add multiple properties to multiple objects, using individual `add_property` calls can be inefficient. The `add_properties_from_records` method provides a much more efficient approach.

### Basic Usage

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum, CollectionEnum

# Initialize the database
db = PlexosDB()
db.create_schema()

# Create the objects first
db.add_object(ClassEnum.Generator, "Generator1")
db.add_object(ClassEnum.Generator, "Generator2")
db.add_object(ClassEnum.Generator, "Generator3")

# Prepare the property records
records = [
    {
        "name": "Generator1",
        "Max Capacity": 100.0,
        "Min Stable Level": 20.0,
        "Heat Rate": 10.5
    },
    {
        "name": "Generator2",
        "Max Capacity": 150.0,
        "Min Stable Level": 30.0,
        "Heat Rate": 9.8
    },
    {
        "name": "Generator3",
        "Max Capacity": 200.0,
        "Min Stable Level": 40.0,
        "Heat Rate": 8.7
    }
]

# Add all properties in bulk
db.add_properties_from_records(
    records,
    object_class=ClassEnum.Generator,
    collection=CollectionEnum.Generators,
    scenario="Base Case"
)
```

### Performance Considerations

The `add_properties_from_records` method processes records in batches (default 10,000 records per batch) and uses SQLite transactions to maximize performance. This makes it much faster than individual property insertions, especially for large datasets.

Key performance features:
- Single transaction for all insertions (atomic operations)
- Batch processing to control memory usage
- Direct SQL execution with prepared statements
- Automatic property enablement (sets `is_dynamic` and `is_enabled` flags)

### Handling Different Object Classes

You can process different types of objects separately:

```python
# Add Generator objects
db.add_object(ClassEnum.Generator, "Generator1")
db.add_object(ClassEnum.Generator, "Generator2")

# Add Region objects
db.add_object(ClassEnum.Region, "Region1")
db.add_object(ClassEnum.Region, "Region2")

# Generator properties
generator_records = [
    {"name": "Generator1", "Max Capacity": 100.0},
    {"name": "Generator2", "Max Capacity": 150.0}
]

# Region properties
region_records = [
    {"name": "Region1", "Load Scaling Factor": 1.1},
    {"name": "Region2", "Load Scaling Factor": 0.9}
]

# Process each set with appropriate parameters
db.add_properties_from_records(
    generator_records,
    object_class=ClassEnum.Generator,
    collection=CollectionEnum.Generators,
    scenario="Base Case"
)

db.add_properties_from_records(
    region_records,
    object_class=ClassEnum.Region,
    collection=CollectionEnum.Regions,
    scenario="Base Case"
)
```

### Data Validation

The method automatically validates:
- All objects exist before attempting inserts
- All property names are valid for the collection
- All required fields are present

## Bulk Inserting Memberships

Creating relationships between many objects can be time-consuming when done individually. The `add_memberships_from_records` method allows you to efficiently create multiple memberships in a single operation.

### Basic Usage

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum, CollectionEnum
from plexosdb.utils import create_membership_record

# Initialize the database
db = PlexosDB()
db.create_schema()

# Create parent and child objects
region_id = db.add_object(ClassEnum.Region, "MainRegion")

# Create multiple nodes
node_ids = []
for i in range(1, 101):  # Create 100 nodes
    node_ids.append(db.add_object(ClassEnum.Node, f"Node{i}"))

# Get necessary IDs for memberships
parent_class_id = db.get_class_id(ClassEnum.Region)
child_class_id = db.get_class_id(ClassEnum.Node)
collection_id = db.get_collection_id(
    CollectionEnum.ReferenceNode,
    parent_class_enum=ClassEnum.Region,
    child_class_enum=ClassEnum.Node
)

# Create membership records
membership_records = create_membership_record(
    node_ids,
    child_object_class_id=child_class_id,
    parent_object_class_id=parent_class_id,
    parent_object_id=region_id,
    collection_id=collection_id
)

# Bulk insert all memberships at once
db.add_memberships_from_records(membership_records)
```


To identify the correct `CollectionEnum` for your relationship, use the `list_collections` method:

```python
collection_list = db.list_collections(parent_class=ClassEnum.Region, child_class=ClassEnum.Node)
print(collection_list)  # Shows available collections for Region-Node relationships
```

This ensures you're using the exact collection name that exists in your database schema.


### Performance Benefits

Using `add_memberships_from_records` offers several advantages over individual `add_membership` calls:

- Significantly reduced execution time for large datasets
- Lower overhead from fewer database operations
- Optional chunking for very large datasets (controlled by the `chunksize` parameter)
- Efficient batch SQL execution

### Manual Record Creation

If you need more control, you can manually create the membership records:

```python
# Create records manually
records = []
for node_id in node_ids:
    records.append({
        'parent_class_id': parent_class_id,
        'parent_object_id': region_id,
        'collection_id': collection_id,
        'child_class_id': child_class_id,
        'child_object_id': node_id
    })

# Bulk insert memberships
db.add_memberships_from_records(records)
```

Each record must contain these fields:
- `parent_class_id`: ID of the parent class
- `parent_object_id`: ID of the parent object
- `collection_id`: ID of the collection
- `child_class_id`: ID of the child class
- `child_object_id`: ID of the child object

## Combined Bulk Operations

For complex model creation, you can combine bulk operations to efficiently build your model:

1. First create all objects using `add_objects` (bulk object creation)
2. Add memberships between objects with `add_memberships_from_records`
3. Add properties to the objects using `add_properties_from_records`

This approach can dramatically improve performance when creating large, complex models.
