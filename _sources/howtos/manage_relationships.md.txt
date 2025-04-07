# Managing Object Relationships

In PLEXOS, objects can have relationships with each other. These relationships are managed through memberships in PlexosDB.

## Creating Relationships (Memberships)

Create relationships between objects:

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum, CollectionEnum

# Initialize database
db = PlexosDB()
db.create_schema()

# Create parent and child objects
db.add_object(ClassEnum.Region, "Region1")
db.add_object(ClassEnum.Node, "Node1")
db.add_object(ClassEnum.Node, "Node2")

# Create a membership between Region and Node
db.add_membership(
    parent_class_enum=ClassEnum.Region,
    child_class_enum=ClassEnum.Node,
    parent_object_name="Region1",
    child_object_name="Node1",
    collection_enum=CollectionEnum.RegionNodes
)

# Add another node to the region
db.add_membership(
    parent_class_enum=ClassEnum.Region,
    child_class_enum=ClassEnum.Node,
    parent_object_name="Region1",
    child_object_name="Node2",
    collection_enum=CollectionEnum.RegionNodes
)
```

## Checking Memberships

Check if a membership exists:

```python
# Check if a membership exists
membership_exists = db.check_membership_exists(
    "Region1",
    "Node1",
    parent_class=ClassEnum.Region,
    child_class=ClassEnum.Node,
    collection=CollectionEnum.RegionNodes
)

if membership_exists:
    print("Membership exists")
else:
    print("Membership does not exist")
```

## Getting Membership Information

Retrieve membership information:

```python
# Get membership ID
membership_id = db.get_membership_id(
    "Region1",
    "Node1",
    CollectionEnum.RegionNodes
)
print(f"Membership ID: {membership_id}")

# Get all memberships for an object
memberships = db.get_memberships(
    "Node1",
    object_class=ClassEnum.Node
)

for membership in memberships:
    print(f"Parent: {membership['parent']} ({membership['parent_class_name']})")
    print(f"Child: {membership['child']} ({membership['child_class_name']})")
    print(f"Collection: {membership['collection_name']}")
    print("---")
```

## Listing Child Objects

List all child objects for a parent:

```python
# List all nodes in a region
child_objects = db.list_child_objects(
    "Region1",
    parent_class=ClassEnum.Region,
    child_class=ClassEnum.Node,
    collection=CollectionEnum.RegionNodes
)

print("Nodes in Region1:")
for child in child_objects:
    print(f"- {child['name']}")
```

## Listing Parent Objects

List all parent objects for a child:

```python
# List all regions a node belongs to
parent_objects = db.list_parent_objects(
    "Node1",
    child_class=ClassEnum.Node,
    parent_class=ClassEnum.Region,
    collection=CollectionEnum.RegionNodes
)

print("Regions containing Node1:")
for parent in parent_objects:
    print(f"- {parent['name']}")
```

## Bulk Adding Memberships

For efficiency when adding many memberships:

```python
# Create membership records
membership_records = [
    {"parent": "Region1", "child": "Node1"},
    {"parent": "Region1", "child": "Node2"},
    {"parent": "Region1", "child": "Node3"},
    # Add more relationships as needed
]

# Bulk add memberships
db.add_memberships_from_records(
    membership_records,
    parent_class=ClassEnum.Region,
    child_class=ClassEnum.Node,
    collection=CollectionEnum.RegionNodes,
    create_missing_objects=True  # This will create Node3 if it doesn't exist
)
```

```{note}
Every object in PlexosDB automatically has a system membership when created. This links the object to the "System" object.
```
