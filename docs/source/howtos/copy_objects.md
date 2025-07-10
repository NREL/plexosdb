# Copying Objects

PlexosDB allows you to create copies of existing objects along with their properties, memberships, and associated data.

## Basic Object Copying

Copy an object and all its properties:

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum

# Initialize database
db = PlexosDB.from_xml("/path/to/model.xml")

db.add_property(
    ClassEnum.Generator,
    object_name="Generator1",
    name="Max Capacity",
    value=100.0,
    scenario="Base Case"
)

db.add_property(
    ClassEnum.Generator,
    object_name="Generator1",
    name="Min Stable Level",
    value=20.0,
    scenario="Base Case"
)

# Copy a generator with all its properties
new_object_id = db.copy_object(
    object_class=ClassEnum.Generator,
    original_object_name="Generator1",
    new_object_name="Generator1_Copy",
    copy_properties=True  # Default is True
)

print(f"Created new object with ID: {new_object_id}")
```

## Copying Objects Without Properties

You can also copy just the object structure without its properties:

```python
# Add Generator object
db.add_object(ClassEnum.Generator, "Generator1")

# Add property
db.add_property(
    ClassEnum.Generator,
    object_name="Generator1",
    name="Max Capacity",
    value=20.0,
    scenario="Base Case"
)

# Copy object structure only
new_object_id = db.copy_object(
    object_class=ClassEnum.Generator,
    original_object_name="Generator1",
    new_object_name="Generator1_Skeleton",
    copy_properties=False
)
```

## Copying Memberships

When copying an object, its memberships are also copied:

```python
# First create some objects with memberships
db.add_object(ClassEnum.Region, "Region1")
db.add_object(ClassEnum.Node, "Node1")

# Add membership
db.add_membership(
    parent_class_enum=ClassEnum.Region,
    child_class_enum=ClassEnum.Node,
    parent_object_name="Region1",
    child_object_name="Node1",
    collection_enum=CollectionEnum.ReferenceNode
)

# Add property
db.add_property(
    ClassEnum.Node,
    object_name="Node1",
    name="Voltage",  # Common node property
    value=138.0,
    scenario="Base Case"
)

# Now copy the node with its memberships
new_object_id = db.copy_object(
    object_class=ClassEnum.Node,
    original_object_name="Node1",
    new_object_name="Node1_Copy",
    copy_properties=False
)

# Check the memberships of the new object
memberships = db.get_memberships_system(
    "Node1_Copy",
    object_class=ClassEnum.Node
)
print(f"New object has {len(memberships)} memberships")
```

## Selective Membership Copying

You can also manually copy specific memberships:

```python
db.add_object(ClassEnum.Node, "Node1")
db.add_object(ClassEnum.Generator, "Generator1")

db.add_membership(
    parent_class_enum=ClassEnum.Generator,
    child_class_enum=ClassEnum.Node,
    parent_object_name="Generator1",
    child_object_name="Node1",
    collection_enum=CollectionEnum.Nodes
)

membership_mapping = db.copy_object_memberships(
    object_class=ClassEnum.Generator,
    original_name="Generator1",
    new_name="Generator1_Copy"
)

print(f"Copied {len(membership_mapping)} memberships")
```

## Practical Example: Duplicating a Set of Objects

This example shows how to duplicate a group of related objects:

```python
# Create a function to copy a generator and all its connections
def duplicate_generator_with_connections(db, original_name, new_name):
    # Add Generator object
    db.add_object(ClassEnum.Generator, original_name)

    # Add Generator property
    db.add_property(
        ClassEnum.Generator,
        object_name=original_name,
        name="Max Capacity",  # Common node property
        value=138.0,
        scenario="Base Case"
    )

    # Copy the generator itself
    db.copy_object(
        object_class=ClassEnum.Generator,
        original_object_name=original_name,
        new_object_name=new_name,
        copy_properties=False
    )

    # Find and copy all connections
    memberships = db.get_memberships_system(
        original_name,
        object_class=ClassEnum.Generator
    )

    # Process each membership to maintain the network structure
    for membership in memberships:
        # Skip system memberships which are automatically created
        if membership["parent_class_name"] == "System":
            continue

        if membership["parent"] == original_name:
            # Original generator was the parent, connect child to new generator
            db.add_membership(
                parent_class_enum=ClassEnum[membership["parent_class_name"]],
                child_class_enum=ClassEnum[membership["child_class_name"]],
                parent_object_name=new_name,
                child_object_name=membership["child"],
                collection_enum=CollectionEnum[membership["collection_name"]]
            )
        else:
            # Original generator was the child, connect new generator to parent
            db.add_membership(
                parent_class_enum=ClassEnum[membership["parent_class_name"]],
                child_class_enum=ClassEnum[membership["child_class_name"]],
                parent_object_name=membership["parent"],
                child_object_name=new_name,
                collection_enum=CollectionEnum[membership["collection_name"]]
            )

    return new_name

# Use the function to duplicate a generator with all its connections
duplicate_generator_with_connections(db, "Generator1", "Generator1_Full_Copy")
```

```{note}
When copying objects with properties that reference other objects (like "Node" for a generator), you may need to update these properties manually if you want them to reference different objects.
```
