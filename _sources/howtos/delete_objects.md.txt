# Deleting Objects and Properties from the Database

This guide demonstrates how to delete objects and properties from the PlexosDB database.

## Deleting Objects

When you delete an object, all its associated data (properties, memberships, etc.) are automatically removed due to foreign key constraints with cascade deletion.

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum

# Initialize database
db = PlexosDB()
db.create_schema()

# Add a simple test generator object
db.add_object(
    ClassEnum.Generator,
    name="TestGenerator",
    description="Example generator",
    category="Thermal"
)

# Delete the entire object (removes all properties and memberships)
db.delete_object(ClassEnum.Generator, name="TestGenerator")
```

## Deleting Properties

You can delete specific properties from objects without removing the object itself. This provides fine-grained control over data management.

### Basic Property Deletion

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum

# Initialize database from XML or create new
db = PlexosDB.from_xml("model.xml")

# Add a generator with properties
db.add_object(ClassEnum.Generator, "PowerPlant1")
db.add_property(ClassEnum.Generator, "PowerPlant1", "Max Capacity", 500.0)
db.add_property(ClassEnum.Generator, "PowerPlant1", "Min Stable Level", 100.0)

# Delete a specific property
db.delete_property(ClassEnum.Generator, "PowerPlant1", property_name="Min Stable Level")

# The object still exists, but only with the "Max Capacity" property
```

### Scenario-Specific Property Deletion

When properties have scenario-specific values, you can delete only the property data associated with a particular scenario:

```python
# Add properties with different scenarios
db.add_property(ClassEnum.Generator, "PowerPlant1", "Max Capacity", 500.0)  # Base case
db.add_property(ClassEnum.Generator, "PowerPlant1", "Max Capacity", 600.0, scenario="High Demand")
db.add_property(ClassEnum.Generator, "PowerPlant1", "Max Capacity", 400.0, scenario="Low Demand")

# Delete only the "High Demand" scenario property
db.delete_property(
    ClassEnum.Generator,
    "PowerPlant1",
    property_name="Max Capacity",
    scenario="High Demand"
)

# The base case and "Low Demand" scenario properties remain
```

### Advanced Property Deletion

You can specify custom collections and parent classes when deleting properties:

```python
from plexosdb.enums import CollectionEnum

# Delete property with specific collection
db.delete_property(
    ClassEnum.Generator,
    "PowerPlant1",
    property_name="Heat Rate",
    collection=CollectionEnum.GeneratorProperties,
    parent_class=ClassEnum.System
)
```

## Important Notes

### Cascade Deletion Behavior

- **Object deletion**: Removes the object and ALL associated data (properties, memberships, text data, etc.)
- **Property deletion**: Removes only the specified property data, including associated text, tags, and date ranges
- **Scenario-specific deletion**: Removes only property data tagged with the specified scenario

### Error Handling

The deletion methods will raise appropriate exceptions if:

- The object doesn't exist (`NotFoundError`)
- The property name is invalid for the collection (`NameError`)
- The property doesn't exist for the object (`NotFoundError`)
- The specified scenario doesn't exist (`NotFoundError`)

```python
from plexosdb.exceptions import NotFoundError, NameError

try:
    db.delete_property(ClassEnum.Generator, "NonexistentGen", property_name="Max Capacity")
except NotFoundError as e:
    print(f"Error: {e}")

try:
    db.delete_property(ClassEnum.Generator, "PowerPlant1", property_name="Invalid Property")
except NameError as e:
    print(f"Error: {e}")
```

## Best Practices

1. **Backup data**: Always backup your database before performing bulk deletions
2. **Verify existence**: Check that objects and properties exist before attempting deletion
3. **Use transactions**: For complex operations, wrap deletions in database transactions
4. **Scenario management**: Be specific about scenarios when deleting scenario-based properties

```python
# Example of safe deletion with verification
if db.check_object_exists(ClassEnum.Generator, "PowerPlant1"):
    properties = db.get_object_properties(ClassEnum.Generator, "PowerPlant1")
    property_names = [p["property"] for p in properties]

    if "Max Capacity" in property_names:
        db.delete_property(ClassEnum.Generator, "PowerPlant1", property_name="Max Capacity")
        print("Property deleted successfully")
    else:
        print("Property not found")
else:
    print("Object not found")
```
