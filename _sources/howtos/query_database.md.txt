# Querying the Database

PlexosDB provides various methods to efficiently retrieve data from your PLEXOS model.

## Getting Object Properties

Retrieve properties for a specific object:

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum

# Initialize database from existing file
db = PlexosDB.from_xml("/path/to/model.xml")

# Get all properties for a generator
properties = db.get_object_properties(
    ClassEnum.Generator,
    "Generator1"
)

# Print the properties
for prop in properties:
    print(f"{prop['property']}: {prop['value']} {prop['unit'] or ''}")
    if prop['scenario']:
        print(f"  Scenario: {prop['scenario']}")
```

## Filtering Properties

You can filter properties by name:

```python
# Get specific properties
capacity_props = db.get_object_properties(
    ClassEnum.Generator,
    "Generator1",
    property_names=["Max Capacity", "Min Stable Level"]
)
```

## Direct SQL Queries

For advanced querying, you can use direct SQL:

```python
# Execute a custom SQL query
results = db.query("""
    SELECT o.name, p.name as property, d.value
    FROM t_data d
    JOIN t_property p ON d.property_id = p.property_id
    JOIN t_membership m ON d.membership_id = m.membership_id
    JOIN t_object o ON m.child_object_id = o.object_id
    JOIN t_class c ON o.class_id = c.class_id
    WHERE c.name = 'Generator'
    LIMIT 10
""")

# Display results
for row in results:
    print(f"{row[0]}: {row[1]} = {row[2]}")
```

## Working with Large Result Sets

For memory-efficient handling of large result sets:

```python
# Use iteration for memory efficiency with large datasets
for row in db.iter_query("""
    SELECT o.name, p.name as property, d.value
    FROM t_data d
    JOIN t_property p ON d.property_id = p.property_id
    JOIN t_membership m ON d.membership_id = m.membership_id
    JOIN t_object o ON m.child_object_id = o.object_id
    WHERE p.name = 'Max Capacity'
"""):
    # Process each row individually
    print(f"{row[0]}: {row[2]}")
```

## Getting Results as Dictionaries

For more readable results, you can get data as dictionaries:

```python
# Get results as dictionaries
results = db.fetchall_dict("""
    SELECT o.name, p.name as property, d.value
    FROM t_data d
    JOIN t_property p ON d.property_id = p.property_id
    JOIN t_membership m ON d.membership_id = m.membership_id
    JOIN t_object o ON m.child_object_id = o.object_id
    LIMIT 5
""")

# Access by column name
for row in results:
    print(f"{row['name']}: {row['property']} = {row['value']}")
```

## Checking Database Structure

You can explore the database structure:

```python
# List all tables
tables = db._db.tables
print(f"Database tables: {tables}")

# List all classes
classes = db.list_classes()
print(f"Available classes: {classes}")

# List all categories for a class
categories = db.list_categories(ClassEnum.Generator)
print(f"Generator categories: {categories}")
```

```{note}
For very large databases, always use iteration methods or add appropriate filters to avoid memory issues.
```
