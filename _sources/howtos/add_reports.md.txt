# How to Add Report Configurations

This guide demonstrates how to add report configurations to your PLEXOS model using the PlexosDB API.

## Basic Report Configuration

Reports in PLEXOS define what data will be available for post-processing after simulation runs. Each report must be associated with a Report object and specify which properties should be reported from which collections.

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum, CollectionEnum

# Create or open a database
db = PlexosDB()
db.create_schema()

# First, create a Report object
db.add_object(ClassEnum.Report, "Generator Outputs")

# Add a report configuration
db.add_report(
    object_name="Generator Outputs",  # Name of the report object
    property="Generation",            # Property to report
    collection=CollectionEnum.Generators,  # Collection containing the property
    parent_class=ClassEnum.System,    # Parent class for the collection
    child_class=ClassEnum.Generator,  # Child class for the collection
    phase_id=4,                       # Phase ID (4=LT, Long Term)
    report_period=True,               # Include period data
    report_summary=True,              # Include summary data
    report_statistics=False,          # Don't include statistics
    report_samples=False,             # Don't include samples
    write_flat_files=False            # Don't write flat files
)
```

## Adding Multiple Report Properties

You can add multiple properties to the same report:

```python
# Create a single report object for generator metrics
db.add_object(ClassEnum.Report, "Generator Performance")

# Add multiple report configurations to it
properties = ["Generation", "Available Capacity", "Generation Cost"]
for prop in properties:
    db.add_report(
        object_name="Generator Performance",
        property=prop,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
        child_class=ClassEnum.Generator,
        report_period=True,
        report_summary=True
    )
```

## Checking Valid Report Properties

Before adding a report, you can check which properties are valid for reporting:

```python
# Make sure the object is added
db.add_object(ClassEnum.Report, "Generator Report")

# List valid properties available for reporting
valid_properties = db.list_valid_properties_report(
    CollectionEnum.Generators,
    parent_class_enum=ClassEnum.System,
    child_class_enum=ClassEnum.Generator
)
print(f"Valid properties for reporting: {valid_properties}")

# Then add reports for selected properties
for prop in ["Generation", "Available Capacity"]:
    if prop in valid_properties:
        db.add_report(
            object_name="Generator Report",
            property=prop,
            collection=CollectionEnum.Generators,
            parent_class=ClassEnum.System,
            child_class=ClassEnum.Generator
        )
    else:
        print(f"Property '{prop}' is not available for reporting")
```

## Understanding Phase IDs

The `phase_id` parameter specifies which simulation phase to create the report for:

- `1`: ST (Short Term)
- `2`: MT (Medium Term)
- `3`: PASA (Projected Assessment of System Adequacy)
- `4`: LT (Long Term)

Choose the appropriate phase for your simulation needs.
