# Importing and Exporting Data

PlexosDB provides methods for importing data from XML files and exporting to XML or CSV formats.

## Importing from XML

Create a database from an existing PLEXOS XML file:

```python
from plexosdb import PlexosDB

# Create database from XML
db = PlexosDB.from_xml("/path/to/model.xml")

# Check PLEXOS version of the imported model
version = db.version
print(f"Imported PLEXOS model version: {version}")
```

## Exporting to XML

Export your database to a PLEXOS-compatible XML file:

```python
from pathlib import Path

# Export the entire database to XML
output_path = Path("/path/to/output_model.xml")
success = db.to_xml(output_path)

if success:
    print(f"Model exported successfully to {output_path}")
else:
    print("Export failed")
```

## Importing from CSV

Import data from CSV files:

```python
# Import specific tables from CSV files
db.import_from_csv(
    "/path/to/csv_directory",
    tables=["t_object", "t_data", "t_property"]
)
```

## Exporting to CSV

Export database tables to CSV files:

```python
# Export all tables to CSV
db.to_csv("/path/to/output_directory")

# Export specific tables
db.to_csv(
    "/path/to/output_directory",
    tables=["t_object", "t_data", "t_property"]
)
```

## Database Backup

Create a backup of your in-memory database:

```python
# Backup the database to a file
db.backup_database("/path/to/backup.db")
```

## Creating and Optimizing Databases

```python
# Create an empty database
db = PlexosDB()
db.create_schema()

# After making many changes, optimize the database
db._db.optimize()
```

## Converting Between Formats

Converting from XML to CSV:

```python
# Import from XML then export to CSV
db = PlexosDB.from_xml("/path/to/model.xml")
db.to_csv("/path/to/csv_output")
```

```{warning}
When working with large files, ensure you have sufficient memory and disk space for the operations.
```
