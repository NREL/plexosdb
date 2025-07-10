# Working with Scenarios

Scenarios in PLEXOS allow you to model different conditions or assumptions.
PlexosDB provides methods to manage these scenarios.

## Creating Scenarios

Scenarios can be added manually to the database by calling `add_scenario`:

```python
from plexosdb import PlexosDB

# Initialize database
db = PlexosDB()
db.create_schema()

db.add_scenario("TestScenario")
```


## Creating Scenario Properties

Scenarios are automatically created when adding properties with a scenario name:

```python
from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum

# Initialize database
db = PlexosDB()
db.create_schema()

# Create a generator
db.add_object(ClassEnum.Generator, "Generator1")

# Add a property with a scenario - this automatically creates the scenario
db.add_property(
    ClassEnum.Generator,
    "Generator1",
    "Max Capacity",
    100.0,
    scenario="Base Case"
)

# Add another property with a different scenario
db.add_property(
    ClassEnum.Generator,
    "Generator1",
    "Max Capacity",
    120.0,
    scenario="High Demand"
)
```

## Listing Available Scenarios

You can list all scenarios in your model:

```python
# List all scenarios
scenarios = db.list_scenarios()
print(f"Available scenarios: {scenarios}")
```

## Checking if a Scenario Exists

```python
# Check if a scenario exists
if db.check_scenario_exists("Base Case"):
    print("Base Case scenario exists")
else:
    print("Base Case scenario does not exist")
```

## Creating Object-Specific Scenarios

You can create scenarios with specific property values for an object:

```python
# Create a new scenario with specific property values
db.create_object_scenario(
    "Generator1",
    "New Scenario",
    {
        "Max Capacity": 130.0,
        "Min Stable Level": 25.0,
        "Heat Rate": 9.5
    },
    object_class=ClassEnum.Generator,
    description="New scenario for testing",
    base_scenario="Base Case"  # Inherits other properties from Base Case
)
```

## Getting Properties for a Specific Scenario

```python
# Get all properties for an object in a specific scenario
from plexosdb.enums import CollectionEnum

# Get the scenario ID
scenario_id = db.get_scenario_id("High Demand")

# Query properties with this scenario
properties = db.query("""
    SELECT o.name, p.name as property, d.value
    FROM t_data d
    JOIN t_property p ON d.property_id = p.property_id
    JOIN t_membership m ON d.membership_id = m.membership_id
    JOIN t_object o ON m.child_object_id = o.object_id
    JOIN t_tag t ON t.data_id = d.data_id
    WHERE t.object_id = ?
""", (scenario_id,))

for row in properties:
    print(f"{row[0]}: {row[1]} = {row[2]}")
```

## Updating Scenarios

```python
# Update scenario name or description
db.update_scenario(
    "High Demand",
    new_name="Peak Demand",
    new_description="Updated scenario for peak demand periods"
)
```

```{tip}
Scenarios are a powerful way to compare different model assumptions without duplicating your entire dataset.
```
