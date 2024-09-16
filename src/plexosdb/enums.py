"""Plexos model enums that define the data schema."""

from enum import Enum, StrEnum


class Schema(Enum):
    """Enum that defines the Plexos Schema."""

    Attributes = ("t_attribute", "attribute_id")
    AttributeData = ("t_attribute_data", "attribute_id")
    Class = ("t_class", "class_id")
    ClassGroup = ("t_class_group", "class_group_id")
    Objects = ("t_object", "object_id")
    Categories = ("t_category", "category_id")
    Collection = ("t_collection", "collection_id")
    CollectionReport = ("t_collection_report", None)
    Memberships = ("t_membership", "membership_id")
    Property = ("t_property", "property_id")
    PropertyGroup = ("t_property_group", "property_group_id")
    PropertyReport = ("t_property_report", None)
    PropertyTag = ("t_property_tag", None)
    Data = ("t_data", "data_id")
    Band = ("t_band", "band_id")
    Report = ("t_report", None)
    DateFrom = ("t_date_from", None)
    DateTo = ("t_date_to", None)
    MemoData = ("t_memo_data", None)
    Message = ("t_message", None)
    Action = ("t_action", None)
    Config = ("t_config", None)
    Tags = ("t_tag", "tag_id")
    Text = ("t_text", "text_id")
    Units = ("t_unit", "unit_id")

    @property
    def name(self):  # noqa: D102
        return self.value[0]

    @property
    def label(self):  # noqa: D102
        return self.value[1]


class ClassEnum(StrEnum):
    """Enum that defines the different Plexos classes."""

    System = "System"
    Generator = "Generator"
    Fuel = "Fuel"
    Battery = "Battery"
    Storage = "Storage"
    Emission = "Emission"
    Reserve = "Reserve"
    Region = "Region"
    Zone = "Zone"
    Node = "Node"
    Line = "Line"
    Transformer = "Transformer"
    Interface = "Interface"
    DataFile = "Data File"
    Timeslice = "Timeslice"
    Scenario = "Scenario"
    Model = "Model"
    Horizon = "Horizon"
    Report = "Report"
    PASA = "PASA"
    MTSchedule = "MTSchedule"
    STSchedule = "STSchedule"
    Transmission = "Transmission"
    Diagnostic = "Diagnostic"
    Production = "Production"
    Performance = "Performance"
    Variable = "Variable"


plexos_class_mapping = {enum_member.name: enum_member.value for enum_member in ClassEnum}


class CollectionEnum(StrEnum):
    """Enum that defines the different Plexos colections via Collection Name."""

    Generators = "Generators"
    Fuels = "Fuels"
    HeadStorage = "HeadStorage"
    TailStorage = "TailStorage"
    Nodes = "Nodes"
    Battery = "Battery"
    Storage = "Storage"
    Emissions = "Emissions"
    Reserves = "Reserves"
    Batteries = "Batteries"
    Regions = "Regions"
    Zones = "Zones"
    Region = "Region"
    Zone = "Zone"
    Lines = "Lines"
    NodeFrom = "NodeFrom"
    NodeTo = "NodeTo"
    Transformers = "Transformers"
    Interfaces = "Interfaces"
    Scenarios = "Scenarios"
    Models = "Models"
    Scenario = "Scenario"
    Horizon = "Horizon"
    Reports = "Reports"
    PASA = "PASA"
    MTSchedule = "MTSchedule"
    STSchedule = "STSchedule"
    Transmission = "Transmission"
    Production = "Production"
    Diagnostic = "Diagnostic"
    Performance = "Performance"
    DataFiles = "DataFiles"


def str2enum(string, schema_enum=Schema) -> Schema | None:
    """Convert string to enum."""
    for e in schema_enum:
        if e.name == string:
            return e
    return None
