"""Plexos model enums that define the data schema."""

from enum import Enum, IntEnum, auto
from .utils import StrEnum


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
    def _generate_next_value_(name, start, count, last_values):
        return str(name)

    System = auto()
    Generator = auto()
    Fuel = auto()
    Battery = auto()
    Storage = auto()
    Emission = auto()
    Reserve = auto()
    Region = auto()
    Zone = auto()
    Node = auto()
    Line = auto()
    Transformer = auto()
    Interface = auto()
    DataFile = auto()
    Timeslice = auto()
    Scenario = auto()
    Model = auto()
    Horizon = auto()
    Report = auto()
    PASA = auto()
    MTSchedule = auto()
    STSchedule = auto()
    Transmission = auto()
    Diagnostic = auto()
    Production = auto()
    Performance = auto()
    Variable = auto()


plexos_class_mapping = {enum_member.name: enum_member.value for enum_member in ClassEnum}


class CollectionEnum(StrEnum):
    """Enum that defines the different Plexos colections via Collection Name."""
    def _generate_next_value_(name, start, count, last_values):
        return str(name)

    Generators = auto()
    Fuels = auto()
    HeadStorage = auto()
    TailStorage = auto()
    Nodes = auto()
    Battery = auto()
    Storage = auto()
    Emissions = auto()
    Reserves = auto()
    Batteries = auto()
    Regions = auto()
    Zones = auto()
    Region = auto()
    Zone = auto()
    Lines = auto()
    NodeFrom = auto()
    NodeTo = auto()
    Transformers = auto()
    Interfaces = auto()
    Scenarios = auto()
    Model = auto()
    Scenario = auto()
    Horizon = auto()
    Report = auto()
    PASA = auto()
    MTSchedule = auto()
    STSchedule = auto()
    Transmission = auto()
    Production = auto()
    Diagnostic = auto()
    Performance = auto()
    DataFiles = auto()



def str2enum(string, schema_enum=Schema) -> Schema | None:
    """Convert string to enum."""
    for e in schema_enum:
        if e.name == string:
            return e
    return None
