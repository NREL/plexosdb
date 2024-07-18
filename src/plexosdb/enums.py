"""Plexos model enums that define the data schema."""

from enum import Enum, IntEnum
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


class ClassEnum(IntEnum):
    """Enum that defines the different Plexos classes."""

    System = 1
    Generator = 2
    Fuel = 4
    Battery = 7
    Storage = 8
    Emission = 10
    Reserve = 14
    Region = 19
    Zone = 21
    Node = 22
    Line = 24
    Transformer = 26
    Interface = 28
    DataFile = 74
    Timeslice = 76
    Scenario = 78
    Model = 80
    Horizon = 82
    Report = 83
    PASA = 87
    MTSchedule = 88
    STSchedule = 89
    Transmission = 90
    Diagnostic = 94
    Production = 91
    Performance = 93
    Variable = 75


plexos_class_mapping = {enum_member.name: enum_member.value for enum_member in ClassEnum}


class CollectionEnum(StrEnum):
    """Enum that defines the different Plexos colections."""

    SystemGenerators = "1"
    GeneratorFuels = "7"
    GeneratorHeadStorage = "10"
    GeneratorTailStorage = "11"
    GeneratorNodes = "12"
    SystemBattery = "79"
    BatteryNodes = "82"
    SystemStorage = "91"
    SystemEmissions = "106"
    EmissionGenerators = "109"
    SystemReserves = "154"
    ReserveGenerators = "157"
    ReserveBatteries = "161"
    ReserveRegions = "165"
    SystemRegions = "196"
    SystemZones = "227"
    SystemNodes = "261"
    NodesRegion = "264"
    NodesZone = "265"
    SystemLines = "283"
    LineNodeFrom = "286"
    LineNodeTo = "287"
    SystemTransformers = "299"
    TransformerNodeFrom = "302"
    TransformerNodeTo = "303"
    SystemInterfaces = "313"
    InterfaceLines = "316"
    SystemScenarios = "700"
    SystemModel = "707"
    ModelScenario = "708"
    ModelHorizon = "710"
    ModelReport = "711"
    ModelPASA = "714"
    ModelMTSchedule = "715"
    ModelSTSchedule = "716"
    ModelTransmission = "718"
    ModelProduction = "719"
    ModelDiagnostic = "722"
    SystemHorizon = "728"
    SystemReport = "729"
    ModelPerformance = "721"
    SystemPASA = "742"
    SystemMTSchedule = "748"
    SystemSTSchedule = "755"
    SystemTransmission = "761"
    SystemProduction = "762"
    SystemPerformance = "764"
    SystemDiagnostic = "765"
    DataFiles = "686"


def str2enum(string, schema_enum=Schema) -> Schema | None:
    """Convert string to enum."""
    for e in schema_enum:
        if e.name == string:
            return e
    return None
