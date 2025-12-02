"""Entrypoint for the PlexosDB client providing its exports."""

from importlib.metadata import version

from loguru import logger

from .db import PlexosDB, PropertyRecord
from .db_manager import SQLiteManager
from .enums import ClassEnum, CollectionEnum
from .xml_handler import XMLHandler

__version__ = version("plexosdb")

logger.disable("r2x_core")

__all__ = (
    "ClassEnum",
    "CollectionEnum",
    "PlexosDB",
    "PropertyRecord",
    "SQLiteManager",
    "XMLHandler",
)
