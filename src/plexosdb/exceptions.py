"""Custom PlexosDB exceptions that highlight domain-specific failures."""


class NotFoundError(Exception):
    """Raised when the database cannot locate a requested entry."""


class MultlipleElementsError(Exception):
    """Raised when a query unexpectedly returns multiple elements."""


class ModelError(Exception):
    """Raised for generic errors related to model relationships."""


class MultipleFilesError(Exception):
    """Raised when multiple files are provided but only one is expected."""


class NameError(ValueError):
    """Raised when an object name is invalid or missing in context."""


class NoPropertiesError(Exception):
    """Raised when a lookup finds no properties for a given object."""


class PropertyError(Exception):
    """Raised when we have a problem with a property."""
