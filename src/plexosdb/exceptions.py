"""Custom exceptions for R2X."""
# ruff: noqa: D101


class NotFoundError(Exception):
    pass


class MultlipleElementsError(Exception):
    pass


class ModelError(Exception):
    pass


class MultipleFilesError(Exception):
    pass


class PropertyNameError(ValueError):
    """Raised when one or more property names are not valid."""

    pass
