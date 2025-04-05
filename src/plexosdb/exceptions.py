# ruff: noqa: D100, D101
class NotFoundError(Exception):
    pass


class MultlipleElementsError(Exception):
    pass


class ModelError(Exception):
    pass


class MultipleFilesError(Exception):
    pass


class NameError(ValueError):
    pass


class NoPropertiesError(Exception):
    pass
