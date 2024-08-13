"""Util functions for plexosdb."""

import ast
from itertools import islice
from typing import Any

from loguru import logger


def batched(iterable, n):
    """Implement batched iterator.

    https://docs.python.org/3/library/itertools.html#itertools.batched
    """
    it = iter(iterable)
    return iter(lambda: tuple(islice(it, n)), ())


def validate_string(value: str) -> Any:
    """Validate string and convert it to python object.

    This function also tries to parse floats or ints.

    Parameters
    ----------
    value: Any
        String value to be converted to Python Object

    Note
    ----
    The ast is slow due to the multiple cases. Use it only on simple for loops
    as this could become a bottleneck.
    """
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    if value == "true" or value == "TRUE":
        return True
    if value == "false" or value == "FALSE":
        return False
    try:
        value = ast.literal_eval(value)
    except:  # noqa: E722
        logger.trace("Could not parse {}", value)
    finally:
        return value
