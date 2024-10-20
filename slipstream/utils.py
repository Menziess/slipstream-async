"""Slipstream utilities."""

import logging
from inspect import signature
from typing import Any, Dict

logger = logging.getLogger(__name__)


def get_params_names(o: Any):
    """Return function parameters."""
    parameters = signature(o).parameters.values()
    return getattr(parameters, 'mapping')


class Singleton(type):
    """Maintain a single instance of a class."""

    _instances: Dict['Singleton', Any] = {}

    def __init__(cls, name, bases, dct):
        """Perform checks before instantiation."""
        if '__update__' not in dct:
            raise TypeError('Expected __update__.')

    def __call__(cls, *args, **kwargs):
        """Apply metaclass singleton action."""
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton, cls).__call__(*args, **kwargs)
        instance = cls._instances[cls]
        instance.__update__(*args, **kwargs)
        return instance
