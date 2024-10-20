"""Top level objects."""

from slipstream.__version__ import VERSION
from slipstream.core import Conf, Topic, handle, stream

__all__ = (
    'VERSION',
    'Conf',
    'Topic',
    'handle',
    'stream',
)
