"""Top level objects."""

from slipstream.__version__ import VERSION
from slipstream.core import Conf, Topic, handle, stream

try:
    from slipstream.caching import Cache
except ImportError:
    pass

__all__ = [
    'VERSION',
    'Conf',
    'Topic',
    'Cache',
    'handle',
    'stream',
]
