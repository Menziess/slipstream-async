"""Top level objects."""

from slipstream.caching import rocksdict_available
from slipstream.core import Conf, aiokafka_available, handle, stream

if rocksdict_available:
    from slipstream.caching import Cache

if aiokafka_available:
    from slipstream.core import Topic


__all__ = [
    'Cache',
    'Conf',
    'Topic',
    'handle',
    'stream',
]
