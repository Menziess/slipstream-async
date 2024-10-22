"""Top level objects."""

import uvloop

from slipstream.__version__ import VERSION
from slipstream.caching import Cache
from slipstream.core import Conf, Topic, handle, stream

uvloop.install()

__all__ = (
    'VERSION',
    'Conf',
    'Topic',
    'Cache',
    'handle',
    'stream',
)
