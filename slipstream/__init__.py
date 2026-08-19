"""Top level objects."""

from slipstream.caching import rocksdict_available
from slipstream.checkpointing import Checkpoint
from slipstream.core import Conf, aiokafka_available, handle, stream

if rocksdict_available:
    from slipstream.caching import Cache as Cache

if aiokafka_available:
    from slipstream.core import Topic as Topic

if rocksdict_available and aiokafka_available:
    __all__ = [
        'Cache',
        'Checkpoint',
        'Conf',
        'Topic',
        'handle',
        'stream',
    ]
elif rocksdict_available:
    __all__ = [
        'Cache',
        'Checkpoint',
        'Conf',
        'handle',
        'stream',
    ]
elif aiokafka_available:
    __all__ = [
        'Checkpoint',
        'Conf',
        'Topic',
        'handle',
        'stream',
    ]
else:
    __all__ = [
        'Checkpoint',
        'Conf',
        'handle',
        'stream',
    ]
