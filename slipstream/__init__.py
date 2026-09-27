"""Top level objects."""

from slipstream.caching import rocksdict_available
from slipstream.checkpointing import Checkpoint
from slipstream.core import Conf, aiokafka_available, handle, stream

__all__ = ['Checkpoint', 'Conf', 'handle', 'stream']

if rocksdict_available:
    from slipstream.caching import Cache as Cache

    __all__ += ['Cache']

if aiokafka_available:
    from slipstream.core import Topic as Topic

    __all__ += ['Topic']
