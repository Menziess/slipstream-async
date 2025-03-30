"""Slipstream codecs."""

from json import dumps, loads
from typing import Any

from slipstream.interfaces import ICodec


class JsonCodec(ICodec):
    """Serialize/deserialize json messages."""

    def encode(self, obj: Any) -> bytes:
        """Serialize message.

        >>> c = JsonCodec()
        >>> c.encode({'key': 1})
        b'{"key": 1}'
        """
        return dumps(obj, default=str).encode()

    def decode(self, s: bytes) -> object:
        """Deserialize message.

        >>> c = JsonCodec()
        >>> c.decode(b'{"key": 1}')
        {'key': 1}
        """
        return loads(s.decode())
