"""Slipstream codecs."""

import logging
from json import dumps, loads
from typing import Any

from slipstream.interfaces import ICodec

logger = logging.getLogger(__name__)


class JsonCodec(ICodec):
    """Serialize/deserialize json messages."""

    def encode(self, obj: Any) -> bytes:
        """Serialize message."""
        return dumps(obj, default=str).encode()

    def decode(self, s: bytes) -> object:
        """Deserialize message."""
        return loads(s.decode())
