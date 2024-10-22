"""Slipstream codecs."""

import logging
from abc import ABCMeta, abstractmethod
from json import dumps, loads
from typing import Any

logger = logging.getLogger(__name__)


class ICodec(metaclass=ABCMeta):
    """Base class for codecs."""

    @abstractmethod
    def encode(self, obj: Any) -> bytes:
        """Serialize object."""
        raise NotImplementedError

    @abstractmethod
    def decode(self, s: bytes) -> object:
        """Deserialize object."""
        raise NotImplementedError


class JsonCodec(ICodec):
    """Serialize/deserialize json messages."""

    def encode(self, obj: Any) -> bytes:
        """Serialize message."""
        return dumps(obj, default=str).encode()

    def decode(self, s: bytes) -> object:
        """Deserialize message."""
        return loads(s.decode())
