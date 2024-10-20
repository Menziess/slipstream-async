"""Slipstream codecs."""

import logging
from abc import ABCMeta, abstractmethod
from json import dumps, loads
from typing import Any

logger = logging.getLogger(__name__)


def deserialize_json(msg: bytes) -> dict:
    """Deserialize json message."""
    return loads(msg.decode())


def serialize_json(msg: dict) -> bytes:
    """Serialize json message."""
    dumped = dumps(msg, default=str)
    return dumped.encode()


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
        return serialize_json(obj)

    def decode(self, s: bytes) -> object:
        """Deserialize message."""
        return deserialize_json(s)
