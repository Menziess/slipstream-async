"""Core module."""

from asyncio import gather
from collections.abc import AsyncIterable
from inspect import signature
from re import sub
from typing import Any, Callable, Iterable

from aiokafka import AIOKafkaConsumer

READ_FROM_START = -2
READ_FROM_END = -1


class Singleton(type):
    """Maintain a single instance of a class."""

    _instances: dict['Singleton', Any] = {}

    def __init__(cls, name, bases, dct):
        """Perform checks before instantiation."""
        if '__update__' not in dct:
            raise TypeError('Expected __update__.')

    def __call__(cls, *args, **kwargs):
        """Apply metaclass singleton action."""
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton, cls).__call__(*args, **kwargs)
        instance = cls._instances[cls]
        instance.__update__(*args, **kwargs)
        return instance


class Conf(metaclass=Singleton):
    iterables: set[tuple[str, AsyncIterable]] = set()
    handlers: dict[str, set[Callable[..., None]]] = {}

    def register_iterable(
        self,
        key: str,
        it: AsyncIterable
    ):
        """Add iterable to global Conf."""
        self.iterables.add((key, it))

    def register_handler(
        self,
        key: str,
        handler: Callable[..., None]
    ):
        """Add handler to global Conf."""
        handlers = self.handlers.get(key, set())
        handlers.add(handler)
        self.handlers[key] = handlers

    async def start(self, **kwargs):
        await gather(*[
            self.distribute_messages(key, it, kwargs)
            for key, it in self.iterables]
        )

    async def distribute_messages(self, key, it, kwargs):
        async for msg in it:
            for h in self.handlers[key]:
                h(msg=msg, kwargs=kwargs)

    def __init__(self, conf: dict = {}) -> None:
        """Define init behavior."""
        self.conf: dict[str, Any] = {}
        self.__update__(conf)

    def __update__(self, conf: dict = {}):
        """Set default app configuration."""
        self.conf = {**self.conf, **conf}
        for key, value in conf.items():
            key = sub('[^0-9a-zA-Z]+', '_', key)
            setattr(self, key, value)

    def __repr__(self) -> str:
        """Represent config."""
        return str(self.conf)


class Topic:

    def __init__(self, bootstrap_servers, topic, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id

    async def __aiter__(self):
        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest'
        )
        if consumer:
            await consumer.start()
        try:
            async for msg in consumer:
                yield msg
        finally:
            await consumer.stop()


def handle(
    *iterable: AsyncIterable,
    sink: Iterable[Callable[..., None]] = []
):
    c = Conf()

    def _deco(f):
        def _handler(msg, kwargs={}):
            parameters = signature(f).parameters.values()
            if any(p.kind == p.VAR_KEYWORD for p in parameters):
                output = f(msg, **kwargs)
            elif parameters:
                output = f(msg)
            else:
                output = f()

            for s in sink:
                s(output)

        for it in iterable:
            iterable_key = str(id(it))
            c.register_iterable(iterable_key, it)
            c.register_handler(iterable_key, _handler)
        return _handler

    return _deco


def stream():
    return Conf().start()
