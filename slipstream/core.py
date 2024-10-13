"""Core module."""

from asyncio import gather, sleep
from collections.abc import AsyncIterable
from inspect import signature
from re import sub
from typing import Any, Callable, Iterable, Union

from aiokafka import AIOKafkaConsumer
from pubsub import pub


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
    iterables: set[tuple[str, Iterable]] = set()

    def register_iterables(self, *it):
        """Add iterables to global Conf."""
        self.iterables.add(*it)

    async def start(self, **kwargs):
        await gather(*[
            self.distribute_messages(it, kwargs)
            for _, it in self.iterables]
        )

    @staticmethod
    async def async_iterable(it):
        for msg in it:
            await sleep(0.01)
            yield msg

    @staticmethod
    async def distribute_messages(it, kwargs):
        iterable_key = str(id(it))
        if isinstance(it, AsyncIterable):
            async for msg in it:
                pub.sendMessage(iterable_key, msg=msg, kwargs=kwargs)
        else:
            async for msg in Conf.async_iterable(it):
                pub.sendMessage(iterable_key, msg=msg, kwargs=kwargs)

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


def slap(
    *iterable: Union[Iterable, AsyncIterable],
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
            c.register_iterables((iterable_key, it))
            pub.subscribe(_handler, iterable_key)
        return _handler

    return _deco


def stream():
    return Conf().start()
