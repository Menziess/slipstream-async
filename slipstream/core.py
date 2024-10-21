"""Core module."""

import logging
from asyncio import gather
from collections.abc import AsyncIterable
from inspect import signature
from re import sub
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
    Iterable,
    Optional,
)

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord

from slipstream.codecs import ICodec
from slipstream.utils import Singleton, get_params_names

READ_FROM_START = -2
READ_FROM_END = -1

logger = logging.getLogger(__name__)


class Conf(metaclass=Singleton):
    iterables: set[tuple[str, AsyncIterable]] = set()
    handlers: dict[str, set[Callable[..., Awaitable[None]]]] = {}

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
        handler: Callable[..., Awaitable[None]]
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
                await h(msg=msg, kwargs=kwargs)

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

    def __init__(
        self,
        name: str,
        conf: dict = {},
        offset: Optional[int] = None,
        codec: Optional[ICodec] = None,
    ):
        c = Conf()
        self.name = name
        self.conf = {**c.conf, **conf}
        self.codec = codec
        self.starting_offset = offset
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None

    async def get_consumer(self):
        """Get started instance of Kafka consumer."""
        params = get_params_names(AIOKafkaConsumer)
        if self.codec:
            self.conf['value_deserializer'] = self.codec.decode
        consumer = AIOKafkaConsumer(self.name, **{
            k: v
            for k, v in self.conf.items()
            if k in params
        })
        await consumer.start()
        return consumer

    async def get_producer(self):
        """Get started instance of Kafka producer."""
        params = get_params_names(AIOKafkaProducer)
        if self.codec:
            self.conf['value_serializer'] = self.codec.encode
        producer = AIOKafkaProducer(**{
            k: v
            for k, v in self.conf.items()
            if k in params
        })
        await producer.start()
        return producer

    async def __call__(self, key, value) -> None:
        """Produce message to topic."""
        if not self.producer:
            self.producer = await self.get_producer()
        await self.producer.send_and_wait(
            self.name,
            key=key,
            value=value,
        )

    async def __aiter__(self) -> AsyncIterator[ConsumerRecord]:
        """Iterate over messages from topic."""
        if not self.consumer:
            self.consumer = await self.get_consumer()
        async for msg in self.consumer:
            yield msg

    async def __next__(self):
        """Get the next message from topic."""
        iterator = self.__aiter__()
        return await anext(iterator)

    async def __del__(self):
        """Cleanup and finalization."""
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()


async def _sink_output(s: Callable[..., Awaitable[None]], output: Any) -> None:
    if isinstance(output, tuple):
        key, value = output
        await s(key=key, value=value)
    else:
        await s(key=None, value=output)


async def _handle_generator_or_value(
    sink: Iterable[Callable[..., Awaitable[None]]],
    output: Any
) -> None:
    if isinstance(output, Generator):
        for val in output:
            for s in sink:
                await _sink_output(s, val)
    else:
        for s in sink:
            await _sink_output(s, output)


def handle(
    *iterable: AsyncIterable,
    sink: Iterable[Callable[..., Awaitable[None]]] = []
):
    c = Conf()

    def _deco(f):
        async def _handler(msg, kwargs={}):
            parameters = signature(f).parameters.values()
            if any(p.kind == p.VAR_KEYWORD for p in parameters):
                output = f(msg, **kwargs)
            elif parameters:
                output = f(msg)
            else:
                output = f()

            await _handle_generator_or_value(sink, output)

        for it in iterable:
            iterable_key = str(id(it))
            c.register_iterable(iterable_key, it)
            c.register_handler(iterable_key, _handler)
        return _handler

    return _deco


def stream():
    return Conf().start()
