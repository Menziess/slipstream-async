"""Core module."""

import logging
from asyncio import gather
from collections.abc import AsyncIterable
from inspect import signature
from re import sub
from typing import Any, Callable, Iterable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from slipstream.utils import Singleton, get_params_names

READ_FROM_START = -2
READ_FROM_END = -1

logger = logging.getLogger(__name__)


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

    def __init__(
        self,
        name: str,
        conf: dict = {},
    ):
        c = Conf()
        self.name = name
        self.conf = {**c.conf, **conf}
        self._producer = None
        self._consumer = None

    def get_consumer(self):
        if not self._consumer:
            params = get_params_names(AIOKafkaConsumer)
            self.consumer = AIOKafkaConsumer(self.name, **{
                k: v
                for k, v in self.conf.items()
                if k in params
            })
        return self.consumer

    def get_producer(self):
        if not self._producer:
            params = get_params_names(AIOKafkaProducer)
            self._producer = AIOKafkaProducer(**{
                k: v
                for k, v in self.conf.items()
                if k in params
            })
        return self._producer

    async def __call__(self, key, value):
        p = self.get_producer()
        await p.start()
        try:
            await p.send_and_wait(
                self.name,
                key=key,
                value=value,
            )
        finally:
            await p.stop()

    async def __aiter__(self):
        consumer = self.get_consumer()
        if consumer:
            await consumer.start()
        try:
            async for msg in consumer:
                yield msg
        finally:
            await consumer.stop()

    async def __next__(self):
        iterator = self.__aiter__()
        return await anext(iterator)


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
