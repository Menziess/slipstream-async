"""Core module."""

import logging
from asyncio import gather, sleep
from collections.abc import AsyncIterable
from inspect import isasyncgenfunction, signature
from re import sub
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
    Iterable,
    Optional,
    Union,
)

try:
    from aiokafka import (
        AIOKafkaClient,
        AIOKafkaConsumer,
        AIOKafkaProducer,
        ConsumerRecord,
    )
    from aiokafka.helpers import create_ssl_context
except ModuleNotFoundError:
    print('Install aiokafka or slipstream-async[kafka]')
    raise

from slipstream.interfaces import ICache, ICodec
from slipstream.utils import (
    PubSub,
    Singleton,
    get_params_names,
    iscoroutinecallable,
)

KAFKA_CLASSES_PARAMS = {
    **get_params_names(AIOKafkaConsumer),
    **get_params_names(AIOKafkaProducer),
    **get_params_names(AIOKafkaClient),
}
READ_FROM_START = -2
READ_FROM_END = -1

logger = logging.getLogger(__name__)


class Conf(metaclass=Singleton):
    """Define default kafka configuration, optionally.

    >>> Conf({'bootstrap_servers': 'localhost:29091'})
    {'bootstrap_servers': 'localhost:29091'}
    """

    pubsub = PubSub()
    topics: list['Topic'] = []
    iterables: set[tuple[str, AsyncIterable]] = set()

    def register_topic(self, topic: 'Topic'):
        """Add topic to global conf."""
        self.topics.append(topic)

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
        handler: Union[
            Callable[..., Awaitable[None]],
            Callable[..., None]
        ]
    ):
        """Add handler to global Conf."""
        self.pubsub.subscribe(key, handler)

    async def _start(self, **kwargs):
        try:
            await gather(*[
                self._distribute_messages(key, it, kwargs)
                for key, it in self.iterables
            ])
        except KeyboardInterrupt:
            pass
        finally:
            await self._shutdown()

    async def _shutdown(self) -> None:
        # When the program immediately crashes give chance for topic
        # consumer and producer to be fully initialized before
        # shutting them down
        await sleep(0.05)
        for t in self.topics:
            await t._shutdown()

    async def _distribute_messages(self, key, it, kwargs):
        async for msg in it:
            await self.pubsub.apublish(key, msg, **kwargs)

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
    """Act as a consumer and producer.

    >>> topic = Topic('emoji', {
    ...     'bootstrap_servers': 'localhost:29091',
    ...     'auto_offset_reset': 'earliest',
    ...     'group_id': 'demo',
    ... })

    Loop over topic (iterable) to consume from it:

    >>> async for msg in topic:               # doctest: +SKIP
    ...     print(msg.value)

    Call topic (callable) with data to produce to it:

    >>> await topic({'msg': 'Hello World!'})  # doctest: +SKIP
    """

    def __init__(
        self,
        name: str,
        conf: dict = {},
        offset: Optional[int] = None,
        codec: Optional[ICodec] = None,
        dry: bool = False,
    ):
        """Create topic instance to produce and consume messages."""
        c = Conf()
        c.register_topic(self)
        self.name = name
        self.conf = {**c.conf, **conf}
        self.starting_offset = offset
        self.codec = codec
        self.dry = dry

        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None

        if diff := set(self.conf).difference(KAFKA_CLASSES_PARAMS):
            logger.warning(
                f'Unexpected Topic {self.name} conf entries: {",".join(diff)}')

        if (
            self.conf.get('security_protocol') in ('SSL', 'SASL_SSL')
            and not self.conf.get('ssl_context')
        ):
            self.conf['ssl_context'] = create_ssl_context()

    @property
    async def admin(self) -> AIOKafkaClient:
        """Get started instance of Kafka admin client."""
        params = get_params_names(AIOKafkaClient)
        return AIOKafkaClient(**{
            k: v
            for k, v in self.conf.items()
            if k in params
        })

    async def seek(self, offset: int, consumer: Optional[AIOKafkaConsumer]):
        """Seek to offset."""
        if not (c := consumer or self.consumer):
            raise RuntimeError('No consumer provided.')
        partitions = c.assignment()
        if offset < READ_FROM_START:
            raise ValueError(f'Offset must be bigger than: {READ_FROM_START}.')
        if offset == READ_FROM_START:
            await c.seek_to_beginning(*partitions)
        elif offset == READ_FROM_END:
            await c.seek_to_end(*partitions)
        else:
            for p in partitions:
                c.seek(p, offset)

    async def get_consumer(self) -> AIOKafkaConsumer:
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
        if self.starting_offset:
            try:
                await self.seek(self.starting_offset, consumer)
            except Exception:
                await consumer.stop()
                raise
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

    async def __call__(
        self,
        key,
        value,
        headers: Optional[dict[str, str]] = None,
        **kwargs
    ) -> None:
        """Produce message to topic."""
        if isinstance(key, str) and not self.conf.get('key_serializer'):
            key = key.encode()
        if isinstance(value, str) and not self.conf.get('value_serializer'):
            value = value.encode()
        headers_list = [
            (k, v.encode())
            for k, v in headers.items()
        ] if headers else None
        if self.dry:
            logger.warning(
                f'Skipped sending message to {self.name} [dry=True].'
            )
            return
        if not self.producer:
            self.producer = await self.get_producer()
        try:
            await self.producer.send_and_wait(
                self.name,
                key=key,
                value=value,
                headers=headers_list,
                **kwargs
            )
        except Exception as e:
            logger.error(
                f'Error raised while producing to Topic {self.name}: '
                f'{e.args[0]}' if e.args else ''
            )
            raise

    async def __aiter__(self) -> AsyncIterator[ConsumerRecord]:
        """Iterate over messages from topic."""
        if not self.consumer:
            self.consumer = await self.get_consumer()
        try:
            async for msg in self.consumer:
                if (
                    isinstance(msg.key, bytes)
                    and not self.conf.get('key_deserializer')
                ):
                    msg.key = msg.key.decode()
                if (
                    isinstance(msg.value, bytes)
                    and not self.conf.get('value_deserializer')
                ):
                    msg.value = msg.value.decode()
                yield msg
        except Exception as e:
            logger.error(
                f'Error raised while consuming from Topic {self.name}: '
                f'{e.args[0]}' if e.args else ''
            )
            raise

    async def __next__(self):
        """Get the next message from topic."""
        iterator = self.__aiter__()
        return await anext(iterator)

    async def _shutdown(self):
        """Cleanup and finalization."""
        for client in (self.consumer, self.producer):
            if not client:
                continue
            try:
                await client.stop()
            except RuntimeError:
                pass


async def _sink_output(
    f: Callable,
    s: Union[
        Callable[..., Awaitable[None]],
        Callable[..., None]
    ],
    output: Any
) -> None:
    is_coroutine = iscoroutinecallable(s)
    if isinstance(s, ICache) and not isinstance(output, tuple):
        raise ValueError(
            f'Cache sink expects: Tuple[key, val] in {f.__name__}.')
    elif isinstance(s, Topic) and not isinstance(output, tuple):
        raise ValueError(
            f'Topic sink expects: Tuple[key, val] in {f.__name__}.')
    elif isinstance(s, (Topic, ICache)):
        await s(*output)  # type: ignore
    else:
        if is_coroutine:
            await s(output)  # type: ignore
        else:
            s(output)


def handle(
    *iterable: AsyncIterable,
    sink: Iterable[Union[
        Callable[..., Awaitable[None]],
        Callable[..., None]]
    ] = []
):
    """Snaps function to stream.

    Ex:
        >>> topic = Topic('demo')                 # doctest: +SKIP
        >>> cache = Cache('state/demo')           # doctest: +SKIP

        >>> @handle(topic, sink=[print, cache])   # doctest: +SKIP
        ... def handler(msg, **kwargs):
        ...     return msg.key, msg.value
    """
    c = Conf()

    def _deco(f) -> Callable[..., Awaitable[None]]:
        parameters = signature(f).parameters.values()
        is_coroutine = iscoroutinecallable(f)
        is_asyncgen = isasyncgenfunction(f)

        async def _handler(msg, **kwargs):
            if is_coroutine and not is_asyncgen:
                if any(p.kind == p.VAR_KEYWORD for p in parameters):
                    output = await f(msg, **kwargs)
                else:
                    output = await f(msg) if parameters else await f()
            else:
                if any(p.kind == p.VAR_KEYWORD for p in parameters):
                    output = f(msg, **kwargs)
                else:
                    output = f(msg) if parameters else f()

            if is_asyncgen:
                async for val in output:
                    for s in sink:
                        await _sink_output(f, s, val)
                return

            for val in output if isinstance(output, Generator) else [output]:
                for s in sink:
                    await _sink_output(f, s, val)

        for it in iterable:
            iterable_key = str(id(it))
            c.register_iterable(iterable_key, it)
            c.register_handler(iterable_key, _handler)
        return _handler

    return _deco


def stream(**kwargs):
    """Start the streams.

    Ex:
        >>> from asyncio import run
        >>> args = {
        ...     'env': 'DEV',
        ... }
        >>> run(stream(**args))
    """
    return Conf()._start(**kwargs)
