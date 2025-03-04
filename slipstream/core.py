"""Core module."""

import logging
from asyncio import gather, sleep, wait_for
from collections.abc import AsyncIterable
from enum import Enum
from inspect import isasyncgenfunction, signature
from re import sub
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
    Iterable,
    Literal,
    Optional,
    cast,
)

from aiokafka import TopicPartition

try:
    from aiokafka import (  # type: ignore
        AIOKafkaClient,
        AIOKafkaConsumer,
        AIOKafkaProducer,
        ConsumerRecord,
    )
    from aiokafka.helpers import create_ssl_context  # type: ignore
except ModuleNotFoundError:
    print('Install aiokafka or slipstream-async[kafka]')
    raise

from slipstream.interfaces import ICache, ICodec
from slipstream.utils import (
    AsyncCallable,
    PubSub,
    Singleton,
    get_params_names,
    iscoroutinecallable,
)

KAFKA_CLASSES_PARAMS: dict[str, Any] = {
    **get_params_names(AIOKafkaConsumer),
    **get_params_names(AIOKafkaProducer),
    **get_params_names(AIOKafkaClient),
}
READ_FROM_START = -2
READ_FROM_END = -1


class Signal(Enum):
    """Signals can be exchanged with streams.

    SENTINEL represents an absent yield value
    PAUSE    represents the signal to pause stream
    RESUME   represents the signal to resume stream
    """

    SENTINEL = 0
    PAUSE = 1
    RESUME = 2


logger = logging.getLogger(__name__)


class PausableStream:
    """Can signal source stream to pause.

    If `it` is of type `AsyncGenerator`, it will receive
    the signal through the yield send syntax in order
    to handle the state change appropriately.
    Alternatively, the `signal` property can be used directly.

    For example, the Topic class uses the signal to pause the Consumer.

    Any value can be sent as a Signal, but only Signal.PAUSE will trigger
    a pause in consumption of the iterable in PausableStream. Any other
    value will resume the PausableStream.
    """

    def __init__(self, it: AsyncIterable[Any]):
        """Create instance that holds iterable and queue to pause it."""
        self._it = it
        self.signal: Signal | Any = None

    def send_signal(self, signal: Signal | Any) -> None:
        """Send signal to stream."""
        self.signal = signal

    async def __aiter__(self) -> AsyncIterator[Any]:
        """Consume iterator while it's not paused."""
        if hasattr(self._it, 'asend'):
            it = cast(AsyncGenerator[Any, Signal | Any], self._it)
            while True:
                try:
                    # The generator gets a chance to handle the signal
                    msg = await it.asend(self.signal)

                    # When the stream is paused and the generator handles
                    # the signal, it should yield SENTINEL
                    if msg is not Signal.SENTINEL:

                        # Otherwise we assume that the generator does not
                        # handle the pause, so we pause here
                        while self.signal is Signal.PAUSE:
                            await sleep(0.1)

                        yield msg

                except StopAsyncIteration:
                    break
        else:
            async for msg in self._it:
                yield msg
                while True:
                    if self.signal is Signal.PAUSE:
                        await sleep(0.1)
                    else:
                        break


class Conf(metaclass=Singleton):
    """The application configuration singleton.

    Register iterables (sources) and handlers (sinks):
    >>> from slipstream import handle

    >>> async def messages():
    ...     for emoji in '🏆📞🐟👌':
    ...         yield emoji

    >>> @handle(messages(), sink=[print])
    ... def handle_message(msg):
    ...     yield f'Hello {msg}!'

    Set application kafka configuration (optional):
    >>> Conf({'bootstrap_servers': 'localhost:29091'})
    {'bootstrap_servers': 'localhost:29091'}

    Provide exit hooks:
    >>> async def exit_hook():
    ...     print('Shutting down application.')

    >>> c = Conf()
    >>> c.register_exit_hook(exit_hook)
    """

    pubsub = PubSub()
    iterables: dict[str, PausableStream] = {}
    exit_hooks: set[AsyncCallable] = set()

    def register_iterable(
        self,
        key: str,
        it: AsyncIterable[Any]
    ):
        """Add iterable to global Conf."""
        self.iterables[key] = PausableStream(it)

    def register_handler(
        self,
        key: str,
        handler: AsyncCallable
    ):
        """Add handler to global Conf."""
        self.pubsub.subscribe(key, handler)

    def register_exit_hook(
        self,
        exit_hook: AsyncCallable
    ):
        """Add exit hook that's called on shutdown."""
        self.exit_hooks.add(exit_hook)

    async def start(self, **kwargs: Any):
        """Start processing registered iterables."""
        try:
            await gather(*[
                self._distribute_messages(key, pausable_stream, kwargs)
                for key, pausable_stream in self.iterables.items()
            ])
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.critical(e)
            raise
        finally:
            await self._shutdown()

    async def _shutdown(self) -> None:
        """Call exit hooks."""
        # When the program immediately crashes give chance for objects
        # to be fully initialized before shutting them down
        await sleep(0.05)
        for hook in self.exit_hooks:
            await hook()

    async def _distribute_messages(
        self,
        key: str,
        pausable_stream: PausableStream,
        kwargs: Any
    ):
        """Publish messages from stream."""
        async for msg in pausable_stream:
            await self.pubsub.apublish(key, msg, **kwargs)

    def __init__(self, conf: dict[str, Any] = {}) -> None:
        """Define init behavior."""
        self.conf: dict[str, Any] = {}
        self.__update__(conf)

    def __update__(self, conf: dict[str, Any] = {}):
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
        conf: dict[str, Any] = {},
        offset: Optional[int] = None,
        codec: Optional[ICodec] = None,
        dry: bool = False,
    ):
        """Create topic instance to produce and consume messages."""
        c = Conf()
        c.register_exit_hook(self.exit_hook)
        self.name = name
        self.conf = {**c.conf, **conf}
        self.starting_offset = offset
        self.codec = codec
        self.dry = dry

        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self._generator: Optional[
            AsyncGenerator[
                Literal[Signal.SENTINEL] | ConsumerRecord[Any, Any],
                bool | None
            ]
        ] = None

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

    async def seek(
        self,
        offset: int | dict[int, int],
        consumer: Optional[AIOKafkaConsumer] = None,
        timeout: float = 30.0
    ):
        """Seek to offset."""
        c = consumer or self.consumer
        if not c:
            raise RuntimeError('No consumer provided.')

        if isinstance(offset, int) and offset < READ_FROM_START:
            raise ValueError('Offset must be bigger than -3.')

        # Wait until all partitions are assigned
        partitions = c.partitions_for_topic(self.name) or set()
        ready_partitions = set()
        max_attempts = int(timeout / 0.1)
        for i in range(max_attempts):
            assignment = c.assignment()
            ready_partitions = set(_.partition for _ in c.assignment())
            if partitions.issubset(ready_partitions):
                break
            if i % 100 == 0:
                logger.info(
                    f'Waiting for partitions {partitions - ready_partitions}')
            await sleep(0.1)
        else:
            raise RuntimeError(
                f'Failed to assign {partitions} after {timeout}s, '
                f'got: {ready_partitions}'
            )

        # The desired offset per partition
        offsets = {
            TopicPartition(self.name, p): offset
            for p in partitions
        } if isinstance(offset, int) else {
            TopicPartition(self.name, p): o
            for p, o in offset.items()
        }

        # Perform seek
        if offset == READ_FROM_START:
            await c.seek_to_beginning(*assignment)
        elif offset == READ_FROM_END:
            await c.seek_to_end(*assignment)
        else:
            for p, o in offsets.items():
                c.seek(p, o)

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
        key: Any,
        value: Any,
        headers: Optional[dict[str, str]] = None,
        **kwargs: Any
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
                f'Skipped sending message to {self.name} [dry=True]'
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
                f'{e.args[0] if e.args else ""}'
            )
            raise

    async def init_generator(self) -> AsyncGenerator[
        Literal[Signal.SENTINEL] | ConsumerRecord[Any, Any],
        bool | None
    ]:
        """Iterate over messages from topic."""
        if not self.consumer:
            self.consumer = await self.get_consumer()

        async def generator(consumer: AIOKafkaConsumer):
            signal = None
            try:
                msg: ConsumerRecord[Any, Any]
                async for msg in consumer:
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

                    signal = yield msg

                    if signal is Signal.PAUSE:
                        consumer.pause(*consumer.assignment())
                        logger.debug(f'{self.name} paused')
                        while True:
                            signal = yield Signal.SENTINEL
                            if signal is Signal.RESUME:
                                logger.debug(f'{self.name} reactivated')
                                consumer.resume(*consumer.assignment())
                                break

                            # Send heartbeats through getmany
                            await consumer.getmany()
                            await sleep(1)

            except Exception as e:
                logger.error(
                    f'Error raised while consuming from Topic {self.name}: '
                    f'{e.args[0] if e.args else ""}'
                )
                raise

        if not self._generator:
            self._generator = generator(self.consumer)
        return self._generator

    async def __aiter__(self) -> AsyncIterator[ConsumerRecord[Any, Any]]:
        """Iterate over messages from topic."""
        if not self._generator:
            await self.init_generator()
        if not self._generator:
            raise RuntimeError('Topic generator was unset.')
        async for msg in self._generator:
            if msg is not Signal.SENTINEL:
                yield msg

    async def asend(self, value) -> ConsumerRecord[Any, Any]:
        """Send data to generator."""
        if not self._generator:
            await self.init_generator()
        if not self._generator:
            raise RuntimeError('Topic generator was unset.')
        generator = cast(
            AsyncGenerator[ConsumerRecord[Any, Any], Signal | None],
            self._generator
        )
        return await generator.asend(value)

    async def __next__(self) -> ConsumerRecord[Any, Any]:
        """Get the next message from topic."""
        if not self._generator:
            await self.init_generator()
        if not self._generator:
            raise RuntimeError('Topic generator was unset.')
        while (msg := await anext(self._generator)) is Signal.SENTINEL:
            continue
        return msg

    async def exit_hook(self) -> None:
        """Cleanup and finalization."""
        for client in (self.consumer, self.producer):
            if not client:
                continue
            try:
                await wait_for(client.stop(), timeout=10)
            except TimeoutError:
                logger.critical(
                    f'Client for topic "{self.name}" failed '
                    f'to shut down in time {client}'
                )
            except Exception as e:
                logger.critical(
                    f'Client for topic "{self.name}" failed '
                    f'to shut down gracefully {client}: {e}'
                )


async def _sink_output(
    f: Callable[..., Any],
    s: AsyncCallable,
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
    *iterable: AsyncIterable[Any],
    sink: Iterable[AsyncCallable] = []
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

    def _deco(f: AsyncCallable) -> Callable[..., Awaitable[Any]]:
        parameters = signature(f).parameters.values()
        is_coroutine = iscoroutinecallable(f)
        is_asyncgen = isasyncgenfunction(f)

        async def _handler(msg: Any, **kwargs: Any):
            """Pass msg depending on user handler function type."""
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

            # If function is async generator, loop over yielded values
            if is_asyncgen:
                async for val in cast(AsyncIterator[Any], output):
                    for s in sink:
                        await _sink_output(f, s, val)
                return

            # Process regular generator
            if isinstance(output, Generator):
                for val in cast(Generator[Any, Any, Any], output):
                    for s in sink:
                        await _sink_output(f, s, val)
                return

            # Process return value
            for s in sink:
                await _sink_output(f, s, output)

        for it in iterable:
            iterable_key = str(id(it))
            c.register_iterable(iterable_key, it)
            c.register_handler(iterable_key, _handler)
        return _handler

    return _deco


def stream(**kwargs: Any):
    """Start the streams.

    Ex:
        >>> from asyncio import run
        >>> args = {
        ...     'env': 'DEV',
        ... }
        >>> run(stream(**args))                   # doctest: +SKIP
    """
    return Conf().start(**kwargs)
