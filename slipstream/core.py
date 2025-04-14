"""Core module."""

import logging
from asyncio import gather, sleep, wait_for
from collections.abc import AsyncIterable
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

from slipstream.interfaces import ICache, ICodec
from slipstream.utils import (
    AsyncCallable,
    AsyncSynchronizedGenerator,
    PubSub,
    Signal,
    Singleton,
    get_param_names,
    iscoroutinecallable,
)

aiokafka_available = False

try:
    import aiokafka  # noqa: F401  # ruff: noqa # pyright: ignore
    aiokafka_available = True
except ImportError:
    pass

__all__ = [
    'READ_FROM_START',
    'READ_FROM_END',
    'PausableStream',
    'Topic',
]


READ_FROM_START = -2
READ_FROM_END = -1


_logger = logging.getLogger(__name__)


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
        self._iterable: AsyncIterable[Any] = it
        self._iterator: AsyncIterator[Any] | None = None
        self.signal: Signal | Any = None

    def send_signal(self, signal: Signal | Any) -> None:
        """Send signal to stream."""
        self.signal = signal

    def __aiter__(self) -> AsyncIterator[Any]:
        """Create iterator."""
        if self._iterator is None:
            if hasattr(self._iterable, 'asend'):
                self._iterator = cast(
                    AsyncGenerator[Any, Signal | Any],
                    self._iterable
                )
            else:
                self._iterator = aiter(self._iterable)
        return self

    async def __anext__(self) -> Any:
        """Consume iterator while it's not paused."""
        it = self._iterator or self.__aiter__()

        if hasattr(it, 'asend'):
            async_gen = cast(AsyncGenerator[Any, Signal | Any], it)

            while True:
                # The generator gets a chance to handle the signal
                msg = await async_gen.asend(self.signal)

                # When the stream is paused and the generator handles
                # the signal, it should yield SENTINEL
                if msg is not Signal.SENTINEL:

                    # Otherwise we assume that the generator does not
                    # handle the pause, so we pause here
                    while self.signal is Signal.PAUSE:
                        await sleep(0.1)
                    return msg
        else:
            msg = await anext(it)
            while self.signal is Signal.PAUSE:
                await sleep(0.1)
            return msg


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
    pipes: dict[AsyncCallable, tuple[str, tuple[AsyncCallable, ...]]] = {}
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
        handler: AsyncCallable,
        *pipe: AsyncCallable
    ):
        """Add handler to global Conf."""
        if pipe:
            self.pipes[handler] = (key, pipe)
        else:
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
            _logger.critical(e)
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
        async def _distribute(stream, kwargs):
            async for msg in stream:
                await self.pubsub.apublish(key, msg, **kwargs)

        if piped_handlers := [
            (handler, v[1])
            for handler, v in self.pipes.items()
            if v[0] == key
        ]:
            s = AsyncSynchronizedGenerator(pausable_stream)
            await gather(_distribute(s, kwargs), *[
                self._pipe(s.copy(), handler, *funcs, **kwargs)
                for handler, funcs in piped_handlers
            ])
        else:
            await _distribute(pausable_stream, kwargs)

    async def _pipe(
        self,
        stream: AsyncIterable,
        handler: AsyncCallable,
        *funcs,
        **kwargs
    ):
        """Push stream through pipe before feeding it to the handler."""
        for func in funcs:
            stream = func(stream)
        async for msg in stream:
            await handler(msg, **kwargs)

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


if aiokafka_available:
    from aiokafka import (  # type: ignore
        AIOKafkaClient,
        AIOKafkaConsumer,
        AIOKafkaProducer,
        ConsumerRecord,
        TopicPartition,
    )
    from aiokafka.helpers import create_ssl_context  # type: ignore

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
            offset: Optional[int | dict[int, int]] = None,
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

            if diff := set(self.conf).difference({
                *get_param_names(AIOKafkaConsumer),
                *get_param_names(AIOKafkaProducer),
                *get_param_names(AIOKafkaClient),
            }):
                _logger.warning(
                    f'Unexpected Topic {self.name} '
                    f'conf entries: {",".join(diff)}'
                )

            if (
                self.conf.get('security_protocol') in ('SSL', 'SASL_SSL')
                and not self.conf.get('ssl_context')
            ):
                self.conf['ssl_context'] = create_ssl_context()

        @property
        async def admin(self) -> AIOKafkaClient:
            """Get started instance of Kafka admin client."""
            params = get_param_names(AIOKafkaClient)
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
            if c is None:
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
                    _logger.info(
                        f'Waiting for partitions '
                        f'{partitions - ready_partitions}'
                    )
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
            params = get_param_names(AIOKafkaConsumer)
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
            params = get_param_names(AIOKafkaProducer)
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
            if isinstance(key, str) and not self.conf.get(
                'key_serializer'
            ):
                key = key.encode()
            if isinstance(value, str) and not self.conf.get(
                'value_serializer'
            ):
                value = value.encode()
            headers_list = [
                (k, v.encode())
                for k, v in headers.items()
            ] if headers else None
            if self.dry:
                _logger.warning(
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
                _logger.error(
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
                            # Future calls to `getmany` will not return
                            # any records from these partitions until
                            # they have been resumed using `resume`
                            consumer.pause(*consumer.assignment())
                            _logger.debug(f'{self.name} paused')
                            while True:
                                signal = yield Signal.SENTINEL
                                if signal is Signal.RESUME:
                                    _logger.debug(f'{self.name} reactivated')
                                    consumer.resume(*consumer.assignment())
                                    break

                                # Send heartbeats through `getmany`
                                await consumer.getmany()
                                await sleep(3)

                except Exception as e:
                    _logger.error(
                        f'Error raised while consuming from Topic '
                        f'{self.name}: {e.args[0] if e.args else ""}'
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
                    _logger.critical(
                        f'Client for topic "{self.name}" failed '
                        f'to shut down in time {client}'
                    )
                except Exception as e:
                    _logger.critical(
                        f'Client for topic "{self.name}" failed '
                        f'to shut down gracefully {client}: {e}'
                    )


async def _sink_output(
    f: Callable[..., Any],
    s: AsyncCallable,
    output: Any
) -> None:
    is_coroutine = iscoroutinecallable(s)
    known_sinks = (Topic, ICache) if aiokafka_available else (ICache,)
    if isinstance(s, known_sinks) and not isinstance(output, tuple):
        raise ValueError(
            f'Sink expects: (key, val) in {f.__name__}, got :{output}.')
    elif isinstance(s, known_sinks):
        await s(*output)
    else:
        if is_coroutine:
            await s(output)
        else:
            s(output)


def handle(
    *iterable: AsyncIterable[Any],
    pipe: Iterable[AsyncCallable] = [],
    sink: Iterable[Callable | AsyncCallable] = []
):
    """Bind sources and sinks to the handler function.

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
            c.register_handler(iterable_key, _handler, *pipe)
        return _handler

    return _deco


def stream(**kwargs: Any):
    """Start processing iterables bound by `handle` function.

    Ex:
        >>> from asyncio import run
        >>> kwargs = {
        ...     'env': 'DEV',
        ... }
        >>> run(stream(**kwargs))                 # doctest: +SKIP
    """
    return Conf().start(**kwargs)
