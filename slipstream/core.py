"""Core module."""

import logging
from asyncio import Event, Lock, gather, sleep, wait_for
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
    Iterable,
)
from inspect import isasyncgenfunction, signature
from re import sub
from typing import (
    Any,
    ClassVar,
    Literal,
    Protocol,
    cast,
)

from slipstream.interfaces import ICache, ICodec
from slipstream.utils import (
    AsyncCallable,
    AsyncSynchronizedGenerator,
    Pipe,
    PubSub,
    Signal,
    Singleton,
    get_param_names,
    iscoroutinecallable,
)

aiokafka_available = False

try:
    import aiokafka  # noqa: F401

    aiokafka_available = True
except ImportError:  # pragma: no cover
    pass

__all__ = [
    'READ_FROM_END',
    'READ_FROM_START',
    'PausableStream',
    'Topic',
]


READ_FROM_START = -2
READ_FROM_END = -1
PRODUCE_PAUSE_REASON = 'produce'


_logger = logging.getLogger(__name__)


class Handler(Protocol):
    """Callable returned by ``@handle``."""

    source: Any
    sources: tuple[Any, ...]
    checkpoint: Any

    async def __call__(self, msg: Any, **kwargs: Any) -> Any: ...


class PausableStream:
    """Can signal source stream to pause.

    If `it` is of type `AsyncGenerator`, it will receive
    the signal through the yield send syntax in order
    to handle the state change appropriately.
    Alternatively, the `signal` property can be used directly.

    For example, the Topic class uses the signal to pause the Consumer.

    Only ``Signal.PAUSE`` pauses consumption; ``Signal.RESUME``
    resumes it when no other pause reasons remain.

    Pause and resume are tracked per ``reason`` so independent callers
    compose (checkpoint downtime vs produce retry). Use ``counted=True``
    when overlapping pauses share a reason, such as concurrent produces.
    """

    @property
    def iterable(self) -> AsyncIterable[Any]:
        """Get iterable."""
        return self._iterable

    def __init__(self, it: AsyncIterable[Any]) -> None:
        """Create instance that holds iterable and queue to pause it."""
        self._iterable: AsyncIterable[Any] = it
        self._iterator: AsyncIterator[Any] | None = None
        self._pause_counts: dict[str, int] = {}
        self.signal: Signal | Any = None
        self.running: Event = Event()
        self.running.set()

    def send_signal(
        self,
        signal: Signal | Any,
        reason: str = '',
        *,
        counted: bool = False,
    ) -> None:
        """Send signal to stream.

        ``PAUSE``/``RESUME`` compose by ``reason``. A counted reason
        increments on pause and decrements on resume; an uncounted
        reason latches until resumed. Resume is applied only when no
        reasons remain.
        """
        if signal is Signal.PAUSE:
            if counted:
                self._pause_counts[reason] = (
                    self._pause_counts.get(reason, 0) + 1
                )
            else:
                self._pause_counts[reason] = 1
            self.signal = Signal.PAUSE
            if self.running.is_set():
                self.running.clear()
            return
        if signal is Signal.RESUME:
            if counted:
                remaining = self._pause_counts.get(reason, 0) - 1
                if remaining > 0:
                    self._pause_counts[reason] = remaining
                else:
                    self._pause_counts.pop(reason, None)
            else:
                self._pause_counts.pop(reason, None)
            if self._pause_counts:
                return
            self.signal = Signal.RESUME
            if not self.running.is_set():
                self.running.set()
            return
        self.signal = signal

    def __aiter__(self) -> AsyncIterator[Any]:
        """Create iterator."""
        if self._iterator is None:
            if hasattr(self._iterable, 'asend'):
                self._iterator = cast(
                    'AsyncGenerator[Any, Signal | Any]',
                    self._iterable,
                )
            else:
                self._iterator = aiter(self._iterable)
        return self

    async def __anext__(self) -> Any:
        """Consume iterator while it's not paused."""
        it = self._iterator or self.__aiter__()

        if hasattr(it, 'asend'):
            async_gen = cast('AsyncGenerator[Any, Signal | Any]', it)

            while True:
                # The generator gets a chance to handle the signal
                msg = await async_gen.asend(self.signal)

                # When the stream is paused and the generator handles
                # the signal, it should yield SENTINEL
                if msg is not Signal.SENTINEL:
                    # Otherwise we assume that the generator does not
                    # handle the pause, so we pause here
                    if not self.running.is_set():
                        await self.running.wait()
                    return msg
        else:
            msg = await anext(it)
            if not self.running.is_set():
                await self.running.wait()
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
    iterables: ClassVar[dict[str, PausableStream]] = {}
    pipes: ClassVar[dict[AsyncCallable, tuple[str, tuple[Pipe, ...]]]] = {}
    exit_hooks: ClassVar[set[AsyncCallable]] = set()

    def __init__(self, conf: dict[str, Any] | None = None) -> None:
        """Define init behavior."""
        self.conf: dict[str, Any] = {}
        if conf:
            self.__update__(conf)

    def register_iterable(self, key: str, it: AsyncIterable[Any]) -> None:
        """Add iterable to global Conf."""
        self.iterables[key] = PausableStream(it)

    def register_handler(
        self,
        key: str,
        handler: AsyncCallable,
        *pipe: Pipe,
    ) -> None:
        """Add handler to global Conf."""
        if pipe:
            self.pipes[handler] = (key, pipe)
        else:
            self.pubsub.subscribe(key, handler)

    def register_exit_hook(self, exit_hook: AsyncCallable) -> None:
        """Add exit hook that's called on shutdown."""
        self.exit_hooks.add(exit_hook)

    def signal_iterables(
        self,
        signal: Signal,
        reason: str = '',
        *,
        counted: bool = False,
    ) -> None:
        """Send ``signal`` to every registered source stream."""
        for stream in self.iterables.values():
            stream.send_signal(signal, reason, counted=counted)

    async def start(self, **kwargs: Any) -> None:
        """Start processing registered iterables."""
        try:
            await gather(
                *[
                    self._distribute_messages(key, pausable_stream, kwargs)
                    for key, pausable_stream in self.iterables.items()
                ],
            )
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
        kwargs: Any,
    ) -> None:
        """Publish messages from stream."""

        async def _distribute(stream: AsyncIterator[Any], kwargs: Any) -> None:
            async for msg in stream:
                await self.pubsub.apublish(key, msg, **kwargs)

        if piped_handlers := [
            (handler, v[1]) for handler, v in self.pipes.items() if v[0] == key
        ]:
            s = AsyncSynchronizedGenerator(pausable_stream)
            await gather(
                _distribute(s, kwargs),
                *[
                    self._pipe(s.copy(), handler, *funcs, **kwargs)
                    for handler, funcs in piped_handlers
                ],
            )
        else:
            await _distribute(pausable_stream, kwargs)

    async def _pipe(
        self,
        stream: AsyncIterable,
        handler: AsyncCallable,
        *funcs: Callable[..., AsyncIterable[Any]],
        **kwargs: Any,
    ) -> None:
        """Push stream through pipe before feeding it to the handler."""
        for func in funcs:
            stream = func(stream)
        async for msg in stream:
            await handler(msg, **kwargs)

    def __update__(self, conf: dict[str, Any] | None = None) -> None:
        """Set default app configuration."""
        if not conf:
            return
        self.conf = {**self.conf, **conf}
        for key, value in conf.items():
            key = sub('[^0-9a-zA-Z]+', '_', key)
            setattr(self, key, value)

    def __getattr__(self, name: str) -> Any:
        if name in self.conf:
            return self.conf[name]
        err_msg = (
            f'"{self.__class__.__name__}" object has no attribute "{name}"'
        )
        raise AttributeError(err_msg)

    def __repr__(self) -> str:
        """Represent config."""
        return str(self.conf)


if aiokafka_available:
    from aiokafka import (
        AIOKafkaClient,
        AIOKafkaConsumer,
        AIOKafkaProducer,
        ConsumerRecord,
        TopicPartition,
    )
    from aiokafka.errors import KafkaTimeoutError
    from aiokafka.helpers import create_ssl_context

    def is_retriable_produce_error(exc: BaseException) -> bool:
        """Return whether a produce failure is safe to retry.

        Trust aiokafka's ``retriable`` flag so the set cannot drift.
        ``KafkaTimeoutError`` is not flagged retriable but is raised
        when ``send_and_wait`` cannot enqueue before
        ``request_timeout_ms``.
        """
        return bool(getattr(exc, 'retriable', False)) or isinstance(
            exc, KafkaTimeoutError
        )

    class Topic:
        """Act as a consumer and producer.

        >>> topic = Topic(
        ...     'emoji',
        ...     {
        ...         'bootstrap_servers': 'localhost:29091',
        ...         'auto_offset_reset': 'earliest',
        ...         'group_id': 'demo',
        ...     },
        ... )

        Loop over topic (iterable) to consume from it:

        >>> async for msg in topic:  # doctest: +SKIP
        ...     print(msg.value)

        Call topic (callable) with data to produce to it:

        >>> await topic({'msg': 'Hello World!'})  # doctest: +SKIP

        Produce retries default to fail-fast (``produce_retries=0``).
        Set ``produce_retries`` (and optional ``produce_retry_backoff``
        seconds) on ``Conf`` or the topic ``conf`` to retry retriable
        broker errors. Sources are paused via ``Conf.signal_iterables``
        while retrying.
        """

        def __init__(
            self,
            name: str,
            conf: dict[str, Any] | None = None,
            offset: int | dict[int, int] | None = None,
            codec: ICodec | None = None,
            dry: bool = False,
        ) -> None:
            """Create topic instance to produce and consume messages."""
            c = Conf()
            c.register_exit_hook(self.exit_hook)
            self.name = name
            merged = {**c.conf, **(conf or {})}
            self.produce_retries = int(merged.pop('produce_retries', 0))
            self.produce_retry_backoff = float(
                merged.pop('produce_retry_backoff', 1.0)
            )
            self.conf = merged
            self.starting_offset = offset
            self.codec = codec
            self.dry = dry

            self.consumer: AIOKafkaConsumer | None = None
            self.producer: AIOKafkaProducer | None = None
            self._producer_lock = Lock()
            self._generator: (
                AsyncGenerator[
                    Literal[Signal.SENTINEL] | ConsumerRecord[Any, Any],
                    bool | None,
                ]
                | None
            ) = None

            if diff := set(self.conf).difference(
                {
                    *get_param_names(AIOKafkaConsumer),
                    *get_param_names(AIOKafkaProducer),
                    *get_param_names(AIOKafkaClient),
                },
            ):
                log_msg = (
                    f'Unexpected Topic {self.name} '
                    f'conf entries: {",".join(diff)}'
                )
                _logger.warning(log_msg)

            if self.conf.get('security_protocol') in (
                'SSL',
                'SASL_SSL',
            ) and not self.conf.get('ssl_context'):
                self.conf['ssl_context'] = create_ssl_context()

        def _maybe_encode(self, val: Any, serializer: str) -> Any:
            """Encode ``str`` when no serializer is configured."""
            if isinstance(val, str) and not self.conf.get(serializer):
                return val.encode()
            return val

        def _maybe_decode(self, val: Any, deserializer: str) -> Any:
            """Decode ``bytes`` when no deserializer is configured."""
            if isinstance(val, bytes) and not self.conf.get(deserializer):
                return val.decode()
            return val

        @property
        async def admin(self) -> AIOKafkaClient:
            """Get started instance of Kafka admin client."""
            params = get_param_names(AIOKafkaClient)
            return AIOKafkaClient(
                **{k: v for k, v in self.conf.items() if k in params},
            )

        async def seek(
            self,
            offset: int | dict[int, int],
            consumer: AIOKafkaConsumer | None = None,
            timeout: float = 30.0,
        ) -> None:
            """Seek to offset."""
            c = consumer or self.consumer
            if c is None:
                err_msg = 'No consumer provided'
                raise RuntimeError(err_msg)

            if isinstance(offset, int) and offset < READ_FROM_START:
                err_msg = 'Offset must be bigger than -3'
                raise ValueError(err_msg)

            # Wait until all partitions are assigned
            partitions = c.partitions_for_topic(self.name) or set()
            ready_partitions = set()
            max_attempts = int(timeout / 0.1)
            for i in range(max_attempts):
                assignment = c.assignment()
                ready_partitions = {_.partition for _ in assignment}
                if partitions.issubset(ready_partitions):
                    break
                if i % 100 == 0:
                    log_msg = (
                        f'Waiting for partitions '
                        f'{partitions - ready_partitions}'
                    )
                    _logger.info(log_msg)
                await sleep(0.1)
            else:
                err_msg = (
                    f'Failed to assign {partitions} after {timeout}s, '
                    f'got: {ready_partitions}'
                )
                raise RuntimeError(err_msg)

            # The desired offset per partition
            offsets = (
                {TopicPartition(self.name, p): offset for p in partitions}
                if isinstance(offset, int)
                else {
                    TopicPartition(self.name, p): o for p, o in offset.items()
                }
            )

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
            consumer = AIOKafkaConsumer(
                self.name,
                **{k: v for k, v in self.conf.items() if k in params},
            )
            await consumer.start()
            if self.starting_offset is not None:
                try:
                    await self.seek(self.starting_offset, consumer)
                except Exception:
                    await consumer.stop()
                    raise
            return consumer

        async def get_producer(self) -> AIOKafkaProducer:
            """Get started instance of Kafka producer."""
            params = get_param_names(AIOKafkaProducer)
            if self.codec:
                self.conf['value_serializer'] = self.codec.encode
            producer = AIOKafkaProducer(
                **{k: v for k, v in self.conf.items() if k in params},
            )
            await producer.start()
            return producer

        async def __call__(
            self,
            key: Any,
            value: Any,
            headers: dict[str, str] | None = None,
            **kwargs: Any,
        ) -> None:
            """Produce message to topic."""
            key = self._maybe_encode(key, 'key_serializer')
            value = self._maybe_encode(value, 'value_serializer')
            headers_list = (
                [(k, v.encode()) for k, v in headers.items()]
                if headers
                else None
            )
            if self.dry:
                err_msg = f'Skipped sending message to {self.name} [dry=True]'
                _logger.warning(err_msg)
                return
            if not self.producer:
                async with self._producer_lock:
                    if not self.producer:
                        self.producer = await self.get_producer()
            await self._send_with_retry(key, value, headers_list, **kwargs)

        async def _send_with_retry(
            self,
            key: Any,
            value: Any,
            headers_list: list[tuple[str, bytes]] | None,
            **kwargs: Any,
        ) -> None:
            """Send once by default; retry only if ``produce_retries`` > 0."""
            producer = self.producer
            if producer is None:
                err_msg = 'Producer not started'
                raise RuntimeError(err_msg)

            paused = False
            attempts = self.produce_retries + 1
            try:
                for attempt in range(attempts):
                    try:
                        await producer.send_and_wait(
                            self.name,
                            key=key,
                            value=value,
                            headers=headers_list,
                            **kwargs,
                        )
                    except Exception as e:  # noqa: PERF203
                        retriable = (
                            attempt + 1 < attempts
                            and is_retriable_produce_error(e)
                        )
                        if not retriable:
                            err_msg = (
                                f'Error while producing to Topic {self.name}: '
                                f'{e.args[0] if e.args else ""}'
                            )
                            _logger.exception(err_msg)
                            raise RuntimeError(err_msg) from e
                        if not paused:
                            Conf().signal_iterables(
                                Signal.PAUSE,
                                PRODUCE_PAUSE_REASON,
                                counted=True,
                            )
                            paused = True
                        _logger.warning(
                            'Retrying produce to Topic %s (%s/%s) after %s',
                            self.name,
                            attempt + 1,
                            self.produce_retries,
                            e,
                        )
                        if self.produce_retry_backoff > 0:
                            await sleep(self.produce_retry_backoff)
                    else:
                        return
            finally:
                if paused:
                    Conf().signal_iterables(
                        Signal.RESUME,
                        PRODUCE_PAUSE_REASON,
                        counted=True,
                    )

        async def _get_generator(
            self,
            consumer: AIOKafkaConsumer,
        ) -> AsyncGenerator[
            Literal[Signal.SENTINEL] | ConsumerRecord[Any, Any],
            bool | None,
        ]:
            """Return generator that iterates over messages from topic."""
            signal = None
            try:
                msg: ConsumerRecord[Any, Any]
                async for msg in consumer:
                    msg.key = self._maybe_decode(msg.key, 'key_deserializer')
                    msg.value = self._maybe_decode(
                        msg.value, 'value_deserializer'
                    )

                    signal = yield msg

                    if signal is Signal.PAUSE:
                        consumer.pause(*consumer.assignment())
                        _logger.info(f'{self.name} paused')
                        while True:
                            signal = yield Signal.SENTINEL
                            if signal is Signal.RESUME:
                                _logger.info(f'{self.name} reactivated')
                                consumer.resume(*consumer.assignment())
                                break
                            await sleep(3)

            except Exception as e:
                err_msg = (
                    f'Error while consuming from Topic {self.name}: '
                    f'{e.args[0] if e.args else ""}'
                )
                _logger.exception(err_msg)
                raise RuntimeError(err_msg) from e

        async def init_generator(
            self,
        ) -> AsyncGenerator[
            Literal[Signal.SENTINEL] | ConsumerRecord[Any, Any],
            bool | None,
        ]:
            """Initialize generator."""
            if not self.consumer:
                self.consumer = await self.get_consumer()
            if not self._generator:
                return self._get_generator(self.consumer)
            return self._generator

        async def __aiter__(self) -> AsyncIterator[ConsumerRecord[Any, Any]]:
            """Iterate over messages from topic."""
            if not self._generator:
                self._generator = await self.init_generator()
            async for msg in self._generator:
                if msg is not Signal.SENTINEL:
                    yield msg

        async def asend(self, value: Any) -> ConsumerRecord[Any, Any]:
            """Send data to generator."""
            if not self._generator:
                self._generator = await self.init_generator()
            generator = cast(
                'AsyncGenerator[ConsumerRecord[Any, Any], Signal | None]',
                self._generator,
            )
            return await generator.asend(value)

        async def __anext__(self) -> ConsumerRecord[Any, Any]:
            """Get the next message from topic."""
            if not self._generator:
                self._generator = await self.init_generator()
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
                    log_msg = (
                        f'Client for topic "{self.name}" failed '
                        f'to shut down in time {client}'
                    )
                    _logger.critical(log_msg)
                except Exception as e:  # noqa: BLE001
                    log_msg = (
                        f'Client for topic "{self.name}" failed '
                        f'to shut down gracefully {client}: {e}'
                    )
                    _logger.critical(log_msg)


async def _sink_output(
    f: Callable[..., Any],
    s: AsyncCallable,
    output: Any,
) -> None:
    """Sink output depending on sink type."""
    is_coroutine = iscoroutinecallable(s)
    known_sinks = (Topic, ICache) if aiokafka_available else (ICache,)
    if isinstance(s, known_sinks) and not isinstance(output, tuple):
        err_msg = f'Sink expects: (key, val) in {f.__name__}, got :{output}'
        raise TypeError(err_msg)
    if isinstance(s, known_sinks):
        await s(*output)
    elif is_coroutine:
        await s(output)
    else:
        s(output)


def _get_processor(
    f: AsyncCallable,
    is_asyncgen: bool,
    sink: Iterable[Callable | AsyncCallable],
) -> AsyncCallable:
    """Process output depending on output type."""

    async def _process_output(output: Any) -> None:
        """Process and route output to sinks."""
        if is_asyncgen:
            async for val in cast('AsyncIterator[Any]', output):
                for s in sink:
                    await _sink_output(f, s, val)
        elif isinstance(output, Generator):
            for val in cast('Generator[Any, Any, Any]', output):
                for s in sink:
                    await _sink_output(f, s, val)
        else:
            for s in sink:
                await _sink_output(f, s, output)

    return _process_output


def _call_kwargs(
    params: Iterable[Any],
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Forward ``**kwargs``, or only ``downtime`` when that param exists."""
    params = list(params)
    if any(p.kind == p.VAR_KEYWORD for p in params):
        return kwargs
    named = {
        p.name
        for p in params
        if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
    }
    return {
        k: v
        for k, v in kwargs.items()
        if k in named and k in ('downtime', 'checkpoint')
    }


def _get_handler(
    f: AsyncCallable, sink: Iterable[Callable | AsyncCallable]
) -> Callable[..., Awaitable[Any]]:
    """Get handler wrapper depending on handler signature."""
    params = signature(f).parameters.values()
    is_coroutine = iscoroutinecallable(f)
    is_asyncgen = isasyncgenfunction(f)

    _processor = _get_processor(f, is_asyncgen, sink)

    if is_coroutine and not is_asyncgen:

        async def _handler(msg: Any, **kwargs: Any) -> None:
            """Execute function and handle its output."""
            extra = _call_kwargs(params, kwargs)
            if not params:
                output = await f()
            elif extra:
                output = await f(msg, **extra)
            else:
                output = await f(msg)
            await _processor(output)
    else:

        async def _handler(msg: Any, **kwargs: Any) -> None:
            """Execute function and handle its output."""
            extra = _call_kwargs(params, kwargs)
            if not params:
                output = f()
            elif extra:
                output = f(msg, **extra)
            else:
                output = f(msg)
            await _processor(output)

    return _handler


def handle(
    *iterable: AsyncIterable[Any],
    pipe: Iterable[Pipe] = [],
    sink: Iterable[Callable | AsyncCallable] = [],
) -> Callable[[AsyncCallable], Handler]:
    """Bind sources and sinks to the handler function.

    Ex:
        >>> topic = Topic('demo')  # doctest: +SKIP
        >>> cache = Cache('state/demo')  # doctest: +SKIP

        >>> @handle(topic, sink=[print, cache])  # doctest: +SKIP
        ... def handler(msg, **kwargs):
        ...     return msg.key, msg.value

    """
    c = Conf()

    def _deco(f: AsyncCallable) -> Handler:
        from slipstream.checkpointing import Checkpoint, bind_checkpoint

        handler: Any = _get_handler(f, sink)
        handler.checkpoint = None
        sources: list[Any] = []
        for item in iterable:
            if isinstance(item, Checkpoint):
                source = item.dependent
                handler = bind_checkpoint(f, handler, item)
            else:
                source = item
            sources.append(source)
            iterable_key = str(id(source))
            c.register_iterable(iterable_key, source)
            c.register_handler(iterable_key, handler, *pipe)
        handler.source = sources[0] if sources else None
        handler.sources = tuple(sources)
        return cast('Handler', handler)

    return _deco


def stream(**kwargs: Any) -> Coroutine[None, None, None]:
    """Start processing iterables bound by `handle` function.

    Ex:
        >>> from asyncio import run
        >>> kwargs = {
        ...     'env': 'DEV',
        ... }
        >>> run(stream(**kwargs))  # doctest: +SKIP
    """
    return Conf().start(**kwargs)
