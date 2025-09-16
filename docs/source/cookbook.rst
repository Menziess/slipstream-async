Cookbook
========

Slipstream favors user freedom over rigid abstractions, letting you craft framework features in just a few lines. Share `your artistry <https://github.com/Menziess/slipstream-async/discussions/categories/show-and-tell>`_!

Timer
^^^^^

Async generators can be used to trigger handler functions.

::

    from asyncio import run, sleep
    from time import strftime

    from slipstream import handle, stream

    async def timer(interval=1.0):
        while True:
            await sleep(interval)
            yield

    @handle(timer())
    def handler():
        print(strftime('%H:%M:%S', localtime()))

    run(stream())

::

    23:25:10
    23:25:11
    23:25:12
    ...

Iterable
^^^^^^^^

Regular non-async ``Iterables`` don't support parallelism.

Adding ``asyncio.sleep`` allows other coroutines to run during the delay.

::

    from asyncio import sleep

    async def async_iterable(it):
        for msg in it:
            await sleep(0.01)
            yield msg

Source
^^^^^^

Any data source that can be turned into an ``AsyncIterable``, can be bound to a handler function.

**Depends on:** `aiohttp <https://docs.aiohttp.org/en/stable/index.html>`_.

For example: the Wikipedia recent changes streaming API:

::

    from asyncio import run

    from aiohttp import ClientSession

    from slipstream import handle, stream

    URL = 'https://stream.wikimedia.org/v2/stream/recentchange'

    async def read_streaming_api(url):
        async with ClientSession(raise_for_status=True) as session:
            async with session.get(url) as r:
                async for line in r.content:
                    yield line

    @handle(read_streaming_api(URL), sink=[print])
    def handler(msg):
        yield f'Wiki change {msg}!'

    run(stream())

Sharing sources is efficient. But processing delays in one handler may delay consumption for both:

::

    wiki_changes = read_streaming_api(url)

    @handle(wiki_changes)
    def first_handler(msg):
        pass

    @handle(wiki_changes)
    def second_handler(msg):
        pass

    run(stream())

Pipe
^^^^

Use the ``pipe`` parameter in :py:class:`slipstream.handle` to transform the stream itself.

**Depends on:** `asyncstdlib <https://asyncstdlib.readthedocs.io/en/latest/index.html>`_.

::

    from asyncio import run, sleep

    from asyncstdlib import pairwise, accumulate

    from slipstream import handle, stream

    async def numbers(n=100):
        for i in range(n):
            await sleep(0.1)
            yield i

    @handle(numbers(), pipe=[pairwise], sink=[print])
    def handler(pair):
        yield pair

    run(stream())

::

    # pipe=[pairwise]    # pipe=[accumulate]    # pipe=[accumulate, pairwise]
    (0, 1)               0                      (0, 1)
    (1, 2)               1                      (1, 3)
    (2, 3)               3                      (3, 6)
    (3, 4)               6                      (6, 10)
    ...                  ...                    ...

Sink
^^^^

Any data sink (such as Redis) that can be turned into a ``Callable`` can be used in :py:class:`slipstream.handle`.

**Depends on:** `redis <https://redis.io/docs/latest/develop/clients/redis-py>`_.

::

    from asyncio import run

    from redis import Redis

    from slipstream import handle, stream

    async def messages():
        for _ in range(2):
            for emoji in '🏆📞🐟👌':
                yield emoji

    r = Redis(host='localhost', port=6379, charset='utf-8', decode_responses=True)

    def cache(pair: tuple):
        r.set(*pair)

    @handle(messages(), sink=[cache])
    def handler(msg):
        count = int(r.get(msg)) + 1 if msg in r else 1
        yield msg, count

    run(stream())

    print({k: int(r[k]) for k in r.keys('*')})

::

    {'👌': 2, '🏆': 2, '📞': 2, '🐟': 2}

Alternatively, :py:class:`slipstream.interfaces.ICache` can be used.

AvroCodec
^^^^^^^^^

Custom codecs can be created using :py:class:`slipstream.interfaces.ICodec`:

**Depends on:** `avro <https://pypi.org/project/avro/>`_.

::

    from io import BytesIO

    from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
    from avro.schema import Schema, parse

    from slipstream.codecs import ICodec

    class AvroCodec(ICodec):
        """Serializes/deserializes avro messages using schema."""

        def __init__(self, path: str):
            with open(path) as a:
                self.schema = parse(a.read())

        def encode(self, obj: Any) -> bytes:
            writer = DatumWriter(self.schema)
            bytes_writer = BytesIO()
            encoder = BinaryEncoder(bytes_writer)
            writer.write(obj, encoder)
            return cast(bytes, bytes_writer.getvalue())

        def decode(self, s: bytes) -> object:
            bytes_reader = BytesIO(s)
            decoder = BinaryDecoder(bytes_reader)
            reader = DatumReader(self.schema)
            return cast(object, reader.read(decoder))

Aggregations
^^^^^^^^^^^^

Streaming aggregations typically don't rely on the whole data's history but are either:

- **Fold or reduce operations:** incremental updates to a state like count or sum over all data, like the code snippet in :ref:`Getting Started <getting_started:persistence>`
- **Window operations:** applying these on data within a window of time (event- or wall-time based)

Here are some of the well-known window types:

- **Tumbling:** fixed-size, non-overlapping, on fixed time interval
- **Hopping:** fixed-size, overlapping, on fixed time interval
- **Sliding:** fixed-size, overlapping, on content of window change
- **Session:** dynamic-size, overlapping, on some condition being met

Let's look at the Sliding window using these emoji's, having timestamps in seconds:

::

    from asyncio import run, sleep

    from slipstream import Cache, handle, stream

    cache = Cache('state/emoji')

    async def messages():
        events = [
            ('🏆', 0.0), ('📞', 0.5), ('🐟', 1.0), ('👌', 2.0),
            ('🏆', 3.5), ('📞', 4.0), ('🐟', 5.0), ('👌', 5.5)
        ]
        for emoji, ts in events:
            await sleep(0.1)
            yield emoji, ts

Fixed-size window sliding with each event:

::

    from collections import Counter

    window_size_seconds = 3.0

    @handle(messages(), sink=[print])
    async def sliding_handler(event):
        _, event_time = event

        events = cache.get('sliding_events', [])
        events.append(event)

        # Keep events within window_size_seconds of current event_time
        events = [
            (e, t) for e, t in events
            if event_time - t <= window_size_seconds
        ]
        cache.put('sliding_events', events)

        counts = Counter(emoji for emoji, _ in events)
        return f'Sliding window ending at {event_time}: {dict(counts)}'

    run(stream())

::

    Sliding window ending at 0.0: {'🏆': 1}
    Sliding window ending at 0.5: {'🏆': 1, '📞': 1}
    Sliding window ending at 1.0: {'🏆': 1, '📞': 1, '🐟': 1}
    Sliding window ending at 2.0: {'🏆': 1, '📞': 1, '🐟': 1, '👌': 1}
    Sliding window ending at 3.5: {'📞': 1, '🐟': 1, '👌': 1, '🏆': 1}
    Sliding window ending at 4.0: {'🐟': 1, '👌': 1, '🏆': 1, '📞': 1}
    Sliding window ending at 5.0: {'👌': 1, '🏆': 1, '📞': 1, '🐟': 1}
    Sliding window ending at 5.5: {'🏆': 1, '📞': 1, '🐟': 1, '👌': 1}

For production-readiness, you’d add:

- **Watermarks:** to determine when a window is "complete" despite late events
- **Late event handling:** drop, reassign, or buffer late events

To handle late data or stream downtimes, see :ref:`cookbook:synchronization`.

Joins
^^^^^

Cross-stream stateful operations such as joins can be achieved using :ref:`Cache <features:cache>`.

Using the messages below, we'll use a temporal-join to find the ``weather`` at the time of each ``activity``:

::

    from datetime import datetime as dt

    weather_messages = iter([
        {'timestamp': dt(2023, 1, 1, 10), 'value': '🌞'},
        {'timestamp': dt(2023, 1, 1, 11), 'value': '⛅'},
        {'timestamp': dt(2023, 1, 1, 12), 'value': '🌦️'},
        {'timestamp': dt(2023, 1, 1, 13), 'value': '🌧'},
    ])
    activity_messages = iter([
        {'timestamp': dt(2023, 1, 1, 10, 30), 'value': 'swimming'},  # 🌞
        {'timestamp': dt(2023, 1, 1, 11, 30), 'value': 'walking home'},  # ⛅
        {'timestamp': dt(2023, 1, 1, 12, 30), 'value': 'shopping'},  # 🌦️
        {'timestamp': dt(2023, 1, 1, 13, 10), 'value': 'lunch'},  # 🌧
    ])

By caching the ``weather`` updates using their (POSIX) event-time as a key, we can find the nearest timestamp value.
This type of join is often called a temporal-join, nearby-join, or merge-as-of:

::

    from asyncio import run, sleep

    from slipstream import Cache, handle, stream

    weather_cache = Cache('state/weather')

    async def async_iterable(it):
        for msg in it:
            await sleep(0.01)
            yield msg

    @handle(async_iterable(weather_messages), sink=[weather_cache])
    def weather_handler(w):
        unix_ts = w['timestamp'].timestamp()
        yield unix_ts, w

    @handle(async_iterable(activity_messages), sink=[print])
    def activity_handler(a):
        unix_ts = a['timestamp'].timestamp()

        for w in weather_cache.values(backwards=True, from_key=unix_ts):
            yield f'The weather during {a["value"]} was {w["value"]}'
            return

        yield a['value'], '?'

    run(stream())

::

    The weather during swimming was 🌞
    The weather during walking home was ⛅
    The weather during shopping was 🌦️
    The weather during lunch was 🌧

This approach works when the ``weather`` updates are guaranteed to be received in time.
If the ``weather`` stream goes down, the ``activity`` stream will be enriched with stale data.

To manage late data, see synchronization 👇

Synchronization
^^^^^^^^^^^^^^^

Using :ref:`features:checkpoint` we can detect and act on stream downtimes, pausing the dependent stream, and optionally send out corrections.

::

    from datetime import datetime as dt

    weather_messages = iter([
        {'timestamp': dt(2023, 1, 1, 10), 'value': '🌞'},
        {'timestamp': dt(2023, 1, 1, 11), 'value': '⛅'},
        {'timestamp': dt(2023, 1, 1, 12), 'value': '🌦️'},
        {'timestamp': dt(2023, 1, 1, 13), 'value': '🌧'},
    ])
    activity_messages = iter([
        {'timestamp': dt(2023, 1, 1, 10, 30), 'value': 'swimming'},  # 🌞
        {'timestamp': dt(2023, 1, 1, 11, 30), 'value': 'walking home'},  # ⛅
        {'timestamp': dt(2023, 1, 1, 12, 30), 'value': 'shopping'},  # 🌦️
        {'timestamp': dt(2023, 1, 1, 13, 10), 'value': 'lunch'},  # 🌧
    ])

Some changes in our setup are required:

- Adding a ``Cache`` for storing the ``Checkpoint``
- Storing the ``AsyncIterables`` in variables for later reference in the ``Checkpoint``

::

    from asyncio import run, sleep
    from datetime import timedelta
    from typing import cast

    from slipstream import Cache, Topic, handle, stream
    from slipstream.checkpointing import Checkpoint, Dependency
    from slipstream.codecs import JsonCodec
    from slipstream.core import READ_FROM_END

    async def async_iterable(it):
        for msg in it:
            await sleep(1)
            yield msg

    weather_stream = async_iterable(weather_messages)
    activity_stream = async_iterable(activity_messages)

    activity = Topic('activity', {
        'bootstrap_servers': 'localhost:29091',
        'auto_offset_reset': 'earliest',
        'group_instance_id': 'activity',
        'group_id': 'activity',
    }, codec=JsonCodec(), offset=READ_FROM_END)
    checkpoints_cache = Cache('state/checkpoints', target_table_size=10000)
    weather_cache = Cache('state/weather')

The ``Checkpoint`` defines the relationship between streams:

- The ``activity`` ``Topic`` depends on the ``weather_stream`` ``AsyncIterable``
- The dependency must be down for 1 hour
- The ``downtime_callback`` function is called when a downtime is detected
- The ``recovery_callback`` function is called when the dependency has caught up again

::

    async def downtime_callback(c: Checkpoint, d: Dependency) -> None:
        print('\tThe stream is automatically paused.')

    async def recovery_callback(c: Checkpoint, d: Dependency) -> None:
        offsets = cast(dict[str, int], d.checkpoint_state)
        print(
            '\tDowntime resolved, '
            f'going back to offset {offsets} for reprocessing.'
        )
        await activity.seek({
            int(p): o for p, o in offsets.items()
        })

    checkpoint = Checkpoint(
        'activity',
        dependent=activity,
        dependencies=[Dependency(
            'weather_stream',
            weather_stream,
            downtime_threshold=timedelta(hours=1)
        )],
        downtime_callback=downtime_callback,
        recovery_callback=recovery_callback,
        cache=checkpoints_cache
    )

In ``handle_weather`` handler we will "kill" the stream for 5 seconds:

::

    @handle(weather_stream, sink=[weather_cache, print])
    async def handle_weather(w):
        """Process weather message."""
        ts = w['timestamp']
        unix_ts = ts.timestamp()
        await checkpoint.heartbeat(ts)
        yield unix_ts, w

        if w['value'] == '⛅':
            print('\tKilling weather stream on purpose')
            await sleep(5)
            print('\tRecovering the weather stream')

    @handle(activity_stream, sink=[activity])
    def producer(val):
        """Send data to activity topic."""
        yield None, val

    @handle(activity, sink=[print])
    async def handle_activity(msg):
        """Process activity message."""
        a = msg.value
        ts = dt.strptime(a['timestamp'], '%Y-%m-%d %H:%M:%S')
        unix_ts = ts.timestamp()

        if downtime := await checkpoint.check_pulse(ts, **{
            str(msg.partition): msg.offset
        }):
            print(
                f'\tDowntime detected: {downtime}, '
                '(could cause faulty enrichment)'
            )

        for w in weather_cache.values(backwards=True, from_key=unix_ts):
            yield f'The weather during {a["value"]} was {w["value"]}'
            return

        yield a["value"], '?'

    run(stream())

During the 5 seconds, the activity messages still flow in. This triggers the downtime detection, because the activity event times supercede the last seen weather event time.
Breakdown:

- ``checkpoint.heartbeat`` registers the weather event time in the checkpoint
- ``checkpoint.check_pulse`` registers the activity event time, checking the pulse of its dependencies
- It also passes some state to the checkpoint, in this case; the Kafka offsets

::

    The weather during swimming was 🌞
        Killing weather stream on purpose
    The weather during walking home was ⛅
        The stream is automatically paused.
        Downtime detected: 1:30:00, (could cause faulty enrichment)
    The weather during shopping was ⛅
        Recovering the weather stream
        Downtime resolved, going back to offset {'0': 2} for reprocessing.
    The weather during shopping was 🌦️
    The weather during lunch was 🌧

- One faulty enrichment took place: ``The weather during shopping was ⛅`` before the ``activity`` stream was paused (waiting for the ``weather_stream`` to recover).
- When the ``weather_stream`` recovered, the user defined ``recovery_callback`` was called.
- The callback seeks the ``activity`` topic back to the offset before the ``weather_stream`` went down, causing the activity events that were sent out with stale data to be reprocessed
- The faulty enrichment was corrected: ``The weather during shopping was 🌦️``

Notice that when sending out corrections is required (using :py:class:`slipstream.Topic.seek` for example), data flows through the handler function again.
This must be handled appropriately when dealing with stateful aggregations (prevent counting/summing an event twice).
All consumers of the data must also be capable of dealing with corrections, by compacting/deduplicating the data by some key.

Endpoint
^^^^^^^^

We can add API endpoints using ``fastapi``.

**Depends on:** `fastapi <https://fastapi-tutorial.readthedocs.io>`_.

This streaming endpoint emits cache updates:

::

    from asyncio import gather, run, sleep
    from time import strftime

    from fastapi import FastAPI
    from fastapi.responses import StreamingResponse
    from uvicorn import Config, Server

    from slipstream import Cache, handle, stream

    app, cache = FastAPI(), Cache('db')

    async def timer(interval=1.0):
        while True:
            yield
            await sleep(interval)

    @handle(timer(), sink=[cache, print])
    def tick_tock():
        yield 'time', strftime('%H:%M:%S')

    async def cache_value_updates():
        async for _, v in cache:
            yield v + '\n'

    @app.get('/updates')
    async def updates():
        return StreamingResponse(
            cache_value_updates(),
            media_type='text/event-stream'
        )

    async def main():
        config = Config(app=app, host='0.0.0.0', port=8000)
        server = Server(config)
        await gather(stream(), server.serve())

    if __name__ == '__main__':
        run(main())

- An update is emitted only when the cache is called as a function (``cache(key, val)``)
- The cache can be used as an ``AsyncIterator`` (``async for k, v in cache``)
- The ``updates`` endpoint returns the emitted updates through a ``StreamingResponse``

::

    curl -N http://127.0.0.1:8000/updates

::

    00:16:57
    00:16:58
    00:16:59
    00:17:00
    ...

When we call the endpoint, we'll receive each update to the cache.
