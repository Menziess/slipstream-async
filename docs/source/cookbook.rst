Cookbook
========

Slipstream favors user freedom over rigid abstractions, letting you craft framework features in just a few lines.

Timer
^^^^^

Async generators can be used to trigger handler functions.

::

    from asyncio import run, sleep
    from time import strftime

    from slipstream import handle, stream

    async def timer(interval=1.0):
        while True:
            yield
            await sleep(interval)

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
            yield msg
            await sleep(0.01)

Joins
^^^^^

Stream-stream joins can be performed using :ref:`Cache <features:cache>`.

We can try to figure out what the ``weather`` was like during each ``activity``:

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

If we cache the ``weather`` updates using their (POSIX) event-time as a key, we can find the nearest timestamp value using a temporal-join (nearby-join / merge-as-of):

::

    from asyncio import run, sleep

    from slipstream import Cache, handle, stream

    weather_cache = Cache('state/weather')

    async def async_iterable(it):
        for msg in it:
            yield msg
            await sleep(0.01)

    @handle(async_iterable(weather_messages), sink=[weather_cache, print])
    def weather_handler(w):
        unix_ts = w['timestamp'].timestamp()
        yield unix_ts, w

    @handle(async_iterable(activity_messages), sink=[print])
    def activity_handler(a):
        unix_ts = a['timestamp'].timestamp()

        for w in weather_cache.values(backwards=True, from_key=unix_ts):
            yield f'>>> The weather during {a["value"]} was {w["value"]}'
            return

        yield a['value'], '?'

    run(stream())

::

    (1672563600.0, {'timestamp': datetime.datetime(2023, 1, 1, 10, 0), 'value': '🌞'})
    >>> The weather during swimming was 🌞
    (1672567200.0, {'timestamp': datetime.datetime(2023, 1, 1, 11, 0), 'value': '⛅'})
    >>> The weather during walking home was ⛅
    (1672570800.0, {'timestamp': datetime.datetime(2023, 1, 1, 12, 0), 'value': '🌦️'})
    >>> The weather during shopping was 🌦️
    (1672574400.0, {'timestamp': datetime.datetime(2023, 1, 1, 13, 0), 'value': '🌧'})
    >>> The weather during lunch was 🌧

This operation requires the ``weather`` updates to be received in time. If the ``weather`` stream goes down, the ``activity`` stream will be enriched with stale data.

We can use stream synchronization to detect dependency downtime, pause the dependent stream, and possibly send out corrections.

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
