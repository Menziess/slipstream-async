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
