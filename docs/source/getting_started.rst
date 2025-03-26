Getting Started
===============

Slipstream's hello-world app would look something like this:

::

    from asyncio import run

    from slipstream import handle, stream

    async def messages():
        for emoji in '🏆📞🐟👌':
            yield emoji

    @handle(messages(), sink=[print])
    def handler(msg):
        yield f'Hello {msg}!'

    run(stream())

::

    Hello 🏆!
    Hello 📞!
    Hello 🐟!
    Hello 👌!

Let's spice things up by introducing Kafka.

Kafka
^^^^^

Spin up a local Kafka broker using `docker-compose.yml <https://github.com/Menziess/slipstream/blob/master/docker-compose.yml>`_ and follow along:

.. code-block:: bash

    docker compose up broker -d

Run the customized hello-world snippet, using :ref:`features:topic`:

::

    from asyncio import run, sleep

    from slipstream import Topic, handle, stream

    topic = Topic('emoji', {
        'bootstrap_servers': 'localhost:29091',
        'group_instance_id': 'demo',
        'group_id': 'demo',
    })

    async def messages():
        while True:
            for emoji in '🏆📞🐟👌':
                yield emoji

    @handle(messages(), sink=[topic])
    async def producer(msg):
        await sleep(.5)
        yield None, f'emoji {msg}'

    @handle(topic, sink=[print])
    def consumer(msg):
        emoji = msg.value
        yield f'received: {emoji}'

    run(stream())

::

    received: emoji 🏆
    received: emoji 📞
    received: emoji 🐟
    received: emoji 👌
    ...

Next up, we'll aggregate the data and do something useful with these emoji's.

Persistence
^^^^^^^^^^^

By adding :ref:`features:cache` we can persist state within our application:

::

    from asyncio import run, sleep

    from slipstream import Cache, Topic, handle, stream

    topic = Topic('emoji', {
        'bootstrap_servers': 'localhost:29091',
        'group_instance_id': 'demo',
        'group_id': 'demo',
    })
    cache = Cache('state/emoji')

    async def timer(interval=1.0):
        while True:
            yield
            await sleep(interval)

    async def messages():
        while True:
            for emoji in '🏆📞🐟👌':
                yield emoji

    @handle(messages(), sink=[topic])
    async def producer(msg):
        await sleep(.5)
        yield None, f'emoji {msg}'

    @handle(topic, sink=[cache])
    def consumer(msg):
        emoji = msg.value
        count = cache.get(emoji, 0) + 1
        yield emoji, count
        print(f'received: {emoji}')

    @handle(timer(3), sink=[print])
    def counter():
        counts = dict(cache.items())
        return f'emoji counts: {counts}'

    run(stream())

Outputting the count every three seconds:

::

    emoji counts: {}
    received: emoji 👌
    received: emoji 🏆
    ...
    emoji counts: {'emoji 🏆': 4, 'emoji 🐟': 2, 'emoji 👌': 3, 'emoji 📞': 3}

If our application crashes and restarts, it automatically loads the last state from the cache.
