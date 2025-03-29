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

By adding a :ref:`features:topic` we can simultaneously send data to Kafka using the ``produce`` handler, and receive data using the ``consume`` handler:

::

    from asyncio import run, sleep

    from slipstream import Topic, handle, stream

    topic = Topic('emoji', {
        'bootstrap_servers': 'localhost:29091',
        'group_instance_id': 'demo',
        'auto_offset_reset': 'earliest',
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

Now if we would like to aggregate these emoji's, we'd need some way to keep track of the results.

Persistence
^^^^^^^^^^^

By adding :ref:`features:cache` we can persist state within our application, making it resilient to crashes and allowing cross-stream stateful operations:

::

    from asyncio import run, sleep

    from slipstream import Cache, Topic, handle, stream

    topic = Topic('emoji', {
        'bootstrap_servers': 'localhost:29091',
        'group_instance_id': 'demo',
        'auto_offset_reset': 'earliest',
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

Notice that ``cache`` is used as a sink, persisting our yielded key and value: ``emoji: count``.

The ``counter`` prints out the cache contents every three seconds:

::

    emoji counts: {}
    received: emoji 👌
    received: emoji 🏆
    ...
    emoji counts: {'emoji 🏆': 4, 'emoji 🐟': 2, 'emoji 👌': 3, 'emoji 📞': 3}

When using :ref:`features:cache`, the data is automatically persisted to disk, and when the application restarts after a crash, the state is loaded from it.

It's set to use `Fifo <https://rocksdict.github.io/RocksDict/rocksdict.html#DBCompactionStyle>`_ compaction style by default, maintaining a window size of roughly 25 MB, but this can be configured.

Read more about Slipstream's :doc:`features <features>`. Or explore the :doc:`cookbook <cookbook>` for more interesting recipes!
