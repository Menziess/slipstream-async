Features
========

Slipstream missing a feature? Create a `new issue <https://github.com/Menziess/slipstream/issues/new>`_.

Topic
^^^^^

Topic can be used to interact with :ref:`Kafka <getting_started:Kafka>`.

**Depends on:** :ref:`aiokafka <installation:topic>`.

::

    from asyncio import run

    from slipstream import Topic

    topic = Topic('emoji', {
        'bootstrap_servers': 'localhost:29091',
        'auto_offset_reset': 'earliest',
        'group_instance_id': 'demo',
        'group_id': 'demo',
    })

    async def main():
        await topic(key='trophy', value='🏆')
        await topic(key='fish', value='🐟')

        async for msg in topic:
            print(msg.key, msg.value)

    run(main())

::

    trophy 🏆
    fish 🐟

Cache
^^^^^

Cache can be used to persist data.

**Depends on:** :ref:`rocksdict <installation:cache>`.

::

    from slipstream import Cache

    cache = Cache('db')

    cache['prize'] = '🏆'
    cache['phone'] = '📞'

    for x, y in cache.items():
        print(x, y)

::

    phone 📞
    prize 🏆

Data is persisted to disk and automatically loaded upon restart, adding to application resilience.

By default, it will retain a window size of 25 MB using `Fifo <https://rocksdict.github.io/RocksDict/rocksdict.html#DBCompactionStyle>`_ compaction, this can be configured by passing ``options`` in :py:class:`slipstream.caching.Cache`.

Transaction
^^^^^^^^^^^

To prevent race conditions, Cache's ``transaction`` context manager can be used:

::

    async with cache.transaction('fish'):
        cache['fish'] = '🐟'

- This only works for asynchronous code (not for multithreading or multiprocessing code)
- Until a transaction is finished, other transactions for the same key will block
- All actions outside of transaction blocks will ignore ongoing transactions (risk for race conditions)
- Reads won't be limited by ongoing transactions

Proxy
^^^^^

Proxy enables passing messages between handlers.

::

    from asyncio import run

    from slipstream import handle, stream
    from slipstream.caching import Proxy

    proxy = Proxy()

    async def messages():
        for emoji in '🏆📞🐟👌':
            yield emoji

    @handle(messages(), sink=[proxy])
    def handler(emoji):
        yield f'Proxied {emoji}!'

    @handle(proxy, sink=[print])
    def handler(msg):
        yield msg

    run(stream())

::

    Proxied 🏆!
    Proxied 📞!
    Proxied 🐟!
    Proxied 👌!

Conf
^^^^

Conf can be used to set default kafka configurations.

::

    from slipstream import Conf, Topic

    Conf({
        'bootstrap_servers': 'localhost:29091',
        'group_id': 'default-demo',
    })

    topic1 = Topic('emoji', {'bootstrap_servers': 'localhost:29092'})

    Conf({
        'security_protocol': 'SASL_SSL',
        'sasl_mechanism': 'PLAIN',
        'sasl_plain_username': 'myuser',
        'sasl_plain_password': 'mypass',
    })

    topic2 = Topic('conf', {'group_id': 'demo'})

    print(topic1.conf)
    print(topic2.conf)

::

    {'bootstrap_servers': 'localhost:29092', 'group_id': 'default-demo'}
    {'bootstrap_servers': 'localhost:29091', 'group_id': 'demo', 'security_protocol': 'SASL_SSL', 'sasl_mechanism': 'PLAIN', 'sasl_plain_username': 'myuser', 'sasl_plain_password': 'mypass'}

Yield
^^^^^

When your handler function returns zero or more values, use ``yield`` instead of ``return``.

::

    from asyncio import run

    from slipstream import handle, stream

    async def numbers():
        for x in range(5):
            yield x

    @handle(numbers(), sink=[print])
    def handler(n):
        if n == 0:
            yield f'zero: {n}'
        if n % 2 == 0:
            yield f'even: {n}'

    run(stream())

::

    zero: 0
    even: 0
    even: 2
    even: 4

Codec
^^^^^

Codecs are used for serializing and deserializing data.

::

    from asyncio import run

    from slipstream import Topic
    from slipstream.codecs import JsonCodec

    topic = Topic('emoji', {
        'bootstrap_servers': 'localhost:29091',
        'auto_offset_reset': 'earliest',
        'group_instance_id': 'demo',
        'group_id': 'demo',
    }, codec=JsonCodec())

    async def main():
        await topic(key='fish', value={'msg': '🐟'})

        async for msg in topic:
            print(msg.value)

    run(main())

::

    {'msg': '🐟'}

You can define your own codecs using :py:class:`slipstream.interfaces.ICodec`, see :ref:`cookbook:AvroCodec` as an example.

Checkpoint
^^^^^^^^^^

Checkpoints can be used to detect late data:

1. Example - `Downtime recovery <https://gist.github.com/Menziess/1a450d06851cbd00292b2a99c77cc854?permalink_comment_id=5459889#gistcomment-5459889>`_
2. Example - `Downtime reprocessing <https://gist.github.com/Menziess/22d8a511f61c04a8142d81510a0db04b?permalink_comment_id=5468001#gistcomment-5468001>`_

A checkpoint consists of one dependent, and many dependency streams:

::

    async def emoji():
        for emoji in '🏆📞🐟👌':
            yield emoji

    dependent, dependency = emoji(), emoji()

    # Cache for persisting one or more Checkpoints
    checkpoints_cache = Cache('state/checkpoints', target_table_size=10000)

    c = Checkpoint(
        'dependent', dependent=dependent,
        dependencies=[Dependency('dependency', dependency)],
        cache=checkpoints_cache
    )

By default ``datetimes`` are compared to detect late data (preferably event times).
While using ``handle``, the dependent stream will automatically be paused when any dependency streams are down or fall behind 10 minutes.
This can be configured in :py:class:`slipstream.checkpointing.Dependency`. The pausing behavior can be disabled in :py:class:`slipstream.checkpointing.Checkpoint`.

::

    @handle(dependency)
    async def dependency_handler(msg):
        await c.heartbeat(msg.value['event_timestamp'])
        yield msg.key, msg.value

    @handle(dependent)
    async def dependent_handler(msg):
        await c.check_pulse(marker=msg.value['event_timestamp'])
        yield msg.key, msg.value

When the dependency stream recovers, it might have to process a backlog of messages. So the dependent stream will remain paused until the dependency stream has caught up.

Heartbeat returns latency info which can be used to handle late data differently (e.g. buffer or drop):

::

    latency = await c.heartbeat(msg.value['event_timestamp'])
    latency
    .. {
    ..     'is_late': True,
    ..     'dependent_marker': datetime(2025, 1, 1, 10),
    ..     'dependency_marker': datetime(2025, 1, 1, 9),
    .. }

Check pulse returns whatever the ``downtime_check`` of the dependency returns. By default it's a ``timedelta`` object.

It also takes arbitrary additional state (kwargs), which can be utilized to reprocess data.
In the example above "Downtime reprocessing", the offset was stored per partition, allowing to seek back to the time at which the dependency went down.

::

    if downtime := await checkpoint.check_pulse(ts, **{
        str(partition): offset
    }):
        print(f'Downtime detected: {downtime}')
