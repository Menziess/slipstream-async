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

    with cache.transaction('fish'):
        cache['fish'] = '🐟'

- This only works for asynchronous code (not for multithreading or multiprocessing code)
- Until a transaction is finished, other transactions for the same key will block
- All actions outside of transaction blocks will ignore ongoing transactions (risk for race conditions)
- Reads won't be limited by ongoing transactions

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

A ``Checkpoint`` can be used to pulse the heartbeat of dependency streams to handle downtimes.

**Example code + output:**

1. `Downtime recovery <https://gist.github.com/Menziess/1a450d06851cbd00292b2a99c77cc854?permalink_comment_id=5459889#gistcomment-5459889>`_
2. `Downtime reprocessing <https://gist.github.com/Menziess/22d8a511f61c04a8142d81510a0db04b?permalink_comment_id=5468001#gistcomment-5468001>`_

A checkpoint consists of a dependent stream and dependency streams:

::

    async def emoji():
        for emoji in '🏆📞🐟👌':
            yield emoji

    dependent, dependency = emoji(), emoji()

    c = Checkpoint(
        'dependent', dependent=dependent,
        dependencies=[Dependency('dependency', dependency)]
    )

Checkpoints automatically handle pausing of dependent streams when they are bound to user handler functions (using ``handle``):

::

    @handle(dependency)
    async def dependency_handler(msg):
        key, val = msg.key, msg.value
        await c.heartbeat(val['event_timestamp'])
        yield key, val

    @handle(dependent)
    async def dependent_handler(msg):
        key, val, offset = msg.key, msg.value, msg.offset
        c.check_pulse(marker=msg['event_timestamp'], offset=offset)
        yield key, msg

On the first pulse check, no message might have been received from ``dependency`` yet.
Therefore the dependency checkpoint is updated with the initial state and marker of the dependent stream:

::

    from asyncio import run

    run(c.check_pulse(marker=datetime(2025, 1, 1, 10), offset=8))
    c['dependency'].checkpoint_marker

::

    datetime.datetime(2025, 1, 1, 10, 0)

When a message is received in ``dependency``, send a heartbeat with its event time, which can be compared with the dependent event times to check for downtime:

::

    run(c.heartbeat(datetime(2025, 1, 1, 10, 30)))

When the pulse is checked after a while, it's apparent that no
dependency messages have been received for 30 minutes:

::

    run(c.check_pulse(marker=datetime(2025, 1, 1, 11), offset=9))

::

    datetime.timedelta(seconds=1800)

