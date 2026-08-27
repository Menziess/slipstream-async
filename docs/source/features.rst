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

1. Example - `Downtime recovery <https://gist.github.com/Menziess/05cf7432cbed72e3a308075eb52869cf>`_
2. Example - `Downtime reprocessing <https://gist.github.com/Menziess/e212727fdd87d3d1c9ea47ea8043476e>`_

A checkpoint consists of one dependent, and many dependency streams:

::

    from datetime import datetime

    from slipstream import Checkpoint
    from slipstream.checkpointing import Dependency

    checkpoint = Checkpoint(
        activity,
        dependencies=Dependency(
            'weather',
            weather,
            marker='timestamp',
        ),
        cache=checkpoints_cache,
        marker=lambda msg: datetime.fromisoformat(msg.value['timestamp']),
        state=lambda msg, state: state | {
            str(msg.partition): msg.offset
        },
        on_recovery=lambda _c, d: activity.seek({
            int(p): o for p, o in d.checkpoint_state.items()
        }),
    )

- The first argument is the dependent stream
- The ``dependencies`` argument accepts a stream or ``Dependency``
- The ``marker`` normally returns a ``datetime`` from dependent messages
- The ``state`` callable receives the message and current state, then returns caller-selected recovery data
- When ``weather`` (dependency) goes down, ``activity`` will be paused so ``weather`` can catch up

The ``state`` callable always receives two arguments: the message and a copy of the current state. It returns the complete next state, so it can merge, replace, or remove entries. A callable ``marker`` receives the message and that next state. This keeps checkpointing agnostic to the state shape while allowing applications to derive comparison markers:

::

    checkpoint = Checkpoint(
        activity,
        dependencies=Dependency(
            'weather',
            weather,
            marker=lambda msg: msg.value['timestamp'],
        ),
        state=lambda msg, state: state | {
            str(msg.partition): msg.value['timestamp'],
        },
        marker=lambda _msg, state: min(state.values()),
    )

``Dependency`` accepts the same ``state`` and state-aware ``marker`` callables when its marker also depends on accumulated state.

Marker conversion belongs to the caller. The default checks subtract dependency markers from dependent markers and compare the difference with ``downtime_threshold``. A dependency that uses a different shape sets its own marker on ``Dependency``:

::

    checkpoint = Checkpoint(
        activity,
        dependencies=[
            Dependency(
                'weather',
                weather,
                marker='observed_at',
            ),
        ],
        marker=lambda msg: datetime.fromisoformat(msg.value['event_time']),
    )

Instead of the dependent stream, pass the checkpoint to ``handle``:

::

    @handle(weather, sink=[weather_cache])
    async def weather(msg):
        yield msg.timestamp, msg.value

    @handle(checkpoint, sink=[print])
    async def activity(msg, checkpoint=None):
        if checkpoint and checkpoint.downtime:
            print(checkpoint.downtime)
        yield msg.key, msg.value

- Add ``checkpoint=None`` on ``activity`` and read ``checkpoint.downtime`` when weather is behind
- The dependent stream is paused when a dependency is down or more than 10 minutes behind

When the dependency stream recovers, it might have to process a backlog of messages. So the dependent stream will remain paused until the dependency stream has caught up.

Rather than pausing ``activity``, leave it running:

::

    checkpoint = Checkpoint(
        activity,
        dependencies=[weather],
        marker=lambda msg: datetime.fromisoformat(msg.value['timestamp']),
        pause_dependent=False,
    )

- Activity keeps flowing while weather is behind
- Still read ``checkpoint.downtime`` if those messages need different handling

A full example is in :ref:`cookbook:synchronization`.
