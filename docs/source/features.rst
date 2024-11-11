.. _features:

Features
============

Each example is self-contained. Can't find what you seek? Create a `new issue <https://github.com/Menziess/slipstream/issues/new>`_.

Topic
-----

Topic can be used to interact with kafka.

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

Topic uses `aiokafka <https://aiokafka.readthedocs.io/en/stable/index.html>`_ under the hood.

Cache
-----

Cache can be used to persist data.

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

Cache is a basic wrapper around `rocksdict <https://congyuwang.github.io/RocksDict/rocksdict.html>`_.

To prevent race conditions, the ``transaction`` context manager can be used:

::

    with cache.transaction('fish'):
        cache['fish'] = '🐟'

- This only works for asynchronous code (not for multithreading or multiprocessing code)
- Until a transaction is finished, other transactions for the same key will block
- All actions outside of transaction blocks will ignore ongoing transactions (risk for race conditions)
- Reads won't be limited by ongoing transactions

Conf
----

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
-----

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

Timer
-----

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

Codec
-----

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

Custom codecs can be created using ``ICodec``:

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
