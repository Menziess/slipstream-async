.. _index:

Slipstream documentation
========================

Slipstream can be summarized as:

- ``iterables``: act as sources for our handler functions
- ``callables``: can be used as sinks
- ``handle`` and ``stream``: a data-flow model used to parallelize stream processing

A typical **slipstream** hello-world snippet would look something like this:

::

    from asyncio import run

    from slipstream import handle, stream

    async def messages():
        for emoji in '🏆📞🐟👌':
            yield emoji

    @handle(messages(), sink=[print])
    def handle_message(msg):
        yield f'Hello {msg}!'

    run(stream())

::

    Hello 🏆!
    Hello 📞!
    Hello 🐟!
    Hello 👌!

This simple yet powerful flow can be used in combination with Kafka and caches to build complex stateful streaming applications.

Install using ``pip``::

    pip install slipstream-async

Kafka
-----

Install ``aiokafka`` separately or along with slipstream (unpinned)::

    pip install slipstream-async[kafka]

Spin up a local kafka broker with `docker-compose.yml <https://github.com/Menziess/slipstream/blob/master/docker-compose.yml>`_ to follow along:

.. code-block:: bash

    docker compose up broker -d

Run the customized hello-world snippet:

::

    from asyncio import run

    from slipstream import Topic, handle, stream

    t = Topic('emoji', {
        'bootstrap_servers': 'localhost:29091',
        'group_instance_id': 'demo',
        'group_id': 'demo',
    })

    async def messages():
        for emoji in '🏆📞🐟👌':
            yield emoji

    @handle(messages(), sink=[t])
    def handle_message(msg):
        yield None, f'emoji {msg}'

    @handle(t, sink=[print])
    def consume_message(msg):
        emoji = msg.value
        yield f'received: {emoji}'

    run(stream())

::

    received: emoji 🏆
    received: emoji 📞
    received: emoji 🐟
    received: emoji 👌

Learn what else you can do from here:

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   features

.. toctree::
   :maxdepth: 1
   :caption: Modules:

   modules

Indices and tables
------------------

* :ref:`genindex`
