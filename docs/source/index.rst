Documentation
=============

.. image:: _static/logo.png
   :width: 25%
   :align: right

Slipstream provides a data-flow model to simplify development of stateful streaming applications.

- **Simplicity:** parallelizing processing, mapping sources to sinks
- **Freedom:** allowing arbitrary code free of limiting abstractions
- **Speed:** optimized and configurable defaults to get started quickly

Consume any ``AsyncIterable`` source: *Kafka, Streaming API's, Python generators, Cache updates*.
Produce to any ``Callable``: *Kafka, RocksDB, API's, Databases*.
Perform any arbitrary stateful operation using regular Python code: *filters, joins, aggregations, pairwise* or *batching*.
Detect and handle dependency stream downtimes: *pausing, catching up, resuming*, and *making corrections*.

Demo
^^^^

Slipstream components interoperate with basic python building blocks, making it easy to craft framework-like features.
For instance, while timers aren't included, you can whip one up effortlessly (``AsyncIterable``):

::

    from asyncio import run, sleep

    async def timer(interval=1.0):
        while True:
            await sleep(interval)
            yield

We'll use ``print`` as our sink (``Callable``):

.. code-block:: python

    print

Let's send our mascot "🐟 *-- blub*" downstream:

::

    from slipstream import handle, stream

    @handle(timer(), sink=[print])
    def handler():
        yield '🐟 - blub'

    run(stream())


    # 🐟 - blub
    # 🐟 - blub
    # 🐟 - blub
    # ...

Some things that stand out:

- We've created an ``Async Iterable`` source ``timer()`` (not generating data, just triggering the handler)
- We used :py:class:`slipstream.handle` to bind the sources and sinks to the ``handler`` function
- We yielded ``🐟 - blub``, which is sent to all the ``Callable`` sinks (just ``print`` in this case)
- Running :py:class:`slipstream.stream` starts the flow from sources via handlers into the sinks

The data-flow model that simplifies development of stateful streaming applications!

Contents
^^^^^^^^

Proceed by interacting with Kafka and caching application state in: :doc:`getting started <getting_started>`.

.. toctree::
   :maxdepth: 2

   installation
   getting_started
   cookbook
   features
   autoapi/index
