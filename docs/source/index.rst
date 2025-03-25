Documentation
=============

.. image:: _static/logo.png
   :width: 25%
   :align: right

Slipstream provides a data-flow model to simplify development of stateful streaming applications.

- **Simplicity:** parallelize using a decorator, maps sources to sinks
- **Freedom:** use any Python code without limiting abstractions
- **Speed:** sensible and optimized defaults to get started

Your typical stateful streaming application:

- **Connects** to many sources: Kafka clusters, Streaming API's, et cetera
- **Processes** streams in parallel: deserializing, aggregating, joining
- **Recovers** from crashes and stream downtime

Demo
^^^^

One such source could be a simple timer for example, any ``Async Iterable`` will do:

::

    from asyncio import run, sleep

    async def timer(interval=1.0):
        while True:
            yield
            await sleep(interval)

For our sink we will use the ``Callable`` ``print``:

.. code-block:: python

    print

In our case we will output a fish that swims downstream on a regular 1 second interval.

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

With :py:class:`slipstream.stream` we can start processing the ``Async Iterables`` sources, and with :py:class:`slipstream.handle` we can direct the flow from these sources through our handler functions into our sinks.

Because everything is built with basic python building blocks, framework-like features can be crafted with ease.
For instance, while timers aren't included, you can whip one up effortlessly.

Explore the :doc:`cookbook <cookbook>` for more recipes!

Contents
^^^^^^^^

.. toctree::
   :maxdepth: 2

   installation
   getting_started
   cookbook
   features
   autoapi/index
