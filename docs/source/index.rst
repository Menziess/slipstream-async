Documentation
=============

.. image:: ../../res/logo.png
   :width: 25%
   :align: right

Slipstream provides a data-flow model to simplify development of stateful streaming applications.

- **Freedom:** use any Python code in or outside handler functions
- **Simplicity:** parallelize using a decorator mapping sources to sinks
- **Speed:** sensible and optimized defaults to get started

Your typical stateful streaming application:

- **Connects** to many sources: Kafka clusters, Streaming API's, et cetera
- **Processes** streams in parallel: deserializing, aggregating, joining
- **Persists** and looks up state: using RocksDB or any other database
- **Recovers** from crashes and stream downtime: by persisting state to disk and using checkpoints to pause streams or send out corrections

::

    async def timer(interval=1.0):
        while True:
            yield
            await sleep(interval)

Contents
^^^^^^^^

.. toctree::
   :maxdepth: 2

   installation
   getting_started
   cookbook
   features
   autoapi/index
