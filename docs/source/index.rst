Documentation
=============

.. image:: https://raw.githubusercontent.com/menziess/slipstream/master/res/logo.png
   :width: 25%
   :align: right

Slipstream provides a data-flow model to simplify development of stateful streaming applications.

- **Freedom:** use any Python code in or outside handler functions
- **Simplicity:** parallelize using a decorator mapping sources to sinks
- **Speed:** sensible and optimized defaults to get started

Because everything is built on top of basic python building blocks, standard functionality like timers aren't part of the library, but can simply be build using the standard

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
