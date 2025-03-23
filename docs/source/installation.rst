Installation
============

Slipstream can be installed using ``pip``:

.. code-block:: c

    pip install slipstream-async

Optional Dependencies
^^^^^^^^^^^^^^^^^^^^^

Keeping the package lightweight and granting you full control, some dependencies can be:

- Pinned down to specific versions for stability and reproducability
- Installed using the extras notation in order to use the latest version

Kafka
-----

:ref:`features:Topic` uses `aiokafka <https://aiokafka.readthedocs.io/en/stable/index.html>`_ under the hood, the default kafka client:

.. code-block:: c

    pip install aiokafka==0.12.0
    pip install slipstream-async[kafka]

RocksDict
---------

:ref:`features:Cache` is a basic wrapper around `rocksdict <https://congyuwang.github.io/RocksDict/rocksdict.html>`_, the default caching implementation:

.. code-block:: c

    pip install rocksdict==0.3.25
    pip install slipstream-async[cache]
