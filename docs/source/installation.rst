Installation
============

Slipstream can be installed using ``pip``:

.. code-block:: c

    pip install slipstream-async

Optional Dependencies
^^^^^^^^^^^^^^^^^^^^^

Some :doc:`features <features>` require extra dependencies that are not installed by default, keeping the package small and giving you full control.

- Pin specific versions for stability and reproducibility
- Install latest using the extras notation

Topic
-----

:ref:`features:Topic` is the default Kafka client, using `aiokafka <https://aiokafka.readthedocs.io/en/stable/index.html>`_ under the hood.

.. code-block:: c

    pip install aiokafka==0.12.0
    pip install slipstream-async[kafka]

Cache
-----

:ref:`features:Cache` is the default embedded cache, a basic wrapper around `rocksdict <https://rocksdict.github.io/RocksDict/rocksdict.html>`_.

.. code-block:: c

    pip install rocksdict==0.3.25
    pip install slipstream-async[cache]
