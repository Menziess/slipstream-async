Optimizations
=============

Slipstream's defaults need no tuning. This page explains what they do, for when you replace them with your own.

Cache
^^^^^

A cache opened with only a path uses the tuned defaults:

::

    from slipstream import Cache

    cache = Cache('db')

- The window keeps the newest 25 MB and drops the oldest files beyond it
- Small files merge in the background, dropping overwritten and deleted rows
- Bloom filters let lookups of missing keys skip most files
- Files never expire by age, only through the window

Scans and lookups of missing keys therefore stay fast when a cache sees many writes and deletes. Only a writable cache merges; read-only and secondary caches read the files their writer merged.

Merging also removes expired rows when the cache expires rows by age:

::

    from rocksdict import AccessType

    cache = Cache('db', access_type=AccessType.with_ttl(3600))

- The ``with_ttl`` duration is in seconds

Rather than reading the window from custom ``options``, the cache sizes its merges from ``target_table_size``, so pass the same window to both:

::

    from rocksdict import DBCompactionStyle, FifoCompactOptions, Options

    from slipstream.caching import MB

    fifo = FifoCompactOptions()
    fifo.max_table_files_size = 10 * MB

    options = Options()
    options.create_if_missing(True)
    options.set_compaction_style(DBCompactionStyle.fifo())
    options.set_fifo_compaction_options(fifo)

    cache = Cache('db', options=options, target_table_size=10 * MB)

- The ``max_table_files_size`` attribute sets the window
- The ``target_table_size`` argument caps each merge at a quarter of it
- The ``options`` argument does not turn merging off
