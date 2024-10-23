.. _index:

Slipstream documentation
========================

Slipstream can be summarized as:

- ``Topic``: default way to interact with kafka
- ``Cache``: default persistence functionality
- ``handle`` and ``stream``: a data-flow model used to parallelize stream processing

A typical hello-world application would look something like this:

::

  from asyncio import run

  from slipstream import handle, stream


  async def messages():
      for emoji in '🏆📞🐟👌':
          yield emoji


  @handle(messages(), sink=[print])
  def print_time(msg):
      yield f'Hello {msg}!'


  if __name__ == '__main__':
      run(stream())

::

  Hello 🏆!
  Hello 📞!
  Hello 🐟!
  Hello 👌!

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   kafka
   install
   examples

.. toctree::
   :maxdepth: 1
   :caption: Modules:

   modules

Indices and tables
------------------

* :ref:`genindex`
