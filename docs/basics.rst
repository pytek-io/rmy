Tutorial
========

A simple greeting application
-----------------------------

One can server application by passing an object to be exposed to its clients as follows.

..  code-block:: python

    import rmy

    class Demo(rmy.BaseRemoteObject):

        @rmy.remote_async_method
        async def greet(self, name):
            return f"Hello {name}!"


    if __name__ == "__main__":
        rmy.run_tcp_server(8080, Demo())

Clients can access this object as follows.

.. code-block:: python

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy = client.fetch_remote_object(Demo)
            while True:
                print('Enter your name:')
                name = input()
                print(proxy.greet.eval(name))


The `proxy` object returned by `fetch_remote_object` is a `Demo` object which can be used to invoke methods on the shared instance that resides in the server process. Note that we did not invoke the method as usual, that is using `__call__` operator but using `eval` instead. 
To be accessible methods need to be decorated with the relevant decorators (eg: `remote_async_method`, `remote_sync_method`, `remote_async_generator`, etc). A decorated method can be invoked remotely either synchronously using `eval` method or asynchronously using `wait`. Here is the asynchronous version of the previous example using `create_async_client`.

.. code-block:: python

    import asyncio

    async def main():
        async with rmy.create_async_client("localhost", 8080) as client:
            proxy = await client.fetch_remote_object(Demo)
            while True:
                print('Enter your name:')
                name = input()
                print(await proxy.greet.wait(name))

    if __name__ == "__main__":
        asyncio.run(main())

RMY has strong support for dynamic Python typing hints, allowing IDEs to provide code completion and type checking. For example they will be able to detect that `eval` and `wait` methods have the same signature as the original method as well infer the returned type of `fetch_remote_object`` from its argument.

Exception handling
------------------

RMY will always return either remote call results or re-raise exceptions locally if any. For example if we modify the `greet` method as follows.

.. code-block:: python

    class Demo(rmy.BaseRemoteObject):
        @rmy.remote_async_method
        async def greet(self, name: str):
            if not name:
                raise ValueError("Name cannot be empty")
            return f"{self.greet} {name}!"

Then the following code will receive an exception.

.. code-block:: python

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy = client.fetch_remote_object(Demo)
            try:
                print(proxy.greet.eval(""))
            except Exception as e:
                print(e)

Exposing generators
-------------------

One can remotely iterate remotely through data returned by an exposed object. For example we can make our greeting service a bit more human like by returning a few sentences randomly spreaded apart.

.. code-block:: python

    import asyncio
    import random

    class Demo(rmy.BaseRemoteObject):
        @rmy.remote_async_generator
        async def chat(self, name):
            for message in [f"Hello {name}!", "How are you?", f"Goodbye {name}!"]:
                yield message
                await asyncio.sleep(random.random())

Then we can iterate through the sentences as follows and print them as they are produced.
    
.. code-block:: python

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy = client.fetch_remote_object(Demo)
            while True:
                print('Enter your name:')
                name = input()
                for sentence in proxy.chat(name):
                    print(sentence)


Iteration policies
------------------

By nature asynchronous systems are prone to slow consumer issues which can cause uncontrolled memory use. RMY provides mechanisms to prevent this from happening. By default it will eagerly iterate through asynchronous generators and send data to the client straightaway. Those data will be buffered by the client. If too many values accumulate, the client code will receive a `BufferFullError` exception. This behaviour can be customized by the `max_data_in_flight_count`  and `max_data_in_flight_size` parameters.
Pushing results to client is usually the expected behaviour unless returned sequence does not correspond to stream of event but rather a sequence of results that we want to return in chunks. Consider the following example.

.. code-block:: python

    class Demo:
        @rmy.remote_async_generator
        async def count(self, bound):
            for i in range(bound):
                yield i

If we iterate through the results as follows, an `BufferFullError` exception will be thrown after `max_data_in_flight_count` loop iterations on the server. 

.. code-block:: python
    
    import time

    if __name__ == "__main__":
        with rmy.create_sync_client("localhost", 8080) as client:
            proxy = client.fetch_remote_object(Demo)
            for i in proxy.count(1000000):
                time.sleep(1)
                print(i)

One would easily realize that in this example the data should be **pulled** by the client as it consumes it, rather than been *pushed* blindly by the server. This can be done by either by wrapping the generator in a `RemoteGeneratorPull` object or by decorating the method with `remote_generator_pull` as follows.

.. code-block:: python

    class Demo:
        @rmy.remote_generator_pull
        async def count(self, bound):
            for i in range(bound):
                yield i


Cancellation and early exits
----------------------------

Coroutines can be cancelled from the client code. In the following example, the `sleep` method will be cancelled after 1 second. 

.. code-block:: python
    
    import asyncio

    class Demo(rmy.BaseRemoteObject):

        def __init__(self):
            self.cancelled = False

        @rmy.remote_sync_method
        def get_cancelled(self):
            return self.cancelled

        @rmy.remote_async_method
        async def sleep(self, duration: int):
            try:
                await asyncio.sleep(duration)
            finally:
                self.cancelled = True

    async def main_async(proxy: Demo):
        task = asyncio.create_task(proxy.sleep.wait(100))
        await asyncio.sleep(1)
        if not task.done():
            task.cancel()
        await asyncio.sleep(.1)
        assert await proxy.get_cancelled.wait()

Note that cancellation is supported only in an `async` fashion. For the server to remain reponsive remote tasks should not  hold the GIL for any significant amount of time.

Interfaces
----------
