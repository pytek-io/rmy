[![PyPI version](https://img.shields.io/pypi/v/rmy)](https://pypi.org/project/rmy/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Versions](https://img.shields.io/pypi/pyversions/rmy)](https://pypi.org/project/rmy/)
[![test suite](https://github.com/pytek-io/rmy/actions/workflows/main.yml/badge.svg)](https://github.com/pytek-io/rmy/actions/workflows/main.yml)
[![Coverage Status](https://coveralls.io/repos/github/pytek-io/rmy/badge.svg?branch=main)](https://coveralls.io/github/pytek-io/rmy?branch=main)

An asyncio compatible RPC framework for building distributed applications.

RMY provides a high level API allowing to expose objects from one process to the other. It frees programmers from having to handle communication between processes while still providing enough control when needed. The simplest possible use of RMY framework looks as follows.


```python
import rmy

class Demo:
    async def greet(self, name):
        return f"Hello {name}!"


if __name__ == "__main__":
    rmy.run_tcp_server(8080, Demo())
```

This will expose an instance of the `Demo` object which can then be remotely accessed to as follows.

```python
import rmy
from hello_rmy_server import Demo

if __name__ == "__main__":
    with rmy.create_sync_client("localhost", 8080) as client:
        demo_remote = client.fetch_remote_object()
        while True:
            print('Enter your name:')
            name = input()
            print(demo_remote.greet(name))
```

One can easily also asynchronously iterate through results generated by another object remotely. MY provides robust ways to iterate over (asynchronous) iterators applying back pressure if required. This allows to implement trivially Pub-Sub or any other event notification pattern easily. A trivial example of remote generator iteration might look like this. 

```python
import random

class Demo:
    ...
    async def count(self):
        for i in range(1_000_000):
            await asyncio.sleep(random.uniform())
            yield i
```

One can remotely iterate through the values using a normal loop.

```python
for i in demo_remote.count():
    print(i)
```

For smplicity sake we showcased examples in synchronous code. One can easily make those asynchronous by instanciating an asynchronous client.