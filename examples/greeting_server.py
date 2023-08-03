import asyncio

import rmy


class Demo(rmy.BaseRemoteObject):
    def __init__(self):
        self._greet = "Hello"

    @rmy.remote_async_method
    async def greet(self, name):
        return f"{self._greet} {name}!"

    async def conversation(self, name):
        yield f"Hello {name}!"
        await asyncio.sleep(1)
        yield f"How are you {name}?"
        await asyncio.sleep(1)
        yield f"Goodbye {name}!"

    @rmy.remote_async_generator
    async def count(self):
        try:
            for i in range(100):
                print("counting", i)
                # import traceback
                # traceback.print_stack()
                yield i
        finally:
            print("finally called")


if __name__ == "__main__":
    rmy.run_tcp_server(8080, Demo())
