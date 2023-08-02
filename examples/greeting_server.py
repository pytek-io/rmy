import rmy
import asyncio


class Demo(rmy.BaseRemoteObject):
    def __init__(self):
        self.greet = "Hello"

    @rmy.remote_async_method
    async def greet(self, name):
        return f"{self.greet} {name}!"

    async def conversation(self, name):
        yield f"Hello {name}!"
        await asyncio.sleep(1)
        yield f"How are you {name}?"
        await asyncio.sleep(1)
        yield f"Goodbye {name}!"

    @rmy.remote_async_generator
    async def count(self):
        i = 0
        try:
            while True:
                i += 1
                print("counting", i)
                yield "None"
        finally:
            print("finally called")


if __name__ == "__main__":
    rmy.run_tcp_server(8080, Demo())
