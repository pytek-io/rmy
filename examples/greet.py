from example_base import demo_main

import rmy

class DemoInterface(rmy.BaseRemoteObject):
    @rmy.remote_async_method
    async def greet(self, name):
        ...

class Demo(rmy.BaseRemoteObject):
    @rmy.remote_async_method
    async def greet(self, name):
        if not name:
            raise ValueError("Name cannot be empty")
        return f"Hello {name}!"


def main_sync(proxy: DemoInterface):
    while True:
        print("What's your name?")
        name = input()
        print(proxy.greet.eval(name))


async def main_async(proxy: DemoInterface):
    while True:
        print("What's your name?")
        name = input()
        print(await proxy.greet.wait(name))


if __name__ == "__main__":
    demo_main(Demo(), main_sync, main_async, DemoInterface)
