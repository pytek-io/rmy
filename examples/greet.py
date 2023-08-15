from example_base import main

import rmy


class Demo(rmy.BaseRemoteObject):
    @rmy.remote_async_method
    async def greet(self, name):
        if not name:
            raise ValueError("Name cannot be empty")
        return f"Hello {name}!"


def main_sync(proxy: Demo):
    while True:
        print("What's your name?")
        name = input()
        print(proxy.greet.eval(name))


async def main_async(proxy: Demo):
    while True:
        print("What's your name?")
        name = input()
        print(await proxy.greet.wait(name))


if __name__ == "__main__":
    main(Demo(), main_sync, main_async)
