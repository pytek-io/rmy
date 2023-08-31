import asyncio
import random

from example_base import demo_main

import rmy


class Demo(rmy.BaseRemoteObject):
    @rmy.remote_async_generator
    async def chat(self, name):
        for message in [f"Hello {name}!", "How are you?", f"Goodbye {name}!"]:
            yield message
            await asyncio.sleep(random.random())


def main_sync(proxy: Demo):
    print("What's your name?")
    name = input()
    for message in proxy.chat.eval(name):
        print(message)


async def main_async(proxy: Demo):
    print("What's your name?")
    name = input()
    async for message in proxy.chat.wait(name):
        print(message)


if __name__ == "__main__":
    demo_main(Demo(), main_sync, main_async)
