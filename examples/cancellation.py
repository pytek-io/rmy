import asyncio

from example_base import main
import rmy


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

if __name__ == "__main__":
    main(Demo(), None, main_async)