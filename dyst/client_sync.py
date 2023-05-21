import asyncio
import contextlib
from datetime import datetime
from typing import Any, Iterator, Optional

import anyio
import janus

from dyst import CLOSE_STREAM, EXCEPTION, OK


class SyncClientBase:
    def __init__(self, portal, async_client) -> None:
        self.portal = portal
        self.async_client = async_client

    @contextlib.asynccontextmanager
    async def _wrap_context_async_stream(self, cancellable_stream, *args, **kwargs):
        queue = janus.Queue()

        async def wrapper():
            try:
                async with cancellable_stream as result_stream:
                    async for value in result_stream:
                        await queue.async_q.put((OK, value))
            except anyio.get_cancelled_exc_class():
                raise
            except Exception as e:
                await queue.async_q.put((EXCEPTION, e))
            finally:
                await queue.async_q.put((CLOSE_STREAM, None))
                task_group.cancel_scope.cancel()

        def result_sync_iterator():
            while True:
                code, message = queue.sync_q.get()
                if code in CLOSE_STREAM:
                    queue.close()
                    break
                if code is EXCEPTION:
                    queue.close()
                    raise message
                yield message

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(wrapper)
            yield result_sync_iterator()

    def wrap_async_context_stream(self, cancellable_stream):
        return self.portal.wrap_async_context_manager(
            self._wrap_context_async_stream(cancellable_stream)
        )

    def wrap_awaitable(self, method, *args, **kwargs):
        return self.portal.call(
            lambda: method if asyncio.iscoroutine(method) else method, *args, **kwargs
        )


class SyncClient(SyncClientBase):
    def read_events(
        self,
        topic: str,
        start: Optional[datetime],
        end: Optional[datetime],
        time_stamps_only: bool = False,
    ):
        return self.wrap_async_context_stream(
            self.async_client.read_events(topic, start, end, time_stamps_only)
        )

    def write_event(
        self,
        topic: str,
        event: Any,
        time_stamp: Optional[datetime] = None,
        override: bool = False,
    ):
        return self.wrap_awaitable(
            self.async_client.write_event(topic, event, time_stamp, override)
        )


@contextlib.contextmanager
def create_sync_client(host_name: str, port: int, name: str) -> Iterator[SyncClient]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(
            create_async_client(host_name, port, name)
        ) as async_client:
            yield SyncClient(portal, async_client)