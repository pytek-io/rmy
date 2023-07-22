import contextlib
import inspect
from typing import Iterator

import anyio

from .client_async import (
    SERVER_OBJECT_ID,
    AsyncClient,
    ClientSession,
    connect,
    decode_iteration_result,
)


async def await_awaitable(awaitable):
    return await awaitable


class SyncClient:
    def __init__(self, portal, async_client) -> None:
        self.portal = portal
        self.async_client: AsyncClient = async_client

    def _sync_generator_iter(self, generator_id, pull_or_push):
        with self.portal.wrap_async_context_manager(
            self.async_client._remote_sync_generator_iter(generator_id, pull_or_push)
        ) as queue:
            for code, time_stamp, result in queue:
                terminated, value = decode_iteration_result(code, result)
                if terminated:
                    break
                yield value
                self.portal.call(
                    self.async_client.send,
                    ClientSession.acknowledge_async_generator_data,
                    generator_id,
                    time_stamp,
                )

    def _wrap_function(self, object_id, function):
        def result(*args, **kwargs):
            result = self.portal.call(
                self.async_client._call_internal_method_remote,
                ClientSession.evaluate_method,
                (object_id, function, args, kwargs),
                False,
            )
            if inspect.iscoroutine(result):
                result = self.portal.call(await_awaitable, result)
            return result

        return result

    def _wrap_awaitable(self, method):
        def result(_self, *args, **kwargs):
            return self.portal.call(method, *args, **kwargs)

        return result

    def fetch_remote_object(self, object_id: int = SERVER_OBJECT_ID):
        return self.portal.call(self.async_client.fetch_remote_object, object_id)

    def create_remote_object(self, object_class, args=(), kwarg={}):
        return self.portal.wrap_async_context_manager(
            self.async_client.create_remote_object(object_class, args, kwarg, sync_client=self)
        )


@contextlib.contextmanager
def create_sync_client(host_name: str, port: int) -> Iterator[SyncClient]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(connect(host_name, port)) as async_client:
            result = SyncClient(portal, async_client)
            async_client.sync_client = result
            yield result
