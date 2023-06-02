import contextlib
from functools import wraps
from pickle import dumps, loads
from typing import Any, AsyncIterator, List, Tuple

import anyio
import anyio.abc

import dyst.abc
from dyst import AsyncClientCore, SessionManager
from fountainhead.client import _create_async_client_core


ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS = 0.1
A_LITTLE_BIT_OF_TIME = 0.1
ERROR_MESSAGE = "an error occured"


class TestConnection(dyst.abc.Connection):
    def __init__(self, sink, stream, name: str) -> None:
        self.name: str = name
        self.sink = sink
        self.stream = stream
        self._closed = anyio.Event()

    async def send(self, message):
        try:
            await self.sink.send(dumps(message))
        except anyio.get_cancelled_exc_class():
            print(f"Sending {message} was cancelled, {self.name} did not send anything.")

    async def __anext__(self) -> Any:
        return loads(await self.stream.receive())

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def aclose(self):
        self.sink.close()
        self.stream.close()
        self._closed.set()

    async def wait_closed(self):
        await self._closed.wait()


def create_test_connection(
    first_name: str, second_name: str
) -> Tuple[TestConnection, TestConnection]:
    first_sink, first_stream = anyio.create_memory_object_stream(100)
    second_sink, second_stream = anyio.create_memory_object_stream(100)
    return TestConnection(first_sink, second_stream, first_name), TestConnection(
        second_sink, first_stream, second_name
    )


@contextlib.asynccontextmanager
async def create_test_environment_core(
    server, nb_clients: int = 1
) -> AsyncIterator[List[AsyncClientCore]]:
    clients = []
    async with anyio.create_task_group() as task_group:
        async with contextlib.AsyncExitStack() as exit_stack:
            for i in range(nb_clients):
                client_name = f"client_{i}"
                first, second = create_test_connection(client_name, "server")
                client = await exit_stack.enter_async_context(
                    _create_async_client_core(task_group, first, name=client_name)
                )
                client_session_core = await exit_stack.enter_async_context(
                    server.on_new_connection(second)
                )
                task_group.start_soon(client_session_core.process_messages)
                clients.append(client)
            yield clients
            task_group.cancel_scope.cancel()


@contextlib.asynccontextmanager
async def create_test_environment(
    remote_object_class, server=None, args=()
) -> AsyncIterator[Any]:
    async with create_test_environment_core(SessionManager(server)) as clients:
        async with clients[0].create_remote_object(remote_object_class, args, {}) as proxy:
            yield proxy
