from __future__ import annotations

import contextlib
from pickle import dumps, loads
from typing import Any, AsyncIterator, Iterator, List, Tuple, TypeVar

import anyio
import anyio.abc
import anyio.lowlevel
import pytest

import rmy.abc
from rmy.server import Server
from rmy import AsyncClient, SyncClient, create_async_client, RemoteException
from rmy.client_async import ASYNC_GENERATOR_OVERFLOWED_MESSAGE

T_Retval = TypeVar("T_Retval")

ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS = 0.1
A_LITTLE_BIT_OF_TIME = 0.1
ERROR_MESSAGE = "an error occured"
TEST_CONNECTION_BUFFER_SIZE = 100


class TestConnection(rmy.abc.Connection):
    def __init__(self, sink, stream, name: str) -> None:
        self.name: str = name
        self.sink = sink
        self.stream = stream
        self._closed = anyio.Event()
        self.dumps = dumps
        self.loads = loads

    def set_dumps(self, dumps):
        self.dumps = dumps

    def set_loads(self, loads):
        self.loads = loads

    def send_nowait(self, message: Tuple[Any, ...]):
        return self.sink.send_nowait(self.dumps(message))

    async def drain(self):
        await anyio.lowlevel.checkpoint()

    async def send(self, message):
        try:
            await self.sink.send(self.dumps(message))
        except anyio.get_cancelled_exc_class():
            print(f"Sending {message} has been cancelled. {self} did not send anything.")
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            print(f"Sending {message} failed, {self.name} did not send anything.")

    async def __anext__(self) -> Any:
        try:
            return self.loads(await self.stream.receive())
        except anyio.EndOfStream:
            raise StopAsyncIteration

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    def close(self):
        self.sink.close()
        self.stream.close()
        self._closed.set()

    async def __aenter__(self) -> TestConnection:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.close()

    async def wait_closed(self):
        await self._closed.wait()

    def __str__(self) -> str:
        return f"TestConnection({self.name})"


def create_test_connection(
    first_name: str, second_name: str
) -> Tuple[TestConnection, TestConnection]:
    first_sink, first_stream = anyio.create_memory_object_stream(TEST_CONNECTION_BUFFER_SIZE)
    second_sink, second_stream = anyio.create_memory_object_stream(TEST_CONNECTION_BUFFER_SIZE)
    return TestConnection(first_sink, second_stream, first_name), TestConnection(
        second_sink, first_stream, second_name
    )


@contextlib.contextmanager
def test_exception():
    exception = RuntimeError(ERROR_MESSAGE)
    with pytest.raises(RemoteException) as e_info:
        yield exception
        returned_exception = e_info.value.args[0]
        assert isinstance(returned_exception, type(exception))
        assert returned_exception.args[0] == exception.args[0]


@contextlib.asynccontextmanager
async def create_test_async_clients(
    server_object, nb_clients: int = 1
) -> AsyncIterator[List[AsyncClient]]:
    server = Server(server_object)
    async with anyio.create_task_group() as test_task_group:
        async with contextlib.AsyncExitStack() as exit_stack:
            clients = []
            # for i in range(nb_clients):
            i = 0
            client_name = f"client_{i}"
            connection_end_1, connection_end_2 = create_test_connection(client_name, "server")
            client = await exit_stack.enter_async_context(create_async_client(connection_end_1))
            clients.append(client)
            client_session = await exit_stack.enter_async_context(
                server.on_new_connection(connection_end_2)
            )
            test_task_group.start_soon(client_session.process_messages)
            await exit_stack.enter_async_context(connection_end_1)
            await exit_stack.enter_async_context(connection_end_2)
            yield clients
            test_task_group.cancel_scope.cancel()


@contextlib.contextmanager
def create_test_sync_clients(server_object, nb_clients: int = 1) -> Iterator[List[SyncClient]]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(
            create_test_async_clients(server_object, nb_clients)
        ) as async_clients:
            sync_clients = [SyncClient(portal, async_client) for async_client in async_clients]
            for sync_client, async_client in zip(sync_clients, async_clients):
                async_client.client_sync = sync_client
            yield sync_clients


@contextlib.asynccontextmanager
async def create_proxy_object_async(remote_object: T_Retval) -> AsyncIterator[T_Retval]:
    async with create_test_async_clients(remote_object, nb_clients=1) as (client,):
        yield await client.fetch_remote_object()


@contextlib.contextmanager
def create_proxy_object_sync(remote_object: T_Retval) -> Iterator[T_Retval]:
    with create_test_sync_clients(remote_object, nb_clients=1) as (client,):
        yield client.fetch_remote_object(0)


@contextlib.contextmanager
def create_test_proxy_object_sync(remote_object_class, server=None, args=()) -> Iterator[Any]:
    with create_test_sync_clients(server, nb_clients=1) as (client,):
        with client.create_remote_object(remote_object_class, args, {}) as proxy:
            yield proxy


class RemoteObject:
    def __init__(self, attribute=None) -> None:
        self.attribute = attribute
        self.ran_tasks = 0
        self.current_value = 0
        self.finally_called = False

    async def echo_coroutine(self, message: str):
        await anyio.sleep(A_LITTLE_BIT_OF_TIME)
        return message

    def echo_sync(self, message: str):
        return message

    async def throw_exception_coroutine(self, exception):
        raise exception

    async def sleep_forever(self):
        try:
            await anyio.sleep_forever()
        finally:
            self.ran_tasks += 1

    async def count(self, bound: int) -> AsyncIterator[int]:
        try:
            for i in range(bound):
                await anyio.sleep(A_LITTLE_BIT_OF_TIME)
                self.current_value = i
                yield i
        finally:
            self.finally_called = True

    def count_sync(self, bound: int) -> Iterator[int]:
        try:
            for i in range(bound):
                # time.sleep(A_LITTLE_BIT_OF_TIME)
                self.current_value = i
                print(f"Sending {i}")
                yield i
        except GeneratorExit:
            print("Generator exit")
        finally:
            self.finally_called = True

    async def count_nowait(self, bound: int) -> AsyncIterator[int]:
        for i in range(bound):
            yield i

    async def async_generator_exception(self, exception) -> AsyncIterator[int]:
        for i in range(10):
            await anyio.sleep(A_LITTLE_BIT_OF_TIME)
            yield i
            if i == 3:
                await self.throw_exception_coroutine(exception)
