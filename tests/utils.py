from __future__ import annotations

import contextlib
from itertools import count
from pickle import dumps, loads
from typing import Any, AsyncIterator, Iterator, List, Tuple, TypeVar

import anyio
import anyio.abc
import anyio.lowlevel
import pytest

import rmy.abc
from rmy import (
    RemoteCoroutine,
    RemoteGeneratorPull,
    RemoteGeneratorPush,
    Session,
    SyncClient,
    create_async_client,
    remote_generator_pull,
)
from rmy.client_async import ASYNC_GENERATOR_OVERFLOWED_MESSAGE
from rmy.server import Server


T_Retval = TypeVar("T_Retval")

ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS = 0.1
A_LITTLE_BIT_OF_TIME = 0.1
ERROR_MESSAGE = "an error occured"
TEST_CONNECTION_BUFFER_SIZE = 100


async def async_generator(bound: int) -> AsyncIterator[int]:
    for i in range(bound):
        yield i


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

    def send_nowait(self, message: Tuple[Any, ...]) -> int:
        try:
            serialized_message = self.dumps(message)
            self.sink.send_nowait(serialized_message)
            return len(serialized_message)
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            print(f"Sending {message} failed, {self.name} did not send anything.")
        return 0

    async def send(self, message) -> int:
        try:
            serialized_message = self.dumps(message)
            await self.sink.send(serialized_message)
            return len(serialized_message)
        except anyio.get_cancelled_exc_class():
            print(f"Sending {message} has been cancelled. {self} did not send anything.")
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            print(f"Sending {message} failed, {self.name} did not send anything.")
        return 0

    async def drain(self):
        await anyio.lowlevel.checkpoint()

    async def __anext__(self) -> Any:
        try:
            return self.loads(await self.stream.receive())
        except (anyio.EndOfStream, anyio.ClosedResourceError):
            raise StopAsyncIteration

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
    with pytest.raises(RuntimeError) as e_info:
        yield exception
        assert isinstance(e_info.value, type(exception))
        assert e_info.value.args[0] == exception.args[0]


@contextlib.asynccontextmanager
async def create_test_async_clients(
    server_object, nb_clients: int = 1
) -> AsyncIterator[List[Session]]:
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
                async_client.sync_client = sync_client
            yield sync_clients


@contextlib.asynccontextmanager
async def create_proxy_object_async(remote_object: T_Retval) -> AsyncIterator[T_Retval]:
    async with create_test_async_clients(remote_object, nb_clients=1) as (client,):
        yield await client._fetch_object_local()


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
                self.current_value = i
                yield i
        except GeneratorExit:
            print("Generator exit")
        finally:
            self.finally_called = True

    async def count_to_infinity_nowait(self) -> AsyncIterator[int]:
        try:
            counter = count()
            while True:
                yield next(counter)
        finally:
            self.finally_called = True

    async def async_generator_exception(self, exception) -> AsyncIterator[int]:
        for i in range(10):
            await anyio.sleep(A_LITTLE_BIT_OF_TIME)
            yield i
            if i == 3:
                await self.throw_exception_coroutine(exception)

    def nested_generators(self, bound: int):
        return [
            RemoteGeneratorPull(range(bound)),
            RemoteGeneratorPull(async_generator(bound)),
            RemoteGeneratorPush(async_generator(bound)),
        ]

    def nested_coroutine(self):
        async def test_coroutine():
            return 1

        return [RemoteCoroutine(test_coroutine())]

    @remote_generator_pull
    async def remote_generator_pull_synced(self) -> AsyncIterator[int]:
        counter = count()
        while True:
            yield next(counter)
