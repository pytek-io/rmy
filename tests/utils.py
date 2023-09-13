from __future__ import annotations
import contextlib
from itertools import count
from pickle import dumps, loads
from typing import (
    Any,
    AsyncIterator,
    Iterator,
    Tuple,
    TypeVar,
    ContextManager,
    Callable,
    AsyncContextManager,
)

import anyio
import anyio.abc
import anyio.lowlevel
import pytest

import rmy.abc
from rmy import RemoteGeneratorPull, RemoteGeneratorPush, RemoteObject, Session
from rmy.client_async import AsyncClient, create_session
from rmy.client_sync import SyncClient
from rmy.server import Server
from rmy.session import RemoteAwaitable


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

    def send_nowait(self, message: Tuple[Any, ...]) -> int:
        try:
            serialized_message = dumps(message)
            self.sink.send_nowait(serialized_message)
            return len(serialized_message)
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            print(f"Sending {message} failed, {self.name} did not send anything.")
        return 0

    async def send(self, message) -> int:
        try:
            print(self.name, "=>", message)
            serialized_message = dumps(message)
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
            return loads(await self.stream.receive())
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
def check_remote_exception(exception: Exception = RuntimeError(ERROR_MESSAGE)):
    with pytest.raises(RuntimeError) as e_info:
        yield exception
    assert isinstance(e_info.value, type(exception))
    assert e_info.value.args[0] == exception.args[0]


@contextlib.contextmanager
def check_exception(expected_exception: Exception):
    with pytest.raises(Exception) as e_info:
        yield expected_exception
    assert isinstance(e_info.value, type(expected_exception))
    assert e_info.value.args[0].startswith(expected_exception.args[0])


@contextlib.asynccontextmanager
async def generate_session_pairs(
    server_object: Any,
) -> AsyncIterator[
    Callable[
        [], AsyncContextManager[Tuple[Session, rmy.abc.Connection, Session, rmy.abc.Connection]]
    ]
]:
    server = Server()
    server.register_object(server_object)

    @contextlib.asynccontextmanager
    async def result(client_name="test client"):
        async with contextlib.AsyncExitStack() as exit_stack:
            await exit_stack.enter_async_context(anyio.create_task_group())
            client_connection, server_connection = create_test_connection(client_name, "server")
            client_session = await exit_stack.enter_async_context(
                create_session(client_connection)
            )
            server_session = await exit_stack.enter_async_context(
                server.on_new_connection(server_connection)
            )
            await exit_stack.enter_async_context(client_connection)
            await exit_stack.enter_async_context(server_connection)
            yield server_session, server_connection, client_session, client_connection

    yield result


@contextlib.asynccontextmanager
async def create_test_async_clients(
    server_object: RemoteObject,
) -> AsyncIterator[Callable[[], AsyncContextManager[AsyncClient]]]:
    async with generate_session_pairs(server_object) as sessions:

        @contextlib.asynccontextmanager
        async def result():
            async with sessions() as (
                server_session,
                server_connection,
                client_session,
                client_connection,
            ):
                yield AsyncClient(client_session)

        yield result


@contextlib.contextmanager
def create_test_sync_clients(
    server_object: Any,
) -> Iterator[Callable[[], ContextManager[SyncClient]]]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(generate_session_pairs(server_object)) as sessions:

            @contextlib.contextmanager
            def result():
                with portal.wrap_async_context_manager(sessions()) as (
                    server_session,
                    server_connection,
                    client_session,
                    client_connection,
                ):
                    sync_client = SyncClient(portal, client_session)
                    client_session.sync_client = sync_client
                    yield sync_client

            yield result


@contextlib.asynccontextmanager
async def create_proxy_object_async(
    remote_object: T_Retval,
) -> AsyncIterator[Callable[[], AsyncContextManager[Tuple[T_Retval, rmy.abc.Connection]]]]:
    @contextlib.asynccontextmanager
    async def result():
        async with generate_session_pairs(remote_object) as sessions:
            async with sessions() as (
                server_session,
                server_connection,
                client_session,
                client_connection,
            ):
                yield await client_session.fetch_remote_object(
                    type(remote_object)
                ), client_connection

    yield result


@contextlib.contextmanager
def create_proxy_object_sync(remote_object: T_Retval) -> Iterator[T_Retval]:
    with create_test_sync_clients(remote_object) as sessions:
        with sessions() as client:
            yield client.fetch_remote_object(type(remote_object))


class TestObject(RemoteObject):
    def __init__(self, attribute=None) -> None:
        self.attribute = attribute
        self.ran_tasks = 0
        self.current_value = 0
        self.finally_called = False

    @rmy.remote_method
    def get_finally_called(self) -> bool:
        return self.finally_called

    @rmy.remote_method
    def get_ran_tasks(self) -> int:
        return self.ran_tasks

    @rmy.remote_method
    def get_current_value(self) -> int:
        return self.current_value

    @rmy.remote_method
    async def echo_coroutine(self, value: T_Retval) -> T_Retval:
        await anyio.sleep(A_LITTLE_BIT_OF_TIME)
        import pickle

        pickle.dumps(value)
        return value

    @rmy.remote_method
    def echo_sync(self, message: str):
        return message

    @rmy.remote_method
    async def throw_exception_coroutine(self, exception):
        raise exception

    @rmy.remote_method
    async def sleep_forever(self):
        try:
            await anyio.sleep_forever()
        finally:
            self.ran_tasks += 1

    @rmy.remote_generator
    async def count(self, bound: int) -> AsyncIterator[int]:
        try:
            for i in range(bound):
                await anyio.sleep(A_LITTLE_BIT_OF_TIME)
                self.current_value = i
                yield i
        finally:
            self.finally_called = True

    @rmy.remote_generator
    def count_sync(self, bound: int) -> Iterator[int]:
        try:
            for i in range(bound):
                self.current_value = i
                yield i
        except GeneratorExit:
            print("Generator exit")
        finally:
            self.finally_called = True

    @rmy.remote_generator
    async def count_to_infinity_nowait(self) -> AsyncIterator[int]:
        try:
            counter = count()
            while True:
                yield next(counter)
        finally:
            self.finally_called = True

    @rmy.remote_generator
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

    @rmy.remote_method
    def nested_coroutine(self):
        async def test_coroutine():
            return 1

        return [RemoteAwaitable(test_coroutine())]

    @rmy.remote_generator
    async def remote_generator_unsynced(self) -> AsyncIterator[int]:
        counter = count()
        while True:
            yield next(counter)

    @rmy.remote_context_manager
    @contextlib.contextmanager
    def sync_context_manager(self, value: T_Retval) -> Iterator[T_Retval]:
        try:
            yield value
        finally:
            self.current_value = 1

    @rmy.remote_context_manager
    @contextlib.asynccontextmanager
    async def async_context_manager(self, value: T_Retval) -> AsyncIterator[T_Retval]:
        try:
            yield value
        finally:
            self.current_value = 1

    def test(self):
        return "test"

    @rmy.remote_context_manager
    @contextlib.asynccontextmanager
    async def remote_object_from_context(self, attribute: Any = None) -> AsyncIterator[TestObject]:
        yield TestObject()


def receive_test_object_async(method):
    async def wrapper():
        async with create_proxy_object_async(TestObject()) as proxies:
            async with proxies() as (proxy, connection):
                await method(proxy)

    return wrapper


def receive_test_object_sync(method):
    def wrapper():
        with create_proxy_object_sync(TestObject()) as proxy:
            method(proxy)

    return wrapper
