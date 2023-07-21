from __future__ import annotations

import asyncio
import contextlib
import inspect
import io
import pickle
import queue
import sys
import traceback
from functools import partial, wraps
from itertools import count
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable, Dict, Optional

import anyio
import anyio.abc
import asyncstdlib

from .abc import AsyncSink, Connection
from .common import RemoteException, cancel_task_on_exit, scoped_insert
from .connection import connect_to_tcp_server


if TYPE_CHECKING:
    from .client_sync import SyncClient


OK = "OK"
CLOSE_SENTINEL = "Close sentinel"
CANCEL_TASK = "Cancel task"
EXCEPTION = "Exception"

SERVER_OBJECT_ID = 0
MAX_DATA_SIZE_IN_FLIGHT = 1_000
MAX_DATA_NB_IN_FLIGHT = 10


ASYNC_SETATTR_ERROR_MESSAGE = "Cannot set attribute on remote object in async mode. Use setattr method instead. \
We intentionally do not support setting attributes using assignment operator on remote objects in async mode. \
This is because it is not a good practice not too wait until a remote operation completes."

ASYNC_GENERATOR_OVERFLOWED_MESSAGE = "Generator iteration overflowed."


class IterationBufferSync(AsyncSink):
    def __init__(self) -> None:
        self._queue = queue.SimpleQueue()

    def set_result(self, value: Any):
        self._queue.put_nowait(value)

    def __iter__(self):
        return self

    def __next__(self):
        return self._queue.get()


class IterationBufferAsync(AsyncSink):
    def __init__(self) -> None:
        self._overflowed = False
        self._queue = asyncio.Queue()

    def set_result(self, value: Any):
        self._queue.put_nowait(value)

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self._queue.get()


class RemoteValue:
    def __init__(self, value):
        self.value = value

    def __iter__(self):
        return self.value.__iter__()

    def __aiter__(self):
        return self.value.__aiter__()


class RemoteGeneratorPush(RemoteValue):
    def __init__(self, value, max_data_in_flight_size=100, max_data_in_flight_count=100):
        if not inspect.isasyncgen(value):
            raise TypeError(
                f"RemoteGeneratorPush can only be used with async generators, received: {type(value)}."
            )
        super().__init__(value)


class RemoteGeneratorPull(RemoteValue):
    pass


class RemoteCoroutine(RemoteValue):
    def __await__(self):
        return self.value.__await__()


def remote_generator_push(method: Callable):
    @wraps(method)
    def result(*args, **kwargs):
        return RemoteGeneratorPush(method(*args, **kwargs))

    return result


def remote_generator_pull(method: Callable):
    @wraps(method)
    def result(*args, **kwargs):
        return RemoteGeneratorPull(method(*args, **kwargs))

    return result


class RMY_Pickler(pickle.Pickler):
    def __init__(self, client_session, file):
        super().__init__(file)
        self.client_session = client_session

    def persistent_id(self, obj):
        if isinstance(obj, RemoteValue):
            return (type(obj).__name__, self.client_session.store_value(obj.value))


class RMY_Unpickler(pickle.Unpickler):
    def __init__(self, file, client: AsyncClient):
        super().__init__(file)
        self.client: AsyncClient = client

    def persistent_load(self, value):
        type_tag, payload = value
        if type_tag in ("RemoteGeneratorPush", "RemoteGeneratorPull"):
            pull_or_push = type_tag == "RemoteGeneratorPull"
            if self.client.client_sync:
                return self.client.client_sync._sync_generator_iter(payload, pull_or_push)
            return self.client.fetch_values_async(payload, pull_or_push)
        elif type_tag == "RemoteCoroutine":
            return self.client._execute_request(
                ClientSession.evaluate_coroutine,
                (payload,),
                is_cancellable=True,
                include_code=False,
            )
        else:
            raise pickle.UnpicklingError("Unsupported object")


class AsyncCallResult:
    def __init__(self, client, remote_value_id: int, function: Callable, args, kwargs):
        self.client: AsyncClient = client
        self.remote_value_id = remote_value_id
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def __await__(self):
        return self.client._evaluate_async_method(
            self.remote_value_id, self.function, self.args, self.kwargs
        ).__await__()

    def __aiter__(self):
        return self.client._evaluate_async_generator(
            self.remote_value_id, self.function, self.args, self.kwargs
        )


def decode_iteration_result(code, result):
    if code in (CLOSE_SENTINEL, CANCEL_TASK):
        return True, None
    if code == EXCEPTION:
        if isinstance(result, RemoteException):
            traceback.print_list(result.args[1])
            raise result.args[0]
        raise result if isinstance(result, Exception) else Exception(result)
    return False, result


class RemoteObject:
    def __init__(self, client, remote_value_id: int):
        self.client = client
        self.object_id = remote_value_id


def __setattr_forbidden__(_self, _name, _value):
    raise AttributeError(ASYNC_SETATTR_ERROR_MESSAGE)


class AsyncClient:
    def __init__(self, connection: Connection) -> None:
        connection.set_loads(self.loads)
        self.connection = connection
        self.request_id = count()
        self.object_id = count()
        self.pending_requests = {}
        self.remote_objects = {}
        self.client_sync: Optional[SyncClient] = None

    async def send(self, *args):
        return await self.connection.send(args)

    def _send_nowait(self, *args):
        return self.connection.send_nowait(args)

    def loads(self, value):
        return RMY_Unpickler(io.BytesIO(value), self).load()

    def set_result(self, request_id: int, message_size: int, status: str, result: Any):
        if future := self.pending_requests.get(request_id):
            future.set_result((status, result, message_size))
        else:
            print(f"Unexpected request id {request_id} received.")

    async def process_messages(
        self, task_status: anyio.abc.TaskStatus = anyio.TASK_STATUS_IGNORED
    ):
        task_status.started()
        async for message_size, (request_id, *args) in self.connection:
            self.set_result(request_id, message_size, *args)

    async def _execute_request(self, code, args, is_cancellable=False, include_code=True) -> Any:
        future = asyncio.Future()
        request_id = next(self.request_id)
        with scoped_insert(self.pending_requests, request_id, future):
            self._send_nowait(code, request_id, *args)
            try:
                code, result, _message_size = await future
                if code in (CANCEL_TASK, OK):
                    return result
                if code == EXCEPTION:
                    if isinstance(result, RemoteException):
                        traceback.print_list(result.args[1])
                        raise result.args[0]
                    raise result if isinstance(result, Exception) else Exception(result)
                else:
                    raise Exception(f"Unexpected code {code} received.")
            except anyio.get_cancelled_exc_class():
                if is_cancellable:
                    self._cancel_request_no_wait(request_id)
                raise

    async def _get_attribute(self, object_id: int, name: str):
        return await self._execute_request(
            ClientSession.get_attribute, (object_id, name), include_code=False
        )

    async def _set_attribute(self, object_id: int, name: str, value: Any):
        return await self._execute_request(
            ClientSession.set_attribute, (object_id, name, value), include_code=False
        )

    def _call_method_remotely(
        self, remote_value_id: int, function: Callable, *args, **kwargs
    ) -> Any:
        return AsyncCallResult(self, remote_value_id, function, args, kwargs)

    def _cancel_request_no_wait(self, request_id: int):
        self._send_nowait(ClientSession.cancel_task, request_id)
        self.pending_requests.pop(request_id, None)

    async def _cancel_request(self, request_id: int):
        self._cancel_request_no_wait(request_id)
        await self.connection.drain()

    async def _evaluate_async_method(self, object_id, function, args, kwargs):
        result = await self._execute_request(
            ClientSession.evaluate_method,
            (object_id, function, args, kwargs),
            is_cancellable=True,
            include_code=False,
        )
        if inspect.iscoroutine(result):
            result = await result
        return result

    async def fetch_values_async(self, generator_id: int, pull_or_push: bool):
        queue = IterationBufferAsync()
        request_id = next(self.request_id)
        with scoped_insert(self.pending_requests, request_id, queue):
            await self.send(
                ClientSession.iterate_generator, request_id, generator_id, pull_or_push
            )
            try:
                async for code, result, message_size in queue:
                    terminated, value = decode_iteration_result(code, result)
                    if terminated:
                        break
                    yield value
                    await self.send(
                        ClientSession.move_async_generator_index, generator_id, message_size
                    )
            finally:
                await self._cancel_request(request_id)

    async def _evaluate_async_generator(self, object_id, function, args, kwargs):
        generator = await self._execute_request(
            ClientSession.evaluate_method,
            (object_id, function, args, kwargs),
            is_cancellable=True,
            include_code=False,
        )
        async for value in generator:
            yield value

    @contextlib.asynccontextmanager
    async def _remote_sync_generator_iter(self, generator_id: int, pull_or_push: bool):
        queue = IterationBufferSync()
        request_id = next(self.request_id)
        with scoped_insert(self.pending_requests, request_id, queue):
            try:
                await self.send(
                    ClientSession.iterate_generator, request_id, generator_id, pull_or_push
                )
                yield queue
            finally:
                await self._cancel_request(request_id)

    @contextlib.asynccontextmanager
    async def create_remote_object(
        self, object_class, args=(), kwarg={}, sync_client: Optional[SyncClient] = None
    ):
        object_id = await self._execute_request(
            ClientSession.create_object, (object_class, args, kwarg), include_code=False
        )
        try:
            yield await self._fetch_remote_object(object_id, sync_client)
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(ClientSession.delete_object, next(self.request_id), object_id)

    async def _fetch_remote_object(
        self, object_id: int = SERVER_OBJECT_ID, sync_client: Optional[SyncClient] = None
    ) -> Any:
        if object_id not in self.remote_objects:
            object_class = await self._execute_request(
                ClientSession.fetch_object, (object_id,), include_code=False
            )
            setattr = partial(self._set_attribute, object_id)
            __getattr__ = partial(self._get_attribute, object_id)
            if sync_client:
                __getattr__ = sync_client._wrap_awaitable(__getattr__)
                setattr = __setattr__ = sync_client._wrap_awaitable(setattr)
            else:
                __setattr__ = __setattr_forbidden__
            object_class = type(
                f"{object_class.__name__}Proxy",
                (RemoteObject, object_class),
                {"__getattr__": __getattr__, "setattr": setattr, "__setattr__": __setattr__},
            )
            remote_object = object_class.__new__(object_class)
            object.__setattr__(remote_object, "client", self)
            object.__setattr__(remote_object, "object_id", object_id)
            for name in dir(object_class):
                if name.startswith("__") and name.endswith("__"):
                    continue
                attribute = getattr(object_class, name)
                if inspect.isfunction(attribute):
                    method = partial(self._call_method_remotely, object_id, attribute)
                    if sync_client:
                        method = sync_client._wrap_function(object_id, attribute)
                    object.__setattr__(remote_object, name, method)
            self.remote_objects[object_id] = remote_object
        return self.remote_objects[object_id]

    async def fetch_remote_object(self, object_id: int = SERVER_OBJECT_ID):
        return await self._fetch_remote_object(object_id)


async def wrap_sync_generator(sync_generator):
    for value in sync_generator:
        yield value


async def wrap_coroutine(coroutine):
    return OK, await coroutine


class GeneratorState:
    def __init__(self):
        self.messages_in_flight_total_size = 0
        self.nb_messages_in_flight = 0
        self.acknowledged_message = anyio.Event()


class AsyncClient1:
    def __init__(self, connection: Connection) -> None:
        connection.set_loads(self.loads)
        self.connection = connection
        self.request_id = count()
        self.object_id = count()
        self.pending_requests = {}
        self.remote_objects = {}
        self.client_sync: Optional[SyncClient] = None

    async def process_messages_client(
        self, task_status: anyio.abc.TaskStatus = anyio.TASK_STATUS_IGNORED
    ):
        task_status.started()
        async for message_size, (request_id, *args) in self.connection:
            self.set_result(request_id, message_size, *args)

    async def process_messages_server(self):
        async for message_size, (method, request_id, *payload) in self.connection:
            try:
                result = method(self, request_id, *payload)
                if inspect.isawaitable(result):
                    await result
            except anyio.get_cancelled_exc_class():
                raise
            except Exception:
                stack = traceback.format_exc()
                await self.send(request_id, EXCEPTION, stack)


class ClientSession:
    def __init__(self, connection: Connection, server_object: Any, task_group) -> None:
        connection.set_dumps(self.dumps)
        self.connection = connection
        self.task_group = task_group
        self.remote_value_id = count()
        self.objects = {next(self.remote_value_id): server_object}
        self.tasks_cancel_callbacks = {}
        self.pending_results = {}
        self.generator_states: Dict[int, GeneratorState] = {}
        self.max_data_size_in_flight = MAX_DATA_SIZE_IN_FLIGHT
        self.max_data_nb_in_flight = MAX_DATA_NB_IN_FLIGHT

    def dumps(self, value):
        file = io.BytesIO()
        RMY_Pickler(self, file).dump(value)
        return file.getvalue()

    async def send(self, *args) -> int:
        return await self.connection.send(args)

    def send_nowait(self, *args) -> int:
        return self.connection.send_nowait(args)

    async def iterate_through_async_generator(
        self, request_id: int, iterator_id: int, coroutine_or_async_generator, pull_or_push: bool
    ):
        generator_state = GeneratorState()
        with scoped_insert(self.generator_states, iterator_id, generator_state):
            async with asyncstdlib.scoped_iter(coroutine_or_async_generator) as aiter:
                async for value in aiter:
                    message_size = await self.send(request_id, OK, value)
                    generator_state.messages_in_flight_total_size += message_size
                    generator_state.nb_messages_in_flight += 1
                    if (
                        generator_state.messages_in_flight_total_size
                        > self.max_data_size_in_flight
                        or generator_state.nb_messages_in_flight > self.max_data_nb_in_flight
                    ):
                        if pull_or_push:
                            await self.send(request_id, "PULL", None)
                        else:
                            raise OverflowError(
                                " ".join(
                                    [
                                        ASYNC_GENERATOR_OVERFLOWED_MESSAGE,
                                        f"Current data size in flight {generator_state.messages_in_flight_total_size}, max is {self.max_data_size_in_flight}.",
                                        f"Current number of messages in flight: {generator_state.nb_messages_in_flight}, max is {self.max_data_nb_in_flight}.",
                                    ]
                                )
                            )
                        await generator_state.acknowledged_message.wait()
            return CLOSE_SENTINEL, None

    def iterate_generator(self, request_id: int, iterator_id: int, pull_or_push: bool):
        if not (generator := self.pending_results.pop(iterator_id, None)):
            return
        self.cancellable_run_task(
            request_id,
            self.iterate_through_async_generator(request_id, iterator_id, generator, pull_or_push),
        )

    def evaluate_coroutine(self, request_id: int, coroutine_id: int):
        if not (coroutine := self.pending_results.pop(coroutine_id, None)):
            return
        self.cancellable_run_task(request_id, wrap_coroutine(coroutine))

    async def run_task(self, request_id, coroutine_or_async_generator):
        status, result = EXCEPTION, None
        try:
            status, result = await coroutine_or_async_generator
        except anyio.get_cancelled_exc_class():
            status = CANCEL_TASK
            raise
        except Exception as e:
            _, e, tb = sys.exc_info()
            status, result = EXCEPTION, RemoteException(e, traceback.extract_tb(tb)[3:])
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(request_id, status, result)

    def cancellable_run_task(self, request_id, coroutine_or_async_context):
        async def task():
            task_group = anyio.create_task_group()
            with scoped_insert(
                self.tasks_cancel_callbacks, request_id, task_group.cancel_scope.cancel
            ):
                async with task_group:
                    task_group.start_soon(
                        self.run_task,
                        request_id,
                        coroutine_or_async_context,
                    )

        self.task_group.start_soon(task)

    async def create_object(self, request_id, object_class, args, kwarg):
        object_id = next(self.remote_value_id)
        code, message = OK, object_id
        try:
            self.objects[object_id] = object_class(*args, **kwarg)
        except Exception:
            code, message = EXCEPTION, traceback.format_exc()
        await self.send(request_id, code, message)

    async def fetch_object(self, request_id, object_id):
        maybe_object = self.objects.get(object_id)
        if maybe_object is not None:
            await self.send(request_id, OK, maybe_object.__class__)
        else:
            await self.send(request_id, EXCEPTION, f"Object {object_id} not found")

    async def get_attribute(self, request_id, object_id, name):
        code, value = OK, None
        try:
            value = getattr(self.objects[object_id], name)
        except Exception as e:
            code, value = EXCEPTION, e
        await self.send(request_id, code, value)

    async def set_attribute(self, request_id, object_id, name, value):
        code, result = OK, None
        try:
            setattr(self.objects[object_id], name, value)
        except Exception as e:
            code, result = EXCEPTION, e
        await self.send(request_id, code, result)

    async def cancel_task(self, request_id: int):
        if running_task_cancel_callback := self.tasks_cancel_callbacks.get(request_id):
            running_task_cancel_callback()

    def move_async_generator_index(self, request_id: int, message_size: int):
        if generator_state := self.generator_states.get(request_id):
            generator_state.messages_in_flight_total_size -= message_size
            generator_state.nb_messages_in_flight -= 1
            generator_state.acknowledged_message.set()

    async def evaluate_method(self, request_id, object_id, method, args, kwargs):
        result = method(self.objects[object_id], *args, **kwargs)
        if inspect.iscoroutine(result):
            result = RemoteCoroutine(result)
        elif inspect.isasyncgen(result):
            result = RemoteGeneratorPush(result)
        elif inspect.isgenerator(result):
            result = RemoteGeneratorPull(result)
        await self.send(request_id, OK, result)

    def store_value(self, value: Any):
        value_id = next(self.remote_value_id)
        self.pending_results[value_id] = value
        return value_id

    def delete_object(self, _request_id, object_id: int):
        self.objects.pop(object_id, None)

    async def process_messages(self):
        async for message_size, (method, request_id, *payload) in self.connection:
            try:
                result = method(self, request_id, *payload)
                if inspect.isawaitable(result):
                    await result
            except anyio.get_cancelled_exc_class():
                raise
            except Exception:
                stack = traceback.format_exc()
                await self.send(request_id, EXCEPTION, stack)

    async def aclose(self):
        self.task_group.cancel_scope.cancel()


@contextlib.asynccontextmanager
async def create_async_client(connection: Connection) -> AsyncIterator[AsyncClient]:
    client = AsyncClient(connection)
    with cancel_task_on_exit(client.process_messages()):
        yield client


@contextlib.asynccontextmanager
async def connect(host_name: str, port: int) -> AsyncIterator[AsyncClient]:
    async with connect_to_tcp_server(host_name, port) as connection:
        async with create_async_client(connection) as client:
            yield client
