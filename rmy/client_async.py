from __future__ import annotations

import asyncio
import contextlib
import inspect
import io
import pickle
import queue
import traceback
from functools import partial, wraps
from itertools import count
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable, Optional

import anyio
import anyio.abc

from .abc import AsyncSink, Connection
from .common import RemoteException, cancel_task_on_exit, scoped_insert
from .connection import connect_to_tcp_server


if TYPE_CHECKING:
    from .client_sync import SyncClient


OK = "OK"
CLOSE_SENTINEL = "Close sentinel"
CANCEL_TASK = "Cancel task"
EXCEPTION = "Exception"
CREATE_OBJECT = "Create object"
DELETE_OBJECT = "Delete object"
FETCH_OBJECT = "Fetch object"
GET_ATTRIBUTE = "Get attribute"
SET_ATTRIBUTE = "Set attribute"
MOVE_GENERATOR_ITERATOR = "Move Async iterator"
EVALUATE_METHOD = "Evaluate method"
ITERATE_GENERATOR = "Iterate generator"
AWAIT_COROUTINE = "Await coroutine"
STREAM_BUFFER_SIZE = 10

SERVER_OBJECT_ID = 0

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
                AWAIT_COROUTINE,
                (payload,),
                is_cancellable=True,
                include_code=False,
            )
        else:
            raise pickle.UnpicklingError("Unsupported object")


class AsyncCallResult:
    def __init__(self, client, object_id: int, function: Callable, args, kwargs):
        self.client: AsyncClient = client
        self.object_id = object_id
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def __await__(self):
        return self.client._evaluate_async_method(
            self.object_id, self.function, self.args, self.kwargs
        ).__await__()

    def __aiter__(self):
        return self.client._evaluate_async_generator(
            self.object_id, self.function, self.args, self.kwargs
        )


def decode_result(code, result, _message_size, include_code=True):
    if code in (CANCEL_TASK, OK):
        return (code, result) if include_code else result
    if code == EXCEPTION:
        if isinstance(result, RemoteException):
            traceback.print_list(result.args[1])
            raise result.args[0]
        raise result if isinstance(result, Exception) else Exception(result)
    else:
        raise Exception(f"Unexpected code {code} received.")


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
    def __init__(self, client, object_id: int):
        self.client = client
        self.object_id = object_id


def __setattr_forbidden__(_self, _name, _value):
    raise AttributeError(ASYNC_SETATTR_ERROR_MESSAGE)


class AsyncClient:
    def __init__(
        self, connection: Connection, async_buffer_size: int = STREAM_BUFFER_SIZE
    ) -> None:
        connection.set_loads(self.loads)
        self.connection = connection
        self._async_buffer_size = async_buffer_size
        self.request_id = count()
        self.object_id = count()
        self.pending_requests = {}
        self.remote_objects = {}
        self.client_sync: Optional[SyncClient] = None

    async def _send(self, *args):
        await self.connection.send(args)

    def _send_nowait(self, *args):
        self.connection.send_nowait(args)

    def loads(self, value):
        return len(value), (RMY_Unpickler(io.BytesIO(value), self).load())

    async def _process_messages_from_server(
        self, task_status: anyio.abc.TaskStatus = anyio.TASK_STATUS_IGNORED
    ):
        task_status.started()
        async for message_size, (request_id, status, result) in self.connection:
            if future := self.pending_requests.get(request_id):
                future.set_result((status, result, message_size))
            else:
                print(f"Unexpected request id {request_id} received.")

    @contextlib.contextmanager
    def _submit_request(self, code, args, is_cancellable=False):
        future = asyncio.Future()
        request_id = next(self.request_id)
        with scoped_insert(self.pending_requests, request_id, future):
            self._send_nowait(code, request_id, args)
            try:
                yield future
            except anyio.get_cancelled_exc_class():
                if is_cancellable:
                    self._cancel_request_no_wait(request_id)
                raise

    async def _execute_request(self, code, args, is_cancellable=False, include_code=True) -> Any:
        with self._submit_request(code, args, is_cancellable) as future:
            return decode_result(*(await future), include_code=include_code)

    async def _get_attribute(self, object_id: int, name: str):
        return await self._execute_request(GET_ATTRIBUTE, (object_id, name), include_code=False)

    async def _set_attribute(self, object_id: int, name: str, value: Any):
        return await self._execute_request(
            SET_ATTRIBUTE, (object_id, name, value), include_code=False
        )

    def _call_method_remotely(self, object_id: int, function: Callable, *args, **kwargs) -> Any:
        return AsyncCallResult(self, object_id, function, args, kwargs)

    def _cancel_request_no_wait(self, request_id: int):
        self._send_nowait(CANCEL_TASK, request_id, ())
        self.pending_requests.pop(request_id, None)

    async def _cancel_request(self, request_id: int):
        self._cancel_request_no_wait(request_id)
        await self.connection.drain()

    async def _evaluate_async_method(self, object_id, function, args, kwargs):
        result = await self._execute_request(
            EVALUATE_METHOD,
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
            await self._send(ITERATE_GENERATOR, request_id, (generator_id, pull_or_push))
            try:
                async for code, result, message_size in queue:
                    terminated, value = decode_iteration_result(code, result)
                    if terminated:
                        break
                    yield value
                    await self._send(MOVE_GENERATOR_ITERATOR, generator_id, (message_size,))
            finally:
                await self._cancel_request(request_id)

    async def _evaluate_async_generator(self, object_id, function, args, kwargs):
        generator = await self._execute_request(
            EVALUATE_METHOD,
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
                await self._send(ITERATE_GENERATOR, request_id, (generator_id, pull_or_push))
                yield queue
            finally:
                await self._cancel_request(request_id)

    @contextlib.asynccontextmanager
    async def create_remote_object(
        self, object_class, args=(), kwarg={}, sync_client: Optional[SyncClient] = None
    ):
        object_id = await self._execute_request(
            CREATE_OBJECT, (object_class, args, kwarg), include_code=False
        )
        try:
            yield await self._fetch_remote_object(object_id, sync_client)
        finally:
            with anyio.CancelScope(shield=True):
                await self._send(DELETE_OBJECT, next(self.request_id), (object_id,))

    async def _fetch_remote_object(
        self, object_id: int = SERVER_OBJECT_ID, sync_client: Optional[SyncClient] = None
    ) -> Any:
        if object_id not in self.remote_objects:
            object_class = await self._execute_request(
                FETCH_OBJECT, (object_id,), include_code=False
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


@contextlib.asynccontextmanager
async def create_async_client(connection: Connection) -> AsyncIterator[AsyncClient]:
    client = AsyncClient(connection)
    with cancel_task_on_exit(client._process_messages_from_server()):
        yield client


@contextlib.asynccontextmanager
async def connect(host_name: str, port: int) -> AsyncIterator[AsyncClient]:
    async with connect_to_tcp_server(host_name, port) as connection:
        async with create_async_client(connection) as client:
            yield client
