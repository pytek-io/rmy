from __future__ import annotations

import asyncio
import contextlib
import contextvars
import datetime
import inspect
import io
import pickle
import queue
import sys
import traceback
from functools import wraps
from itertools import count
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Iterator,
    TypeVar,
)

import anyio
import anyio.abc
import asyncstdlib

from .abc import AsyncSink, Connection
from .common import RemoteException, scoped_insert


if TYPE_CHECKING:
    from .client_sync import SyncClient

current_session: contextvars.ContextVar[Session] = contextvars.ContextVar("client")

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

if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

T_Retval = TypeVar("T_Retval")
T_ParamSpec = ParamSpec("T_ParamSpec")
T = TypeVar("T")


class decorator(Generic[T_ParamSpec, T_Retval]):
    def __init__(self, method):
        self._method = method

    def __get__(self, instance, _class):
        self._instance = instance
        return self

    def __call__(self, *args, **kwargs):
        assert not self._instance.is_proxy, "Cannot call remote method directly on proxy object."
        return self._method(self._instance, *args, **kwargs)

    def _eval_async(self, args, kwargs):
        return self._instance.session.evaluate_method_local(
            self._instance.object_id, self._method.__name__, args, kwargs
        )

    def _eval_sync(self, args, kwargs):
        assert self._instance.is_proxy
        return self._instance.session.sync_client._wrap_function(
            self._instance.object_id, self._method.__name__
        )(*args, **kwargs)


class remote_method(decorator[T_ParamSpec, T_Retval]):
    def rma(self, *args, **kwargs) -> Awaitable[T_Retval]:
        return self._eval_async(args, kwargs)

    def rms(self, *args, **kwargs) -> Callable[T_ParamSpec, T_Retval]:
        return self._eval_sync(args, kwargs)


class remote_async_method(remote_method[T_ParamSpec, T_Retval]):
    def __init__(self, method: Callable[T_ParamSpec, Awaitable[T_Retval]]):
        super().__init__(method)


class remote_sync_method(remote_method[T_ParamSpec, T_Retval]):
    def __init__(self, method: Callable[T_ParamSpec, T_Retval]):
        super().__init__(method)


class remote_generator(decorator[T_ParamSpec, T_Retval]):
    async def rma(self, *args, **kwargs) -> AsyncIterator[T_Retval]:
        async for value in await self._eval_async(args, kwargs):
            yield value

    def rms(self, *args, **kwargs) -> Iterator[T_Retval]:
        return self._eval_sync(args, kwargs)


class remote_async_generator(remote_generator[T_ParamSpec, T_Retval]):
    def __init__(self, method: Callable[T_ParamSpec, AsyncIterator[T_Retval]]):
        super().__init__(method)


class remote_sync_generator(remote_generator[T_ParamSpec, T_Retval]):
    def __init__(self, method: Callable[T_ParamSpec, Iterator[T_Retval]]):
        super().__init__(method)


class remote_context_manager(decorator[T_ParamSpec, T_Retval]):
    @contextlib.asynccontextmanager
    async def rma(self, *args, **kwargs) -> AsyncIterator[T_Retval]:
        async with await self._eval_async(args, kwargs) as value:
            yield value

    @contextlib.contextmanager
    def rms(self, *args, **kwargs) -> Iterator[T_Retval]:
        value = self._eval_sync(args, kwargs)
        with self._instance.session.sync_client.portal.wrap_async_context_manager(value) as value:
            yield value


class remote_async_context_manager(remote_context_manager[T_ParamSpec, T_Retval]):
    def __init__(self, method: Callable[T_ParamSpec, T_Retval]):
        super().__init__(method)


class remote_sync_context_manager(remote_context_manager[T_ParamSpec, T_Retval]):
    def __init__(self, method: Callable[T_ParamSpec, T_Retval]):
        super().__init__(method)


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

    async def __anext__(self) -> Any:
        return await self._queue.get()


class RemoteValue:
    def __init__(self, value):
        self.value = value

    def __iter__(self):
        return self.value.__iter__()

    def __aiter__(self):
        return self.value.__aiter__()


def iterate_generator_async(value_id, pull_or_push):
    session = current_session.get()
    if session.sync_client:
        return session.sync_client._sync_generator_iter(value_id, pull_or_push)
    return session.iterate_generator_async_local(value_id, pull_or_push)


class RemoteGeneratorPush(RemoteValue):
    def __init__(self, value):
        if not inspect.isasyncgen(value):
            raise TypeError(
                f"RemoteGeneratorPush can only be used with async generators, received: {type(value)}."
            )
        super().__init__(value)

    def __reduce__(self):
        return iterate_generator_async, (current_session.get().store_value(self.value), False)


class RemoteGeneratorPull(RemoteValue):
    def __reduce__(self):
        return iterate_generator_async, (current_session.get().store_value(self.value), True)


def create_awaitable_result(value_id):
    return current_session.get().call_internal_method(Session.await_coroutine_remote, (value_id,))


class RemoteAwaitable(RemoteValue):
    def __await__(self):
        return self.value.__await__()

    def __reduce__(self):
        return create_awaitable_result, (current_session.get().store_value(self.value),)


def create_context_manager_result(value_id):
    return current_session.get().manage_async_context_local(value_id)


class RemoteAsyncContext(RemoteValue):
    def __reduce__(self):
        return create_context_manager_result, (current_session.get().store_value(self.value),)


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


def decode_iteration_result(code, result):
    if code in (CLOSE_SENTINEL, CANCEL_TASK):
        return True, None
    if code == EXCEPTION:
        if isinstance(result, RemoteException):
            traceback.print_list(result.args[1])
            raise result.args[0]
        raise result if isinstance(result, Exception) else Exception(result)
    return False, result


def forbidden_method(*args, **kwargs):
    raise Exception("This method should not be called.")


class BaseRemoteObject:
    local_value_id = None
    is_proxy = False

    @classmethod
    def create_proxy_instance(cls, object_id):
        session = current_session.get()
        if object_id not in session.remote_objects:
            obj = cls.__new__(cls)
            BaseRemoteObject.__init__(obj, session, True, object_id)
            session.remote_objects[object_id] = obj
            if not hasattr(cls, "__patched__"):
                for k, v in ((k, getattr(cls, k)) for k in dir(cls)):
                    if not isinstance(v, remote_method) and inspect.isfunction(v) and not hasattr(BaseRemoteObject, k):
                        setattr(cls, k, forbidden_method)
                cls.__patched__ = True
        return session.remote_objects[object_id]

    @classmethod
    def lookup_local_object(cls, object_id):
        return current_session.get().local_objects[object_id]

    def __init__(self, session: Session, is_proxy: bool, object_id: int):
        self.session = session
        self.is_proxy = is_proxy
        self.object_id = object_id

    def __reduce__(self):
        session = current_session.get()
        if self.is_proxy:
            return self.lookup_local_object, (self.object_id,)
        else:
            if self.local_value_id is None:
                local_value_id = next(session.local_value_id)
                self.local_value_id = local_value_id
                session.local_objects[local_value_id] = self
        return self.create_proxy_instance, (self.local_value_id,)

    async def getattr_async(self, name: str) -> Any:
        return await self.session.get_attribute_local(self.object_id, name)

    def getattr_sync(self, name: str) -> Any:
        return self.session.sync_client._wrap_awaitable(self.getattr_async)(None, name)

    def setattr_async(self, name: str, value: Any):
        return self.session.set_attribute_local(self.object_id, name, value)

    def setattr_sync(self, name: str, value: Any):
        self.session.sync_client._wrap_awaitable(self.setattr_async)(None, name, value)

    # def __setattr__(_self, _name, _value):
    #     raise AttributeError(ASYNC_SETATTR_ERROR_MESSAGE)

    # def __getattr__(self, name: str) -> Any:
    #     raise AttributeError(ASYNC_SETATTR_ERROR_MESSAGE)


class RemoteObject:
    def __init__(self, session, remote_value_id: int):
        self.session = session
        self.object_id = remote_value_id


def __setattr_forbidden__(_self, _name, _value):
    raise AttributeError(ASYNC_SETATTR_ERROR_MESSAGE)


async def wrap_sync_generator(sync_generator):
    for value in sync_generator:
        yield value


async def wrap_coroutine(coroutine):
    return OK, await coroutine


class GeneratorState:
    def __init__(self):
        self.messages_in_flight_total_size = 0
        self.messages_in_flight = {}
        self.acknowledged_message = anyio.Event()


class Session:
    def __init__(self, connection: Connection, task_group: anyio.abc.TaskGroup) -> None:
        self.task_group: anyio.abc.TaskGroup = task_group
        connection.set_loads(self.loads)
        connection.set_dumps(self.dumps)
        self.connection = connection
        # managing remote objects
        self.request_id = count()
        self.pending_results_local = {}
        self.remote_objects = {}
        self.sync_client: SyncClient = None  # noqa
        # managing local objects (ie. objects actually living in the current process)
        self.local_value_id = (
            count()
        )  # seems that 0 got sometimes turned into None when reduced in pickle
        self.local_objects = {}
        self.pending_results_local = {}
        self.tasks_cancellation_callbacks = {}
        self.generator_states: Dict[int, GeneratorState] = {}
        self.max_data_size_in_flight = MAX_DATA_SIZE_IN_FLIGHT
        self.max_data_nb_in_flight = MAX_DATA_NB_IN_FLIGHT

    async def send(self, *args):
        return await self.connection.send(args)

    def send_nowait(self, *args):
        return self.connection.send_nowait(args)

    def loads(self, value):
        return pickle.loads(value)
        return RMY_Unpickler(io.BytesIO(value), self).load()

    def dumps(self, value):
        return pickle.dumps(value)
        file = io.BytesIO()
        RMY_Pickler(self, file).dump(value)
        return file.getvalue()

    async def send_request_result(self, request_id: int, status: str, value: Any):
        time_stamp = datetime.datetime.now().timestamp()
        return time_stamp, await self.send(
            Session.set_pending_result, request_id, status, time_stamp, value
        )

    async def process_messages(
        self, task_status: anyio.abc.TaskStatus = anyio.TASK_STATUS_IGNORED
    ):
        task_status.started()
        async for method, request_id, *payload in self.connection:
            try:
                result = method(self, request_id, *payload)
                if inspect.isawaitable(result):
                    await result
            except anyio.get_cancelled_exc_class():
                raise
            except Exception:
                if method != Session.set_pending_result:
                    stack = traceback.format_exc()
                    await self.send_request_result(request_id, EXCEPTION, stack)

    async def aclose(self):
        self.task_group.cancel_scope.cancel()

    async def call_internal_method(self, method, args) -> Any:
        result = asyncio.Future()
        request_id = next(self.request_id)
        with scoped_insert(self.pending_results_local, request_id, result):
            async with self.cancel_request_on_exit(request_id, on_exception_only=False):
                self.send_nowait(method, request_id, *args)
                method, _time_stamp, value = await result
                if method in (CANCEL_TASK, OK):
                    return value
                if method == EXCEPTION:
                    if isinstance(value, RemoteException):
                        traceback.print_list(value.args[1])
                        raise value.args[0]
                    raise value if isinstance(value, Exception) else Exception(value)
                else:
                    raise Exception(f"Unexpected code {method} received.")

    @contextlib.asynccontextmanager
    async def cancel_request_on_exit(self, request_id: int, on_exception_only: bool = True):
        exception_thrown = False
        try:
            yield
        except Exception:
            exception_thrown = True
            raise
        finally:
            if exception_thrown or not on_exception_only:
                with anyio.CancelScope(shield=True):
                    self.send_nowait(Session.cancel_task_remote, request_id)
                    # await self.send(Base._cancel_task_remote, request_id) # FIXME: this should be used instead of send_nowait

    def set_pending_result(self, request_id: int, status: str, time_stamp: float, value: Any):
        if result := self.pending_results_local.get(request_id):
            result.set_result((status, time_stamp, value))
        else:
            print(f"Unexpected result for request id {request_id} received.")

    async def evaluate_method_local(self, object_id, function, args, kwargs):
        result = await self.call_internal_method(
            Session.evaluate_method_remote, (object_id, function, args, kwargs)
        )
        if inspect.iscoroutine(result):
            result = await result
        return result

    async def context_manager_async_enter_local(self, context_id):
        return await self.call_internal_method(
            Session.context_manager_async_enter_remote, (context_id,)
        )

    async def context_manager_async_exit_local(self, context_id):
        with anyio.CancelScope(shield=True):
            await self.call_internal_method(
                Session.context_manager_async_exit_remote, (context_id,)
            )

    @contextlib.asynccontextmanager
    async def manage_async_context_local(self, context_id: int):
        try:
            yield await self.context_manager_async_enter_local(context_id)
        finally:
            await self.context_manager_async_exit_local(context_id)

    async def get_attribute_local(self, object_id: int, name: str):
        return await self.call_internal_method(Session.get_attribute_remote, (object_id, name))

    async def set_attribute_local(self, object_id: int, name: str, value: Any):
        return await self.call_internal_method(
            Session.set_attribute_remote, (object_id, name, value)
        )

    async def iterate_generator_async_local(self, generator_id: int, pull_or_push: bool):
        queue = IterationBufferAsync()
        request_id = next(self.request_id)
        with scoped_insert(self.pending_results_local, request_id, queue):
            async with self.cancel_request_on_exit(request_id, on_exception_only=False):
                await self.send(
                    Session.iterate_generator_remote, request_id, generator_id, pull_or_push
                )
                async for code, time_stamp, result in queue:
                    terminated, value = decode_iteration_result(code, result)
                    if terminated:
                        break
                    yield value
                    await self.send(
                        Session.acknowledge_async_generator_data_remote,
                        generator_id,
                        time_stamp,
                    )

    @contextlib.asynccontextmanager
    async def iterate_generator_sync_local(self, generator_id: int, pull_or_push: bool):
        queue = IterationBufferSync()
        request_id = next(self.request_id)
        with scoped_insert(self.pending_results_local, request_id, queue):
            async with self.cancel_request_on_exit(request_id, on_exception_only=False):
                await self.send(
                    Session.iterate_generator_remote, request_id, generator_id, pull_or_push
                )
                yield queue

    @contextlib.asynccontextmanager
    async def create_object_local(self, object_class, args=(), kwarg={}):
        object_id = await self.call_internal_method(
            Session.create_object_remote, (object_class, args, kwarg)
        )
        try:
            yield await self.fetch_object_local(object_id)
        finally:
            with anyio.CancelScope(shield=True):
                await self.send(Session.delete_object_remote, next(self.request_id), object_id)

    async def fetch_object_local(self, object_id: int) -> Any:
        return await self.call_internal_method(Session.fetch_object_remote, (object_id,))

    def register_object(self, object: Any):
        object_id = next(self.local_value_id)
        self.local_objects[object_id] = object
        return object_id

    async def iterate_async_generator(
        self,
        request_id: int,
        iterator_id: int,
        async_iterator: AsyncIterator[Any],
        pull_or_push: bool,
    ):
        generator_state = GeneratorState()
        with scoped_insert(self.generator_states, iterator_id, generator_state):
            async with asyncstdlib.scoped_iter(async_iterator) as scoped_async_iterator:
                async for value in scoped_async_iterator:
                    time_stamp, message_size = await self.send_request_result(
                        request_id, OK, value
                    )
                    generator_state.messages_in_flight_total_size += message_size
                    generator_state.messages_in_flight[time_stamp] = message_size
                    if (
                        generator_state.messages_in_flight_total_size
                        > self.max_data_size_in_flight
                        or len(generator_state.messages_in_flight) > self.max_data_nb_in_flight
                    ):
                        if not pull_or_push:
                            raise OverflowError(
                                " ".join(
                                    [
                                        ASYNC_GENERATOR_OVERFLOWED_MESSAGE,
                                        f"Current data size in flight {generator_state.messages_in_flight_total_size}, max is {self.max_data_size_in_flight}.",
                                        f"Current number of messages in flight: {generator_state.messages_in_flight}, max is {self.max_data_nb_in_flight}.",
                                    ]
                                )
                            )
                        await generator_state.acknowledged_message.wait()
            return CLOSE_SENTINEL, None

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
                await self.send_request_result(request_id, status, result)

    def run_cancellable_task(self, request_id, coroutine_or_async_context):
        async def task():
            task_group = anyio.create_task_group()
            with scoped_insert(
                self.tasks_cancellation_callbacks, request_id, task_group.cancel_scope.cancel
            ):
                async with task_group:
                    task_group.start_soon(
                        self.run_task,
                        request_id,
                        coroutine_or_async_context,
                    )

        self.task_group.start_soon(task)

    def store_value(self, value: Any):
        value_id = next(self.local_value_id)
        self.pending_results_local[value_id] = value
        return value_id

    async def context_manager_async_enter_remote(self, request_id: int, context_id: int):
        code, result = EXCEPTION, f"Context manager {context_id} not found"
        if context_manager := self.pending_results_local.get(context_id):
            try:
                code, result = OK, await context_manager.__aenter__()
            except Exception as e:
                code, result = EXCEPTION, e
        await self.send_request_result(request_id, code, result)

    async def context_manager_async_exit_remote(self, request_id: int, context_id: int):
        code, result = EXCEPTION, f"Context manager {context_id} not found"
        if context_manager := self.pending_results_local.pop(context_id, None):
            try:
                code, result = OK, await context_manager.__aexit__(None, None, None)
            except Exception as e:
                code, result = EXCEPTION, e
        await self.send_request_result(request_id, code, result)

    def iterate_generator_remote(self, request_id: int, iterator_id: int, pull_or_push: bool):
        if not (generator := self.pending_results_local.pop(iterator_id, None)):
            return
        self.run_cancellable_task(
            request_id,
            self.iterate_async_generator(request_id, iterator_id, generator, pull_or_push),
        )

    def await_coroutine_remote(self, request_id: int, coroutine_id: int):
        if not (coroutine := self.pending_results_local.pop(coroutine_id, None)):
            return
        self.run_cancellable_task(request_id, wrap_coroutine(coroutine))

    def acknowledge_async_generator_data_remote(self, request_id: int, time_stamp: float):
        if generator_state := self.generator_states.get(request_id):
            generator_state.messages_in_flight_total_size -= (
                generator_state.messages_in_flight.pop(time_stamp)
            )
            generator_state.acknowledged_message.set()

    def delete_object_remote(self, _request_id, object_id: int):
        self.local_objects.pop(object_id, None)

    async def cancel_task_remote(self, request_id: int):
        if running_task_cancel_callback := self.tasks_cancellation_callbacks.get(request_id):
            running_task_cancel_callback()

    async def evaluate_method_remote(self, request_id, object_id, method, args, kwargs):
        if isinstance(method, str):
            result = getattr(self.local_objects[object_id], method)(*args, **kwargs)
        else:
            result = method(self.local_objects[object_id], *args, **kwargs)
        if inspect.iscoroutine(result):
            result = RemoteAwaitable(result)
        elif inspect.isasyncgen(result):
            result = RemoteGeneratorPush(result)
        elif inspect.isgenerator(result):
            result = RemoteGeneratorPull(result)
        elif all(hasattr(result, name) for name in ["__aenter__", "__aexit__"]):
            result = RemoteAsyncContext(result)
        await self.send_request_result(request_id, OK, result)

    async def create_object_remote(self, request_id, object_class, args, kwarg):
        try:
            code, message = OK, self.register_object(object_class(*args, **kwarg))
        except Exception:
            code, message = EXCEPTION, traceback.format_exc()
        await self.send_request_result(request_id, code, message)

    async def fetch_object_remote(self, request_id, object_id):
        if (maybe_object := self.local_objects.get(object_id)) is not None:
            await self.send_request_result(request_id, OK, maybe_object)
        else:
            await self.send_request_result(request_id, EXCEPTION, f"Object {object_id} not found")

    async def get_attribute_remote(self, request_id, object_id, name):
        code, value = OK, None
        try:
            value = getattr(self.local_objects[object_id], name)
        except Exception as e:
            code, value = EXCEPTION, e
        await self.send_request_result(request_id, code, value)

    async def set_attribute_remote(self, request_id, object_id, name, value):
        code, result = OK, None
        try:
            setattr(self.local_objects[object_id], name, value)
        except Exception as e:
            code, result = EXCEPTION, e
        await self.send_request_result(request_id, code, result)
