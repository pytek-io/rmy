from __future__ import annotations

import asyncio
import contextlib
import contextvars
import datetime
import inspect
import sys
import threading
import traceback
from collections import deque
from itertools import count
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    ContextManager,
    Dict,
    Generic,
    Iterator,
    TypeVar,
)
import weakref
from functools import partial

import anyio
import anyio.abc
import asyncstdlib

from .abc import Connection
from .common import RemoteException, scoped_insert

if TYPE_CHECKING:
    from .client_sync import SyncClient

current_session: contextvars.ContextVar[Session] = contextvars.ContextVar("current_session")

OK = "OK"
OVERFLOWERROR = "OverflowError"
CLOSE_SENTINEL = "Close sentinel"
CANCEL_TASK = "Cancel task"
EXCEPTION = "Exception"

SERVER_OBJECT_ID = 0
MAX_DATA_SIZE_IN_FLIGHT = 1_000
MAX_DATA_NB_IN_FLIGHT = 10


ASYNC_GENERATOR_OVERFLOWED_MESSAGE = "Generator iteration overflowed."

if sys.version_info >= (3, 10):
    from typing import Concatenate, ParamSpec
else:
    from typing_extensions import Concatenate, ParamSpec

T_Retval = TypeVar("T_Retval")
T_ParamSpec = ParamSpec("T_ParamSpec")
T = TypeVar("T")
M = TypeVar("M")


class Trampoline(Generic[T]):
    def __init__(self, wrapper_class: Callable[[M, BaseRemoteObject], T], method: M):
        self.wrapper_class = wrapper_class
        self.method = method

    def __get__(self, instance, _class) -> T:
        return self.wrapper_class(self.method, instance)


class RemoteWrapper(Generic[T_ParamSpec, T_Retval]):
    def __init__(self, method, instance: BaseRemoteObject):
        self._method = method
        self._instance = instance

    def __call__(self, *args, **kwargs):
        assert not self._instance.is_proxy, "Cannot call remote method directly on proxy object."
        return self._method(self._instance, *args, **kwargs)

    def _eval_async(self, args, kwargs):
        if self._instance.session.sync_client:
            raise Exception("Cannot call async method on sync proxy object.")
        return self._instance.session.evaluate_method_local(
            self._instance.object_id, self._method.__name__, args, kwargs
        )

    def _eval_sync(self, args, kwargs):
        assert self._instance.is_proxy
        if not self._instance.session.sync_client:
            raise Exception("Cannot call sync method on async proxy object.")
        return self._instance.session.sync_client._wrap_function(
            self._instance.object_id, self._method.__name__
        )(*args, **kwargs)


class RemoteMethodWrapper(RemoteWrapper[T_ParamSpec, T_Retval]):
    async def wait(self, *args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> T_Retval:
        return await self._eval_async(args, kwargs)

    def eval(
        self, *args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs
    ) -> Callable[T_ParamSpec, T_Retval]:
        return self._eval_sync(args, kwargs)


def remote_async_method(
    method: Callable[Concatenate[Any, T_ParamSpec], Awaitable[T_Retval]]
) -> Trampoline[RemoteMethodWrapper[T_ParamSpec, T_Retval]]:
    return Trampoline(RemoteMethodWrapper, method)


def remote_sync_method(
    method: Callable[Concatenate[Any, T_ParamSpec], T_Retval]
) -> Trampoline[RemoteMethodWrapper[T_ParamSpec, T_Retval]]:
    return Trampoline(RemoteMethodWrapper, method)


class RemoteGenerator(RemoteWrapper[T_ParamSpec, T_Retval]):
    async def wait(
        self, *args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs
    ) -> AsyncIterator[T_Retval]:
        async for value in await self._eval_async(args, kwargs):
            yield value

    def eval(self, *args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> Iterator[T_Retval]:
        return self._eval_sync(args, kwargs)


def remote_async_generator(
    method: Callable[Concatenate[Any, T_ParamSpec], AsyncIterator[T_Retval]]
) -> Trampoline[RemoteGenerator[T_ParamSpec, T_Retval]]:
    return Trampoline(RemoteGenerator, method)


def remote_sync_generator(
    method: Callable[Concatenate[Any, T_ParamSpec], Iterator[T_Retval]]
) -> Trampoline[RemoteGenerator[T_ParamSpec, T_Retval]]:
    return Trampoline(RemoteGenerator, method)


class RemoteContextManager(RemoteWrapper[T_ParamSpec, T_Retval]):
    @contextlib.asynccontextmanager
    async def wait(
        self, *args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs
    ) -> AsyncIterator[T_Retval]:
        async with await self._eval_async(args, kwargs) as value:
            yield value

    @contextlib.contextmanager
    def eval(self, *args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> Iterator[T_Retval]:
        value = self._eval_sync(args, kwargs)
        with self._instance.session.sync_client.portal.wrap_async_context_manager(value) as value:
            yield value


def remote_async_context_manager(
    method: Callable[Concatenate[Any, T_ParamSpec], AsyncContextManager[T_Retval]]
) -> Trampoline[RemoteContextManager[T_ParamSpec, T_Retval]]:
    return Trampoline(RemoteContextManager, method)


def remote_sync_context_manager(
    method: Callable[Concatenate[Any, T_ParamSpec], ContextManager[T_Retval]]
) -> Trampoline[RemoteContextManager[T_ParamSpec, T_Retval]]:
    return Trampoline(RemoteContextManager, method)


class SyncQueue:
    def __init__(self):
        self._queue = deque()
        self._lock = threading.Lock()
        self._new_value = threading.Event()

    def put_front(self, item):
        with self._lock:
            self._queue.append(item)
            self._new_value.set()

    def put(self, item):
        with self._lock:
            self._queue.appendleft(item)
            self._new_value.set()

    def get(self):
        while True:
            with self._lock:
                try:
                    return self._queue.pop()
                except IndexError:
                    pass
                self._new_value.clear()
            self._new_value.wait()


class IterationBuffer:
    def __init__(self, queue: SyncQueue | AsyncQueue) -> None:
        self._queue = queue

    def set_result(self, value: Any):
        code, *args = value
        if code == OVERFLOWERROR:
            self._queue.put_front(value)
        else:
            self._queue.put(value)

    def __iter__(self):
        return self

    def __next__(self):
        return self._queue.get()

    def __aiter__(self):
        return self

    async def __anext__(self) -> Any:
        return await self._queue.get()


class AsyncQueue:
    def __init__(self) -> None:
        self._queue = deque()
        self._new_value = asyncio.Event()

    def put_front(self, item):
        self._queue.append(item)
        self._new_value.set()

    def put(self, item):
        self._queue.appendleft(item)
        self._new_value.set()

    async def get(self):
        while True:
            try:
                return self._queue.pop()
            except IndexError:
                pass
            self._new_value.clear()
            await self._new_value.wait()


class RemoteValue:
    def __init__(self, value):
        self.value = value

    def inflate(self, value_id: int):
        raise NotImplementedError()

    def __reduce__(self):
        return self.inflate, (current_session.get().store_value(self.value),)


class BaseRemoteGenerator(RemoteValue):
    pull_or_push = True

    def __iter__(self):
        return self.value.__iter__()

    def __aiter__(self):
        return self.value.__aiter__()

    @classmethod
    def inflate(cls, value_id):
        session = current_session.get()
        if session.sync_client:
            result = session.sync_client._sync_generator_iter(
                value_id, cls.pull_or_push
            )
        else:
            result = session.iterate_generator_async_local(value_id, cls.pull_or_push)
        return result


class RemoteGeneratorPull(BaseRemoteGenerator):
    pass


class RemoteGeneratorPush(BaseRemoteGenerator):
    pull_or_push = False

    def __init__(self, value):
        if not inspect.isasyncgen(value):
            raise TypeError(
                f"{self.__class__.__name__} can only be used with async generators, received: {type(value)}."
            )
        super().__init__(value)


class RemoteAwaitable(RemoteValue):
    def __await__(self):
        return self.value.__await__()

    @classmethod
    def inflate(cls, value_id):
        return current_session.get().call_internal_method(
            Session.await_coroutine_remote, (value_id,)
        )


class RemoteAsyncContext(RemoteValue):
    @classmethod
    def inflate(cls, value_id):
        return current_session.get().manage_async_context_local(value_id)


def decode_iteration_result(code, result):
    if code in (CLOSE_SENTINEL, CANCEL_TASK):
        return True, None
    if code == EXCEPTION:
        if isinstance(result, RemoteException):
            traceback.print_list(result.args[1])
            raise result.args[0]
        raise result if isinstance(result, Exception) else Exception(result)
    if code == OVERFLOWERROR:
        raise OverflowError(result)
    return False, result


def forbidden_method(*args, **kwargs):
    raise Exception("This method should not be called.")


class BaseRemoteObject:
    local_value_id = None
    is_proxy = False

    def __init__(self, session=None, is_proxy=False, object_id=0):
        """Will be invoked when needed, no need to call it explicitely."""
        self.session: Session = session  # type: ignore
        self.is_proxy: bool = is_proxy
        self.object_id: int = object_id

    @classmethod
    def create_proxy_instance(cls, object_id):
        if not hasattr(cls, "__patched__"):
            for k, v in ((k, getattr(cls, k)) for k in dir(cls)):
                if (
                    inspect.isfunction(v)
                    and not isinstance(v, RemoteMethodWrapper)
                    and not hasattr(BaseRemoteObject, k)
                ):
                    setattr(cls, k, forbidden_method)
            cls.__patched__ = True
        session = current_session.get()
        if object_id not in session.remote_objects:
            obj = cls.__new__(cls)
            BaseRemoteObject.__init__(obj, session, True, object_id)
            session.remote_objects[object_id] = obj
        return session.remote_objects[object_id]

    @classmethod
    def lookup_local_object(cls, object_id):
        return current_session.get().local_objects[object_id]

    def __reduce__(self):
        session = current_session.get()
        if self.is_proxy:
            method, id = self.lookup_local_object, self.object_id
        else:
            if self.local_value_id is None:
                session.register_object(self)
            method, id = self.create_proxy_instance, self.local_value_id
        return method, (id,)


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
        self.connection = connection
        # managing remote objects
        self.request_id = count()
        self.remote_pending_results = {}
        self.remote_objects = {}
        self.sync_client: SyncClient = None  # type: ignore
        # managing local objects (ie. objects actually living in the current process)
        self.remote_value_id = count()
        self.local_objects = {}
        self.local_pending_results = {}
        self.local_tasks_cancellation_callbacks = {}
        self.generator_states: Dict[int, GeneratorState] = {}
        self.max_data_size_in_flight = MAX_DATA_SIZE_IN_FLIGHT
        self.max_data_nb_in_flight = MAX_DATA_NB_IN_FLIGHT

    async def send(self, *args):
        current_session.set(self)
        return await self.connection.send(args)

    def send_nowait(self, *args):
        current_session.set(self)
        return self.connection.send_nowait(args)

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
            except Exception as e:
                if method != Session.set_pending_result:
                    stack = traceback.format_exc()
                    await self.send_request_result(request_id, EXCEPTION, stack)
                else:
                    traceback.print_exc()

    async def aclose(self):
        self.task_group.cancel_scope.cancel()

    async def call_internal_method(self, method, args) -> Any:
        result = asyncio.Future()
        async with self.manage_pending_request(result) as request_id:
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

    def on_result_drop(self, request_id: int, weakref_):
        if self.local_pending_results.pop(request_id, None):
            self.send_nowait(Session.cancel_task_remote, request_id)

    @contextlib.asynccontextmanager
    async def manage_pending_request(self, result) -> AsyncIterator[int]:
        request_id = next(self.request_id)
        with scoped_insert(
            self.local_pending_results,
            request_id,
            weakref.ref(result, partial(self.on_result_drop, request_id))
        ):
            try:
                yield request_id
            finally:
                with anyio.CancelScope(shield=True):
                    await self.send(Session.cancel_task_remote, request_id)

    def set_pending_result(self, request_id: int, status: str, time_stamp: float, value: Any):
        if result := self.local_pending_results.get(request_id):
            if result := result():
                result.set_result((status, time_stamp, value))
        elif not status == CANCEL_TASK:
            print(f"Unexpected result for request id {request_id} received {status} {value}.")

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

    async def iterate_generator_async_local(
        self, generator_id: int, pull_or_push: bool
    ):
        queue = IterationBuffer(AsyncQueue())
        async with self.manage_pending_request(queue) as request_id:
            await self.send(
                Session.remote_iterate_generator, request_id, generator_id, pull_or_push
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
    async def iterate_generator_sync_local(
        self, generator_id: int, pull_or_push: bool
    ):
        queue = IterationBuffer(SyncQueue())
        async with self.manage_pending_request(queue) as request_id:
            await self.send(
                Session.remote_iterate_generator, request_id, generator_id, pull_or_push
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
        object_id = next(self.remote_value_id)
        if object.local_value_id is None:
            object.local_value_id = object_id
        else:
            # the server object is already registered
            assert object.local_value_id == object_id
        self.local_objects[object_id] = object
        return object_id

    async def _remote_iterate_generator(
        self,
        request_id: int,
        iterator_id: int,
        iterator: AsyncIterator[Any] | Iterator[Any],
        pull_or_push: bool,
    ):
        generator_state = GeneratorState()
        with scoped_insert(self.generator_states, iterator_id, generator_state):
            async with asyncstdlib.scoped_iter(iterator) as scoped_async_iterator:
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
                        if pull_or_push:
                            await generator_state.acknowledged_message.wait()
                        else:
                            message = " ".join(
                                [
                                    ASYNC_GENERATOR_OVERFLOWED_MESSAGE,
                                    f"Current data size in flight {generator_state.messages_in_flight_total_size}, max is {self.max_data_size_in_flight}.",  # noqa
                                    f"Current number of messages in flight: {len(generator_state.messages_in_flight)}, max is {self.max_data_nb_in_flight}.",  # noqa
                                ]
                            )
                            await self.send_request_result(request_id, OVERFLOWERROR, message)
                            break
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
                self.local_tasks_cancellation_callbacks, request_id, task_group.cancel_scope.cancel
            ):
                async with task_group:
                    task_group.start_soon(
                        self.run_task,
                        request_id,
                        coroutine_or_async_context,
                    )

        self.task_group.start_soon(task)

    def store_value(self, value: Any):
        value_id = next(self.remote_value_id)
        self.remote_pending_results[value_id] = value
        return value_id

    async def context_manager_async_enter_remote(self, request_id: int, context_id: int):
        code, result = EXCEPTION, f"Context manager {context_id} not found"
        if context_manager := self.remote_pending_results.get(context_id):
            try:
                code, result = OK, await context_manager.__aenter__()
            except Exception as e:
                code, result = EXCEPTION, e
        await self.send_request_result(request_id, code, result)

    async def context_manager_async_exit_remote(self, request_id: int, context_id: int):
        code, result = EXCEPTION, f"Context manager {context_id} not found"
        if context_manager := self.remote_pending_results.pop(context_id, None):
            try:
                code, result = OK, await context_manager.__aexit__(None, None, None)
            except Exception as e:
                code, result = EXCEPTION, e
        await self.send_request_result(request_id, code, result)

    def remote_iterate_generator(self, request_id: int, iterator_id: int, pull_or_push: bool):
        if generator := self.remote_pending_results.pop(iterator_id, None):
            self.run_cancellable_task(
                request_id,
                self._remote_iterate_generator(request_id, iterator_id, generator, pull_or_push),
            )

    def await_coroutine_remote(self, request_id: int, coroutine_id: int):
        if coroutine := self.remote_pending_results.pop(coroutine_id, None):
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
        if running_task_cancel_callback := self.local_tasks_cancellation_callbacks.get(request_id):
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
