import contextlib
import signal
import sys
import traceback
from typing import Callable, Coroutine, Dict, Iterator, Tuple, TypeVar

import anyio
import anyio.abc


if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

T_Retval = TypeVar("T_Retval")
T_ParamSpec = ParamSpec("T_ParamSpec")
T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


@contextlib.contextmanager
def print_error_stack():
    try:
        yield
    except anyio.get_cancelled_exc_class():
        raise
    except Exception:
        traceback.print_exc()
        raise


@contextlib.contextmanager
def scoped_dict_insert(register: Dict[K, V], key: K, value: V) -> Iterator[Tuple[K, V]]:
    register[key] = value
    try:
        yield key, value
    finally:
        register.pop(key, None)


@contextlib.contextmanager
def scoped_iter(closeable):
    try:
        yield closeable
    finally:
        closeable.close()


@contextlib.asynccontextmanager
async def cancel_task_on_exit(async_method: Callable[[], Coroutine]):
    async with anyio.create_task_group() as task_group:
        try:
            task_group.start_soon(async_method, name="cancel_task_on_exit")
            yield
        finally:
            task_group.cancel_scope.cancel()


async def cancel_task_group_on_signal(task_group: anyio.abc.TaskGroup):
    with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async for signum in signals:
            if signum == signal.SIGINT:
                print("Ctrl+C pressed!")
            else:
                print(f"Received signal {signum}, terminating.")

            task_group.cancel_scope.cancel()
            return


class RemoteException(Exception):
    """Use this to signal expected errors to users."""

    pass
