import contextlib
import inspect
from typing import Any, Iterator, Type, TypeVar

import anyio

from .client_async import connect_session
from .session import DEFAULT_SERVER_OBJECT_ID, Session, decode_iteration_result


T = TypeVar("T")


class SyncClient:
    def __init__(self, portal, session) -> None:
        self.portal = portal
        self.session: Session = session

    def _sync_generator_iter(self, generator_id: int, pull_or_push: bool):
        with self.portal.wrap_async_context_manager(
            self.session.iterate_generator_sync_local(generator_id, pull_or_push)
        ) as queue:
            for code, time_stamp, result in queue:
                terminated, value = decode_iteration_result(code, result)
                if terminated:
                    break
                yield value
                self.portal.call(
                    self.session.send,
                    Session.acknowledge_async_generator_data_remote,
                    generator_id,
                    time_stamp,
                )

    def _wrap_function(self, object_id, function):
        async def async_method(args, kwargs):
            result = await self.session.call_internal_method(
                Session.evaluate_object_method_remote, (object_id, function, args, kwargs)
            )
            if inspect.iscoroutine(result):
                result = await result
            return result

        return lambda *args, **kwargs: self.portal.call(async_method, args, kwargs)

    def _wrap_awaitable(self, method):
        def result(_self, *args, **kwargs):
            return self.portal.call(method, *args, **kwargs)

        return result

    def fetch_remote_object(self, object_class: Type[T], object_id=DEFAULT_SERVER_OBJECT_ID) -> T:
        return self.portal.call(self.session.fetch_remote_object, object_class, object_id)


@contextlib.contextmanager
def create_sync_client(host_name: str, port: int) -> Iterator[SyncClient]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(connect_session(host_name, port)) as session:
            sync_client = SyncClient(portal, session)
            session.sync_client = sync_client
            yield sync_client
