import contextlib
import inspect
from typing import Iterator, Type, TypeVar

import anyio

from .client_async import connect_session
from .session import DEFAULT_SERVER_OBJECT_ID, Session, decode_iteration_result


T = TypeVar("T")


class SyncClient:
    def __init__(self, session: Session) -> None:
        self.session = session

    def fetch_remote_object(self, object_class: Type[T], object_id=DEFAULT_SERVER_OBJECT_ID) -> T:
        return self.session.portal.call(self.session.fetch_remote_object, object_class, object_id)


@contextlib.contextmanager
def create_sync_client(host_name: str, port: int) -> Iterator[SyncClient]:
    with anyio.start_blocking_portal("asyncio") as portal:
        with portal.wrap_async_context_manager(connect_session(host_name, port, portal)) as session:
            yield SyncClient(session)
