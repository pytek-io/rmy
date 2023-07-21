from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import Any, AsyncIterator, Callable, Tuple


class Connection(AsyncIterator[Any], metaclass=ABCMeta):
    @abstractmethod
    async def send(self, message: Tuple[Any, ...]) -> int:
        ...

    @abstractmethod
    def send_nowait(self, message: Tuple[Any, ...]) -> int:
        ...

    @abstractmethod
    async def drain(self):
        ...

    @abstractmethod
    def close(self):
        ...

    @abstractmethod
    def set_dumps(self, dumps: Callable[[Any], bytes]):
        ...

    @abstractmethod
    def set_loads(self, loads: Callable[[bytes], Any]):
        ...

    def __aiter__(self):
        return self

    async def __anext__(self) -> Tuple[int, Any]:
        ...

class AsyncSink:
    @abstractmethod
    def set_result(self, value: Any, message_size: int):
        ...
