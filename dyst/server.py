import inspect
import logging
import os
import traceback
from collections import defaultdict
from datetime import datetime
from typing import ByteString, Dict, Optional, Set

import anyio

from dyst import Connection


SUBSCRIPTION_BUFFER_SIZE = 100
OVERRIDE_ERROR_MESSAGE = "Trying to override an existing event without override set to True."


class UserException(Exception):
    """Use this to signal expected errors to users."""

    pass


class ClientSessionBase:
    """Implements non functional specific details."""

    def __init__(self, server, task_group, name, connection: Connection) -> None:
        self.task_group = task_group
        self.name = name
        self.connection = connection
        self.running_tasks = {}
        self.server: Server = server

    def __str__(self) -> str:
        return self.name

    async def send(self, *args):
        await self.connection.send(args)

    async def evaluate(self, request_id, coroutine):
        send_termination = True
        try:
            success, result = True, await coroutine
        except anyio.get_cancelled_exc_class():
            send_termination = False
            raise
        except UserException as e:
            success, result = False, e.args[0]
        except:
            success, result = False, traceback.format_exc()
        if send_termination:
            await self.send(request_id, success, result)

    async def evaluate_stream(self, request_id, stream):
        success, result = True, None
        try:
            async for result in stream:
                await self.send(request_id, True, result)
        except anyio.get_cancelled_exc_class():
            raise
        except Exception:
            success, result = False, traceback.format_exc()
        finally:
            await self.send(request_id, success, result)

    async def cancellable_task_runner(self, request_id, command, details):
        try:
            async with anyio.create_task_group() as self.running_tasks[request_id]:
                coroutine_or_async_context = getattr(self, command)(request_id, *details)
                self.running_tasks[request_id].start_soon(
                    self.evaluate
                    if inspect.isawaitable(coroutine_or_async_context)
                    else self.evaluate_stream,
                    request_id,
                    coroutine_or_async_context,
                )
        finally:
            self.running_tasks.pop(request_id, None)

    async def manage_session(self):
        async for request_id, command, details in self.connection:
            if command is None:
                task_group = self.running_tasks.get(request_id)
                logging.info(f"{self.name} cancelling subscription {request_id}")
                if task_group:
                    task_group.cancel_scope.cancel()
            else:
                self.task_group.start_soon(
                    self.cancellable_task_runner, request_id, command, details
                )

    async def run(self):
        self.task_group.start_soon(self.manage_session)
        await self.connection.wait_closed()
        await self.task_group.cancel_scope.cancel()
        logging.info(f"{self} disconnected")


class ClientSession(ClientSessionBase):
    async def write_event(
        self,
        request_id: int,
        topic: str,
        event: ByteString,
        time_stamp: Optional[datetime],
        override: bool,
    ):
        if topic.startswith("/"):
            topic = topic[1:]
        folder_path = os.path.join(self.server.event_folder, topic)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        time_stamp = time_stamp or datetime.now()
        file_path = os.path.join(folder_path, str(time_stamp.timestamp()))
        if os.path.exists(file_path) and not override:
            raise UserException(OVERRIDE_ERROR_MESSAGE)
        async with await anyio.open_file(file_path, "wb") as file:
            await file.write(event)
        logging.info(f"Saved event from {self} under: {file_path}")
        for request_id, subscription in self.server.subscriptions[topic]:
            try:
                subscription.send_nowait(time_stamp)
            except anyio.WouldBlock:
                self.running_tasks[request_id].cancel_scope.cancel()
        return time_stamp

    async def read_event(self, _request_id: int, topic: str, time_stamp: datetime):
        file_path = os.path.join(self.server.event_folder, topic, str(time_stamp.timestamp()))
        async with await anyio.open_file(file_path, "rb") as file:
            return await file.read()

    async def read_events(
        self,
        request_id: int,
        topic: str,
        start: Optional[datetime],
        end: Optional[datetime],
        time_stamps_only: bool,
    ):
        sink, stream = anyio.create_memory_object_stream(SUBSCRIPTION_BUFFER_SIZE)
        subscriptions = self.server.subscriptions[topic]
        subscription = (request_id, sink)
        try:
            subscriptions.add(subscription)
            folder_path = os.path.join(self.server.event_folder, topic)
            if os.path.exists(folder_path):
                for time_stamp in map(
                    lambda s: datetime.fromtimestamp(float(s)), os.listdir(folder_path)
                ):
                    if start is not None and time_stamp < start:
                        continue
                    if end is not None and time_stamp > end:
                        continue
                    yield (
                        time_stamp
                        if time_stamps_only
                        else (
                            time_stamp,
                            await self.read_event(request_id, topic, time_stamp),
                        )
                    )
            while end is None or datetime.now() < end:
                time_stamp = await stream.receive()
                yield (
                    time_stamp
                    if time_stamps_only
                    else (
                        time_stamp,
                        await self.read_event(request_id, topic, time_stamp),
                    )
                )
        finally:
            if subscription in subscriptions:
                subscriptions.remove(subscription)


class ServerBase:
    def __init__(self, task_group) -> None:
        self.task_group = task_group
        self.subscriptions: Dict[str, Set] = defaultdict(set)

    async def manage_client_session(self, connection: Connection):
        (name,) = await connection.recv()
        logging.info(f"{name} connected")
        async with anyio.create_task_group() as task_group:
            await self.client_session_type(self, task_group, name, connection).run()

    async def manage_client_session_raw(self, raw_websocket):
        await self.manage_client_session(Connection(raw_websocket))


class Server(ServerBase):
    client_session_type = ClientSession

    def __init__(self, event_folder, task_group) -> None:
        super().__init__(task_group)
        self.event_folder = event_folder
