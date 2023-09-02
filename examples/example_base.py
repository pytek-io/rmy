import argparse
import asyncio
from typing import TypeVar

import rmy


T = TypeVar("T")


async def run_main_async(object_class, port: int, main_async):
    async with rmy.create_async_client("localhost", port) as client:
        await main_async(await client.fetch_remote_object(object_class))


def demo_main(demo_object, main_sync, main_async=None, interface=None):
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("--server", action="store_true", help="run as server")
    parser.add_argument("--port", default="8081", help="TCP port to use")
    parser.add_argument("--async", action="store_true", help="run async version")
    args = parser.parse_args()
    if args.server:
        rmy.run_tcp_server(args.port, demo_object)
    else:
        object_class = interface or type(demo_object)
        if args.__dict__["async"]:
            asyncio.run(run_main_async(object_class, args.port, main_async))
        else:
            with rmy.create_sync_client("localhost", args.port) as client:
                main_sync(client.fetch_remote_object(object_class))
