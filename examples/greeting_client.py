import time

from greeting_server import Demo

import rmy


if __name__ == "__main__":
    with rmy.create_sync_client("localhost", 8080) as client:
        proxy: Demo = client.fetch_remote_object()
        while True:
            print("Enter your name:")
            # name = input()
            name = "input()"
            print(proxy.greet.wait(name))
            for message in proxy.count.eval():
                print(message)
                time.sleep(1)
            break
