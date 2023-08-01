
import rmy
from greeting_server import Demo
import time

if __name__ == "__main__":
    with rmy.create_sync_client("localhost", 8080) as client:
        proxy: Demo = client.fetch_remote_object()
        for message in proxy.count.rms():
            time.sleep(1)
        while True:
            print('Enter your name:')
            name = input()
            print(proxy.greet(name).rms())

    
