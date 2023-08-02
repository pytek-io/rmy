
import rmy
from greeting_server import Demo

if __name__ == "__main__":
    with rmy.create_sync_client("localhost", 8080) as client:
        proxy: Demo = client.fetch_remote_object()
        # while True:
        print('Enter your name:')
        # name = input()
        name = "world"
        print(proxy.greet.rms(name))
        # for message in proxy.count.rms():
        #     time.sleep(1)


    
