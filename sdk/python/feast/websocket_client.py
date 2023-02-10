import websocket
from pydantic import BaseModel
from feast.data_source import PushMode
from feast.errors import PushSourceNotFoundException
import feast


class WebsocketClient(BaseModel):
    def __init__(self, sfv, to):
        self.sfv = sfv
        self.to = to
        super().__init__()

    def push_sfv(self, app, message):
        if self.to == PushMode.ONLINE or to == PushMode.ONLINE_AND_OFFLINE:
            self.fs.write_to_online_store(self.sfv.name, rows)
        if to == PushMode.OFFLINE or to == PushMode.ONLINE_AND_OFFLINE:
            self.fs.write_to_offline_store(self.sfv.name, rows)
        print(message)


def start_websocket_client(store: "feast.FeatureStore", host: str, port: int):
    print("Now websocket client is starting to listen!")
    client = WebsocketClient(store.get_stream_feature_view(""), PushModel.ONLINE_AND_OFFLINE)
    app = websocket.WebSocketApp(f"ws://{host}:{port}/push", on_message=client.push_sfv)
    app.run_forever()
