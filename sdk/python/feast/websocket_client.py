import json
from datetime import datetime

import pandas as pd
import websocket
from feast.data_source import PushMode

import feast


class WebsocketClient:
    def __init__(self, store, sfv, to):
        self.store = store
        self.sfv = sfv
        self.to = to

    def push_sfv(self, app, message):
        message = json.loads(message)
        message["created"] = datetime.strptime(message["created"], '%Y-%m-%d %H:%M:%S')
        message["event_timestamp"] = datetime.strptime(message["event_timestamp"], '%Y-%m-%d %H:%M:%S')
        rows = pd.DataFrame([message])

        print(rows)
        if self.to == PushMode.ONLINE or self.to == PushMode.ONLINE_AND_OFFLINE:
            self.store.write_to_online_store(self.sfv.name, rows)
        if self.to == PushMode.OFFLINE or self.to == PushMode.ONLINE_AND_OFFLINE:
            self.store.write_to_offline_store(self.sfv.name, rows)


def start_websocket_client(store: "feast.FeatureStore", host: str, port: int, to: PushMode):
    client = WebsocketClient(store, store.get_stream_feature_view("stream_driver_hourly_stats"), to)
    app = websocket.WebSocketApp(f"ws://{host}:{port}/push", on_message=client.push_sfv)
    app.run_forever()
