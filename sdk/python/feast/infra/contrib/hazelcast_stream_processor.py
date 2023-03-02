from threading import Thread
from time import sleep
from types import MethodType
from typing import Optional, List

import hazelcast
import pandas as pd
from feast import FeatureStore, StreamFeatureView
from feast.data_source import PushMode
from feast.infra.contrib.stream_processor import StreamProcessor, ProcessorConfig


class HazelcastStreamProcessorConfig(ProcessorConfig):
    client: hazelcast.HazelcastClient
    map_name: str
    processing_time: str
    query_timeout: int


class HazelcastStreamProcessor(StreamProcessor):
    client: hazelcast.HazelcastClient
    format: str
    preprocess_fn: Optional[MethodType]
    join_keys: List[str]

    def __init__(
            self,
            *,
            fs: FeatureStore,
            sfv: StreamFeatureView,
            config: ProcessorConfig,
    ):
        self.map_name = config.map_name
        self.format = "json"
        self.client = config.client
        self.processing_time = 15
        self.query_timeout = 15
        self.join_keys = [fs.get_entity(entity).join_key for entity in sfv.entities]
        super().__init__(fs=fs, sfv=sfv, data_source=sfv.stream_source)

    def ingest_stream_feature_view(self, to: PushMode = PushMode.ONLINE) -> None:
        mp = self.client.get_map(self.map_name)
        while True:
            query_result = self.client.sql.execute(f"SELECT * FROM {self.map_name}").result()
            rows_dict = []
            for i in query_result:
                column = i.metadata.columns
                di = dict()
                for j in column:
                    di[j.name] = i[j.name]
                rows_dict.append(di)
            rows = pd.DataFrame(rows_dict)
            rows = (
                rows.sort_values(
                    by=self.join_keys + [self.sfv.timestamp_field], ascending=True
                )
                .groupby(self.join_keys)
                .nth(0)
            )
            rows["created"] = pd.to_datetime("now", utc=True)
            rows = rows.reset_index()
            if rows.size > 0:
                print(rows)
                if to == PushMode.ONLINE or to == PushMode.ONLINE_AND_OFFLINE:
                    self.fs.write_to_online_store(self.sfv.name, rows)
                if to == PushMode.OFFLINE or to == PushMode.ONLINE_AND_OFFLINE:
                    self.fs.write_to_offline_store(self.sfv.name, rows)
            sleep(self.query_timeout)
        return online_store_query

