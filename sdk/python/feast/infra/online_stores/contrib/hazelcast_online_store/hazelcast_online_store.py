import logging
from datetime import datetime
from typing import Sequence, List, Optional, Tuple, Dict, Callable, Any, Literal
import pytz
from hazelcast.core import HazelcastJsonValue

from feast import RepoConfig, FeatureView, Entity
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto, Value
import hazelcast

from feast.repo_config import FeastConfigBaseModel
from pydantic import StrictStr

from feast.usage import log_exceptions_and_usage


class HazelcastOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for Hazelcast store"""

    type: Literal["hazelcast"] = "hazelcast"
    """Online store type selector"""

    cluster_name: StrictStr = "dev"
    """Name of the cluster you want to connect. The default cluster name is `dev`"""

    cluster_members: Optional[List] = ["localhost:5701"]
    """List of member addresses which is connected to your cluster"""

    discovery_token: Optional[StrictStr] = ""
    """The discovery token of your Hazelcast Viridian cluster"""

    ssl_cafile_path: Optional[StrictStr] = ""
    """Absolute path of CA certificates in PEM format."""

    ssl_certfile_path: Optional[StrictStr] = ""
    """Absolute path of the client certificate in PEM format."""

    ssl_keyfile_path: Optional[StrictStr] = ""
    """Absolute path of the private key file for the client certificate in the PEM format."""

    ssl_password: Optional[StrictStr] = ""
    """Password for decrypting the keyfile if it is encrypted."""

    key_ttl_seconds: Optional[int] = 0
    """Hazelcast key bin TTL (in seconds) for expiring entities"""


class HazelcastOnlineStore(OnlineStore):
    _client: Optional[hazelcast.HazelcastClient] = None

    def _get_client(self, config: HazelcastOnlineStoreConfig):
        logging.getLogger("hazelcast").setLevel(logging.ERROR)
        if not self._client:
            if config.discovery_token != "":
                self._client = hazelcast.HazelcastClient(
                    cluster_name=config.cluster_name,
                    statistics_enabled=True,
                    ssl_enabled=True,
                    cloud_discovery_token=config.discovery_token,
                    ssl_cafile=config.ssl_cafile_path,
                    ssl_certfile=config.ssl_certfile_path,
                    ssl_keyfile=config.ssl_keyfile_path,
                    ssl_password=config.ssl_password
                )
            elif config.ssl_cafile_path != "":
                self._client = hazelcast.HazelcastClient(
                    cluster_name=config.cluster_name,
                    statistics_enabled=True,
                    ssl_enabled=True,
                    ssl_cafile=config.ssl_cafile_path,
                    ssl_certfile=config.ssl_certfile_path,
                    ssl_keyfile=config.ssl_keyfile_path,
                    ssl_password=config.ssl_password
                )
            else:
                self._client = hazelcast.HazelcastClient(
                    statistics_enabled=True,
                    cluster_members=config.cluster_members,
                    cluster_name=config.cluster_name
                )
        return self._client

    @log_exceptions_and_usage(online_store="hazelcast")
    def online_write_batch(
            self,
            config: RepoConfig,
            table: FeatureView,
            data: List[
                Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
            ],
            progress: Optional[Callable[[int], Any]]
    ) -> None:
        online_store_config = config.online_store
        assert isinstance(online_store_config, HazelcastOnlineStoreConfig)

        client = self._get_client(online_store_config)
        fv_map = client.get_map(_table_id(config.project, table))

        for entity_key, values, event_ts, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            event_ts_utc = _to_utc_timestamp(event_ts)
            created_ts_utc = 0
            if created_ts is not None:
                created_ts_utc = _to_utc_timestamp(created_ts)
            for feature_name, value in values.items():
                feature_value = value.SerializeToString().hex()
                __key = str(entity_key_bin) + feature_name
                fv_map.put(__key, HazelcastJsonValue({"entity_key": entity_key_bin,
                                                      "feature_name": feature_name,
                                                      "feature_value": feature_value,
                                                      "event_ts": event_ts_utc,
                                                      "created_ts": created_ts_utc}),
                           config.online_store.key_ttl_seconds)
                if progress:
                    progress(1)

    def online_read(
            self,
            config: RepoConfig,
            table: FeatureView,
            entity_keys: List[EntityKeyProto],
            requested_features: Optional[List[str]] = None
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        online_store_config = config.online_store
        assert isinstance(online_store_config, HazelcastOnlineStoreConfig)

        client = self._get_client(online_store_config)

        entries: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        fv_map = client.get_map(_table_id(config.project, table))

        hz_keys = []
        entity_keys_str = {}
        for entity_key in entity_keys:
            feature_keys = []
            entity_key_str = str(serialize_entity_key(entity_key,
                                                      entity_key_serialization_version=config.entity_key_serialization_version).hex())
            if requested_features:
                for feature in requested_features:
                    feature_keys.append(entity_key_str + feature)
            else:
                for feature in table.features:
                    feature_keys.append(entity_key_str + feature.name)
            hz_keys.extend(feature_keys)
            entity_keys_str[entity_key_str] = feature_keys

        data = fv_map.get_all(hz_keys).result()

        entities = []
        for _key in hz_keys:
            data[_key] = data[_key].loads()
            entities.append(data[_key]["entity_key"])

        for entity_key in entity_keys_str:
            if entity_key in entities:
                entry = dict()
                event_ts: Optional[datetime] = None
                for _key in entity_keys_str[entity_key]:
                    row = data[_key]
                    value = ValueProto()
                    value.ParseFromString(bytes.fromhex(row["feature_value"]))
                    entry[row["feature_name"]] = value
                    event_ts = datetime.utcfromtimestamp(row["event_ts"])
                entries.append((event_ts, entry))
            else:
                entries.append((None, None))
        return entries

    def update(
            self,
            config: RepoConfig,
            tables_to_delete: Sequence[FeatureView],
            tables_to_keep: Sequence[FeatureView],
            entities_to_delete: Sequence[Entity],
            entities_to_keep: Sequence[Entity],
            partial: bool
    ):
        online_store_config = config.online_store
        assert isinstance(online_store_config, HazelcastOnlineStoreConfig)

        client = self._get_client(online_store_config)
        project = config.project

        for table in tables_to_keep:
            client.sql.execute(
                f"""CREATE OR REPLACE MAPPING {_table_id(project, table)} (
                        __key VARCHAR,
                        entity_key VARCHAR,
                        feature_name VARCHAR,
                        feature_value VARCHAR,
                        event_ts INTEGER,
                        created_ts INTEGER
                    )
                    TYPE IMap
                    OPTIONS (
                        'keyFormat' = 'varchar',
                        'valueFormat' = 'json-flat'
                    )
                """).result()

        for table in tables_to_delete:
            client.sql.execute(f"DELETE FROM {_table_id(config.project, table)}").result()
            client.sql.execute(f"DROP MAPPING IF EXISTS {_table_id(config.project, table)}").result()

    def teardown(
            self,
            config: RepoConfig,
            tables: Sequence[FeatureView],
            entities: Sequence[Entity]
    ):
        online_store_config = config.online_store
        assert isinstance(online_store_config, HazelcastOnlineStoreConfig)

        client = self._get_client(online_store_config)
        project = config.project

        for table in tables:
            client.sql.execute(f"DELETE FROM {_table_id(config.project, table)}")
            client.sql.execute(f"DROP MAPPING IF EXISTS {_table_id(project, table)}")


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


def _to_utc_timestamp(ts: datetime) -> int:
    if ts.tzinfo is None:
        return int(ts.timestamp())
    else:
        return int(ts.astimezone(pytz.utc).replace(tzinfo=None).timestamp())
