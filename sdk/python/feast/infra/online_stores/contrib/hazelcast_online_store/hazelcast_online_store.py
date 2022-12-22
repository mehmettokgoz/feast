import logging
from datetime import datetime
from typing import Sequence, List, Optional, Tuple, Dict, Callable, Any, Literal
import pytz

from feast import RepoConfig, FeatureView, Entity
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from hazelcast import HazelcastClient

from feast.repo_config import FeastConfigBaseModel

from pydantic import StrictStr
from pydantic.types import StrictBool

from feast.usage import log_exceptions_and_usage


class HazelcastOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for Hazelcast store"""

    type: Literal["hazelcast"] = "hazelcast"
    """Online store type selector"""

    cluster_name: StrictStr = "dev"
    """Name of the cluster you want to connect. The default cluster name is `dev`"""

    cluster_members: Optional[List] = ["localhost:5701"]
    """List of member addresses which is connected to your cluster"""

    cloud: Optional[StrictBool] = False
    """Enable if you want to connect a Hazelcast Viridian cluster"""

    discovery_token: Optional[StrictStr]
    """The discovery token of your Hazelcast Viridian cluster"""

    ssl_enabled: Optional[StrictBool] = False
    """Enable if you want to use TLS while connecting to your cluster"""

    ssl_cafile_path: Optional[StrictStr]
    """Absolute path of concatenated CA certificates used to validate server's certificates in PEM format."""

    ssl_certfile_path: Optional[StrictStr]
    """Absolute path of the client certificate in PEM format."""

    ssl_keyfile_path: Optional[StrictStr]
    """Absolute path of the private key file for the client certificate in the PEM format."""

    ssl_password: Optional[StrictStr]
    """Password for decrypting the keyfile if it is encrypted."""


class HazelcastOnlineStore(OnlineStore):
    _client: Optional[HazelcastClient] = None

    def _get_client(self, config: HazelcastOnlineStoreConfig):
        logger = logging.getLogger("hazelcast")
        logger.setLevel(logging.ERROR)
        if not self._client:
            if config.cloud:
                self._client = HazelcastClient(
                    cluster_name=config.cluster_name,
                    statistics_enabled=True,
                    ssl_enabled=True,
                    cloud_discovery_token=config.discovery_token,
                    ssl_cafile=config.ssl_cafile_path,
                    ssl_certfile=config.ssl_certfile_path,
                    ssl_keyfile=config.ssl_keyfile_path,
                    ssl_password=config.ssl_password
                )
            elif config.ssl_enabled:
                self._client = HazelcastClient(
                    cluster_name=config.cluster_name,
                    statistics_enabled=True,
                    ssl_enabled=True,
                    ssl_cafile=config.ssl_cafile_path,
                    ssl_certfile=config.ssl_certfile_path,
                    ssl_keyfile=config.ssl_keyfile_path,
                    ssl_password=config.ssl_password
                )
            else:
                self._client = HazelcastClient(
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
        project = config.project

        query = f"""
            SINK INTO {_feature_map_name(project, table)}
            (entity_key, feature_name, feature_value, event_ts, created_ts)
            values (?, ?, ?, ?, ?);
        """
        for entity_key, features, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=2,
            ).hex()
            timestamp = _convert_to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = _convert_to_naive_utc(created_ts)
            for feature_name, feature_value in features.items():
                client.sql.execute(query,
                                   entity_key_bin,
                                   feature_name,
                                   feature_value.SerializeToString().hex(),
                                   timestamp,
                                   created_ts)
            if progress:
                progress(1)

    @log_exceptions_and_usage(online_store="hazelcast")
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

        result: List[Tuple[Optional[datetime], Optional[Dict[str, Any]]]] = []
        requested_features_str = ", ".join(f"'{f}'" for f in requested_features)
        query = f"""
                SELECT feature_name, feature_value, event_ts 
                FROM {_feature_map_name(config.project, table)} 
                WHERE entity_key=? AND feature_name IN ({requested_features_str})"""
        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=2,
            ).hex()
            records = client.sql.execute(query, entity_key_bin).result()
            entry = {}
            entry_ts: Optional[datetime] = None
            if records:
                for row in records:
                    value = ValueProto()
                    value.ParseFromString(bytes.fromhex(row["feature_value"]))
                    entry[row["feature_name"]] = value
                    entry_ts = row["event_ts"]
            if not entry:
                result.append((None, None))
            else:
                result.append((entry_ts, entry))
        return result

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
                f"""CREATE MAPPING IF NOT EXISTS {_feature_map_name(project, table)} (
                        entity_key VARCHAR EXTERNAL NAME "__key.entity_key",
                        feature_name VARCHAR EXTERNAL NAME "__key.feature_name",
                        feature_value OBJECT,
                        event_ts TIMESTAMP WITH TIME ZONE,
                        created_ts TIMESTAMP WITH TIME ZONE
                    )
                    TYPE IMap
                    OPTIONS (
                        'keyFormat' = 'json-flat',
                        'valueFormat' = 'json-flat'
                    )
                """).result()
        for table in tables_to_delete:
            client.sql.execute(f"DELETE FROM {_feature_map_name(config.project, table)}").result()
            client.sql.execute(f"DROP MAPPING IF EXISTS {_feature_map_name(config.project, table)}").result()

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
            client.sql.execute(f"DELETE FROM {_feature_map_name(config.project, table)}")
            client.sql.execute(f"DROP MAPPING IF EXISTS {_feature_map_name(project, table)}")


def _feature_map_name(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


def _convert_to_naive_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)
