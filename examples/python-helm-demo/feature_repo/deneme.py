import pathlib
from pathlib import Path

from feast import FeatureStore

from feast import FeatureStore, Entity, FeatureView, Feature, FileSource, RepoConfig
from datetime import timedelta
fs = FeatureStore(repo_path=".")
driver = Entity(name="driver_id", description="driver id")
driver_hourly_stats = FileSource(
     path="/Users/hazelcast/Desktop/feast/examples/python-helm-demo/feature_repo/data/registry.db",
     timestamp_field="event_timestamp",
     created_timestamp_column="created",
)
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(seconds=86400 * 1),
    source=driver_hourly_stats,
)
fs = FeatureStore(repo_path=".")
fs.apply([driver_hourly_stats_view, driver])