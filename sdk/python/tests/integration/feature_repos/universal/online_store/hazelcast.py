from typing import Dict

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import OnlineStoreCreator


class HazelcastOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = (
            DockerContainer(
                "hazelcast/hazelcast:5.2.1"
            )
            .with_exposed_ports("5701")
        )

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        log_string_to_wait_for = r"\[Hazelcast\] cluster is now running"
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=10
        )
        return {"type": "hazelcast", "project_id": "test-project"}

    def teardown(self):
        self.container.stop()