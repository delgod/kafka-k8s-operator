#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm for Kafka on Kubernetes.

Apache Kafka is an open-source distributed event streaming platform
used by thousands of companies for high-performance data pipelines,
streaming analytics, data integration, and mission-critical applications.
"""

import logging
from typing import Dict, Optional

from charms.kafka_libs.v0.helpers import (
    AUTH_CONFIG_PATH,
    DATA_DIR,
    LOGS_DIR,
    MAIN_CONFIG_PATH,
    UNIX_GROUP,
    UNIX_USER,
    generate_password,
    get_auth_config,
    get_kafka_cmd,
    get_main_config,
)
from charms.zookeeper_libs.v0.zookeeper import ZooKeeperConfiguration
from ops.charm import CharmBase, HookEvent, RelationDepartedEvent, RelationJoinedEvent
from ops.main import main
from ops.model import ActiveStatus
from ops.pebble import Layer, PathError, ProtocolError

logger = logging.getLogger(__name__)
PEER = "kafka-peers"
REL_NAME = "zookeeper"


class KafkaCharm(CharmBase):
    """A Juju Charm to deploy Kafka on Kubernetes."""

    def __init__(self, *args) -> None:
        super().__init__(*args)
        self.framework.observe(self.on.kafka_pebble_ready, self._reconfig)
        self.framework.observe(self.on.leader_elected, self._on_leader_elected)
        self.framework.observe(self.on[REL_NAME].relation_joined, self._zk_joined)
        self.framework.observe(self.on[REL_NAME].relation_changed, self._reconfig)
        self.framework.observe(self.on[REL_NAME].relation_departed, self._reconfig)

    def _zk_joined(self, event: RelationJoinedEvent) -> None:
        if self.unit.is_leader():
            event.relation.data[self.app].update({"chroot": "/" + self.app.name})

    def _reconfig(self, event: HookEvent) -> None:
        """Configure pebble layer specification."""
        # Wait for the password used for synchronisation between members.
        # It should be generated once and be the same on all members.
        if "sync_password" not in self.app_data:
            logger.error("Super password is not ready yet.")
            event.defer()
            return

        # Get a reference the container attribute
        container = self.unit.get_container("kafka")

        # Prepare configs
        if self.zookeeper_config is not None and type(event) is not RelationDepartedEvent:
            self._put_general_config(event)
            self._put_auth_config(event)
            container.add_layer("kafka", self._kafka_layer(True), combine=True)
        else:
            if container.get_services("kafka"):
                container.stop("kafka")
            container.add_layer("kafka", self._kafka_layer(False), combine=True)

        # Add initial Pebble config layer using the Pebble API
        # Restart changed services and start startup-enabled services.
        container.replan()
        # TODO: rework status
        self.unit.status = ActiveStatus()

    def _generate_passwords(self) -> None:
        """Generate passwords and put them into peer relation.

        The same sync password on all members needed.
        It means, it is needed to generate them once and share between members.
        NB: only leader should execute this function.
        """
        if "sync_password" not in self.app_data:
            self.app_data["sync_password"] = generate_password()

    def _on_leader_elected(self, _):
        # Admin password should be created before running Kafka.
        # This code runs on leader_elected event before pebble_ready
        self._generate_passwords()

    @staticmethod
    def _kafka_layer(startup: bool) -> Layer:
        """Returns a Pebble configuration layer for Kafka."""
        layer_config = {
            "summary": "Kafka layer",
            "description": "Pebble config layer for Kafka",
            "services": {
                "kafka": {
                    "override": "replace",
                    "summary": "Kafka",
                    "command": get_kafka_cmd(),
                    "startup": "enabled" if startup else "disabled",
                    "user": UNIX_USER,
                    "group": UNIX_GROUP,
                }
            },
        }
        return Layer(layer_config)

    @property
    def app_data(self) -> Dict[str, str]:
        """Peer relation data object."""
        return self.model.get_relation(PEER).data[self.app]

    @property
    def zookeeper_config(self) -> Optional[ZooKeeperConfiguration]:
        """Create a configuration object with settings.

        Needed for correct handling interactions with MongoDB.

        Returns:
            A MongoDBConfiguration object
        """
        for relation in self.model.relations[REL_NAME]:
            username = relation.data[relation.app].get("username", None)
            password = relation.data[relation.app].get("password", None)
            hosts = relation.data[relation.app].get("endpoints", None)
            chroot = relation.data[relation.app].get("chroot", None)
            if username is None or password is None or hosts is None or chroot is None:
                continue
            return ZooKeeperConfiguration(
                username=username,
                password=password,
                hosts=set(hosts.split(",")),
                chroot=chroot,
                acl="cdrwa",
            )

        return None

    def _put_auth_config(self, event: HookEvent) -> None:
        """Upload the Auth config to a workload container."""
        container = self.unit.get_container("kafka")
        if not container.can_connect():
            logger.debug("kafka container is not ready yet.")
            event.defer()
            return

        new_content = get_auth_config(self.zookeeper_config)
        old_content = None
        try:
            old_content = container.pull(AUTH_CONFIG_PATH).read()
        except (PathError, ProtocolError):
            pass

        if new_content == old_content:
            logger.debug("Restart not needed")
            return

        try:
            container.push(
                AUTH_CONFIG_PATH,
                new_content,
                make_dirs=True,
                permissions=0o400,
                user=UNIX_USER,
                group=UNIX_GROUP,
            )
        except (PathError, ProtocolError) as e:
            logger.error("Cannot put configs: %r", e)
            event.defer()
            return

        if container.get_services("kafka"):
            container.stop("kafka")

        container.replan()

    def _put_general_config(self, event: HookEvent) -> None:
        """Upload the configs to a workload container."""
        container = self.unit.get_container("kafka")
        if not container.can_connect():
            logger.debug("kafka container is not ready yet.")
            event.defer()
            return
        try:
            if not container.exists(DATA_DIR):
                container.make_dir(
                    DATA_DIR,
                    make_parents=True,
                    permissions=0o700,
                    user=UNIX_USER,
                    group=UNIX_GROUP,
                )
            if not container.exists(LOGS_DIR):
                container.make_dir(
                    LOGS_DIR,
                    make_parents=True,
                    permissions=0o755,
                    user=UNIX_USER,
                    group=UNIX_GROUP,
                )
            myid = self._get_unit_id_by_unit(self.unit.name)
            container.push(
                MAIN_CONFIG_PATH,
                get_main_config(myid, self.app.planned_units(), self.zookeeper_config),
                make_dirs=True,
                permissions=0o400,
                user=UNIX_USER,
                group=UNIX_GROUP,
            )
        except (PathError, ProtocolError) as e:
            logger.error("Cannot put configs: %r", e)
            event.defer()
            return

    @property
    def _is_zookeeper_ready(self) -> bool:
        return False

    @staticmethod
    def _get_unit_id_by_unit(unit_name: str) -> int:
        """Cut number from the unit name."""
        return int(unit_name.split("/")[1])

    def _get_hostname_by_unit(self, unit_name: str) -> str:
        """Create a DNS name for a unit.

        Args:
            unit_name: the juju unit name, e.g. "kafka-k8s/1".

        Returns:
            A string representing the hostname of the unit.
        """
        unit_id = self._get_unit_id_by_unit(unit_name)
        return f"{self.app.name}-{unit_id}.{self.app.name}-endpoints"


if __name__ == "__main__":
    main(KafkaCharm)
