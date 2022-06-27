"""Simple functions, which can be used in both K8s and VM charms."""
# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import string
import secrets
import logging
from typing import List
from charms.zookeeper_libs.v0.zookeeper import ZooKeeperConfiguration

# The unique Charmhub library identifier, never change it
LIBID = "1057f353503741a98ed79309b5be7a31"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version.
LIBPATCH = 0

CONFIG_DIR = "/opt/kafka/config"
DATA_DIR = "/var/lib/kafka"
LOGS_DIR = "/var/log/kafka"
MAIN_CONFIG_PATH = f"{CONFIG_DIR}/kafka.cfg"
AUTH_CONFIG_PATH = f"{CONFIG_DIR}/zookeeper-jaas.cfg"
UNIX_USER = "nobody"
UNIX_GROUP = "nogroup"

logger = logging.getLogger(__name__)


def get_kafka_cmd() -> str:
    """Construct the ZooKeeper startup command line.

    Returns:
        A string representing the command used to start MongoDB.

    """

    cmd = [
        "java",
        "-server -Djava.awt.headless=true",

        "-Xmx1G -Xms1G",
        "-XX:MetaspaceSize=96m",
        "-XX:MinMetaspaceFreeRatio=50",
        "-XX:MaxMetaspaceFreeRatio=80",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:OnOutOfMemoryError='kill -9 %p'",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=20",
        "-XX:InitiatingHeapOccupancyPercent=35",
        "-XX:G1HeapRegionSize=16M",
        "-XX:+ExplicitGCInvokesConcurrent",
        "-XX:MaxInlineLevel=15",
        f"'-Xlog:gc*:file={LOGS_DIR}/kafkaServer-gc.log:time,tags:filecount=10,filesize=100M'",

        "-Dcom.sun.management.jmxremote",
        "-Dcom.sun.management.jmxremote.local.only=true",

        f"-Dkafka.logs.dir={LOGS_DIR}",

        f"-Djava.security.auth.login.config={AUTH_CONFIG_PATH}",

        f"-cp '/opt/kafka/libs/*:{CONFIG_DIR}:'",
        "kafka.Kafka",
        MAIN_CONFIG_PATH,
    ]
    return " ".join(cmd)


def generate_password() -> str:
    """Generate a random password string.

    Returns:
       A random password string.
    """
    choices = string.ascii_letters + string.digits
    return "".join([secrets.choice(choices) for _ in range(32)])


def get_main_config(myid, planned_units: int, fqdn, sync_password, zookeeper_uri: str) -> str:
    """Generate content of the main Kafka config file"""
    replication_factor = int(planned_units / 2) + 1
    sync_str = "listener.name.sasl_plaintext.scram-sha-512.sasl.jaas.config=" \
               "org.apache.kafka.common.security.scram.ScramLoginModule " \
               f"required username=\"sync\" password=\"{sync_password}\";"
    return f"""
        broker.id={myid}
        listeners=SASL_PLAINTEXT://:9093
        advertised.listeners=SASL_PLAINTEXT://{fqdn}:9093
        log.dirs={LOGS_DIR}

        default.replication.factor={replication_factor}
        offsets.topic.replication.factor={replication_factor}
        transaction.state.log.replication.factor={replication_factor}
        transaction.state.log.min.isr={replication_factor}

        zookeeper.connect={zookeeper_uri}

        sasl.enabled.mechanisms=SCRAM-SHA-512
        sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
        security.inter.broker.protocol=SASL_PLAINTEXT
        authorizer.class.name=kafka.security.authorizer.AclAuthorizer
        allow.everyone.if.no.acl.found=false
        super.users=User:sync
        listener.name.sasl_plaintext.sasl.enabled.mechanisms=SCRAM-SHA-512
        {sync_str}
    """


def get_auth_config(config: ZooKeeperConfiguration) -> str:
    """Generate content of the auth ZooKeeper config file"""
    return f"""
        Client {{
            org.apache.zookeeper.server.auth.DigestLoginModule required
            username="{config.username}"
            password="{config.password}";
        }};
    """


def get_add_user_cmd(username, password, zookeeper_uri: str) -> List[str]:
    """Return command to add user"""
    return [
        "java",
        "-cp", f"/opt/kafka/libs/*:{CONFIG_DIR}:",
        f"-Djava.security.auth.login.config={AUTH_CONFIG_PATH}",
        "kafka.admin.ConfigCommand",
        f"--zookeeper={zookeeper_uri}",
        "--alter",
        "--entity-type=users",
        f"--entity-name={username}",
        f"--add-config=SCRAM-SHA-512=[password={password}]",
    ]
