"""
Kappa replay semantics for the Flink speed layer (RF-10).

The replay logic lives in :mod:`pipelines.orchestration.flink_replay` as pure
functions precisely so it can be tested here without a Kubernetes cluster.
"""

from __future__ import annotations

import pytest

from pipelines.orchestration.flink_replay import (
    CONSUMER_GROUP_VARS,
    START_OFFSET_VAR,
    build_replay_env,
    replay_group_id,
)

ORDERS_ENV = [
    {"name": "KAFKA_BOOTSTRAP_SERVERS", "value": "kafka.platform.svc.cluster.local:9092"},
    {"name": "KAFKA_TOPIC", "value": "sales.orders.events"},
    {"name": "KAFKA_GROUP_ID", "value": "flink-orders-speed-group"},
    {"name": "KAFKA_START_OFFSET", "value": "latest"},
    {"name": "ICEBERG_CATALOG_URI", "value": "http://polaris.platform.svc.cluster.local:8181/api/catalog"},
]


def test_build_replay_env_rewinds_offset_and_rotates_group() -> None:
    env = build_replay_env(
        ORDERS_ENV,
        consumer_group_var="KAFKA_GROUP_ID",
        group_id="flink-orders-stream-replay-20260317120000",
    )
    values = {entry["name"]: entry["value"] for entry in env}

    assert values["KAFKA_START_OFFSET"] == "earliest"
    assert values["KAFKA_GROUP_ID"] == "flink-orders-stream-replay-20260317120000"
    # Everything else must survive untouched or the job loses its wiring.
    assert values["KAFKA_BOOTSTRAP_SERVERS"] == "kafka.platform.svc.cluster.local:9092"
    assert values["KAFKA_TOPIC"] == "sales.orders.events"
    assert values["ICEBERG_CATALOG_URI"].endswith("/api/catalog")
    assert len(env) == len(ORDERS_ENV)


def test_build_replay_env_does_not_mutate_the_input() -> None:
    before = [dict(entry) for entry in ORDERS_ENV]
    build_replay_env(ORDERS_ENV, consumer_group_var="KAFKA_GROUP_ID", group_id="g")
    assert ORDERS_ENV == before


def test_build_replay_env_never_rewinds_to_earliest_with_a_stale_group() -> None:
    """The whole point: earliest + an already-committed group replays nothing."""
    env = build_replay_env(ORDERS_ENV, consumer_group_var="KAFKA_GROUP_ID", group_id="brand-new-group")
    values = {entry["name"]: entry["value"] for entry in env}

    assert values["KAFKA_START_OFFSET"] == "earliest"
    assert values["KAFKA_GROUP_ID"] == "brand-new-group"
    assert values["KAFKA_GROUP_ID"] != "flink-orders-speed-group"


def test_build_replay_env_rejects_a_deployment_without_a_consumer_group_var() -> None:
    env_without_group = [entry for entry in ORDERS_ENV if entry["name"] != "KAFKA_GROUP_ID"]
    with pytest.raises(ValueError, match="cannot guarantee"):
        build_replay_env(env_without_group, consumer_group_var="KAFKA_GROUP_ID", group_id="g")


@pytest.mark.parametrize("consumer_group_var", CONSUMER_GROUP_VARS)
def test_each_supported_consumer_group_var_can_be_rotated(consumer_group_var: str) -> None:
    env = [
        {"name": consumer_group_var, "value": "original"},
        {"name": START_OFFSET_VAR, "value": "latest"},
    ]
    rotated = build_replay_env(env, consumer_group_var=consumer_group_var, group_id="fresh")
    values = {entry["name"]: entry["value"] for entry in rotated}
    assert values[consumer_group_var] == "fresh"
    assert values[START_OFFSET_VAR] == "earliest"


def test_replay_group_id_is_unique_per_deployment_and_stamp() -> None:
    first = replay_group_id("flink-orders-stream", "20260317120000")
    second = replay_group_id("flink-orders-stream", "20260317120001")
    other = replay_group_id("flink-clickstream-stream", "20260317120000")

    assert first == "flink-orders-stream-replay-20260317120000"
    assert len({first, second, other}) == 3
