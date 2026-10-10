"""
Kappa replay environment for the Apache Flink speed layer (RF-10).

Pure functions (no Kubernetes, no subprocess) so the semantics are unit-testable
without a cluster.

``KAFKA_START_OFFSET=earliest`` only rewinds a consumer group with no committed
offsets. A group that has already consumed resumes from its last commit, so
flipping the flag alone would replay nothing and still report success. Every
replay therefore rotates ``KAFKA_GROUP_ID`` too.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("dagster.resources.replay")

#: Env var carrying the Kafka consumer group of each speed-layer job.
CONSUMER_GROUP_VARS = ("KAFKA_GROUP_ID", "KAFKA_CLICKSTREAM_GROUP_ID")

#: Env var selecting the read offset ("earliest" = full log replay).
START_OFFSET_VAR = "KAFKA_START_OFFSET"


def replay_group_id(deployment: str, stamp: str) -> str:
    """Returns a fresh, per-replay consumer group name for a deployment."""
    return f"{deployment}-replay-{stamp}"


def build_replay_env(
    current_env: list[dict[str, Any]],
    *,
    consumer_group_var: str,
    group_id: str,
) -> list[dict[str, Any]]:
    """Returns the env block that turns a speed-layer job into a full replay.

    Args:
        current_env: The ``spec.env`` block currently on the FlinkDeployment.
        consumer_group_var: Name of the consumer-group env var to rotate.
        group_id: The fresh consumer group to use for this replay.

    Returns:
        A new env list: every entry preserved except ``consumer_group_var``
        (rotated to ``group_id``) and ``KAFKA_START_OFFSET`` (set to
        ``earliest``).

    Raises:
        ValueError: If ``consumer_group_var`` is absent, which would mean the
            replay could not be guaranteed to read from offset 0.
    """
    if not any(entry.get("name") == consumer_group_var for entry in current_env):
        raise ValueError(
            f"FlinkDeployment env does not declare {consumer_group_var}; cannot guarantee a "
            f"full-log replay because the consumer group would keep its committed offsets."
        )

    rotated: list[dict[str, Any]] = []
    for entry in current_env:
        updated = dict(entry)
        name = updated.get("name")
        if name == consumer_group_var:
            updated["value"] = group_id
        elif name == START_OFFSET_VAR:
            updated["value"] = "earliest"
        rotated.append(updated)
    return rotated
