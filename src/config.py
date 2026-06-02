"""Environment-backed configuration for the change-stream replicator."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

from dotenv import load_dotenv


_REQUIRED_VARS: Final[tuple[str, ...]] = (
    "SOURCE_URI",
    "TARGET_URI",
    "SOURCE_DB",
    "SOURCE_COLLECTION",
    "TARGET_DB",
    "TARGET_COLLECTION",
)


@dataclass(frozen=True)
class ReplicatorConfig:
    source_uri: str
    target_uri: str
    source_db: str
    source_collection: str
    target_db: str
    target_collection: str
    resume_state_collection: str
    pipeline_id: str

    @staticmethod
    def from_env() -> "ReplicatorConfig":
        load_dotenv()

        missing = [name for name in _REQUIRED_VARS if not os.getenv(name)]
        if missing:
            raise RuntimeError(
                f"Missing required environment variables: {', '.join(missing)}. "
                "Copy .env.example to .env and fill in the values."
            )

        return ReplicatorConfig(
            source_uri=os.environ["SOURCE_URI"],
            target_uri=os.environ["TARGET_URI"],
            source_db=os.environ["SOURCE_DB"],
            source_collection=os.environ["SOURCE_COLLECTION"],
            target_db=os.environ["TARGET_DB"],
            target_collection=os.environ["TARGET_COLLECTION"],
            resume_state_collection=os.getenv(
                "RESUME_STATE_COLLECTION", "_replication_state"
            ),
            pipeline_id=os.getenv("PIPELINE_ID", "products_default"),
        )
