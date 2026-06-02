"""Shared helpers for the load-generator demo scripts.

Reuses ReplicatorConfig so all scripts read the same .env and target the same
source collection that the replicator is tailing.
"""

from __future__ import annotations

import random
import string
from datetime import datetime, timezone
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from src.config import ReplicatorConfig


_CATEGORIES: tuple[str, ...] = (
    "electronics", "books", "clothing", "home", "toys", "sports", "food",
)
_ADJECTIVES: tuple[str, ...] = (
    "Ultra", "Eco", "Smart", "Premium", "Compact", "Wireless",
    "Vintage", "Pro", "Mini", "Heavy-Duty",
)
_NOUNS: tuple[str, ...] = (
    "Widget", "Gadget", "Speaker", "Lamp", "Mug", "Chair",
    "Backpack", "Bottle", "Notebook", "Drone",
)
_TAGS: tuple[str, ...] = (
    "sale", "new", "popular", "limited", "eco", "imported", "exclusive",
)


def random_sku() -> str:
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
    return f"SKU-{suffix}"


def random_product() -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "sku": random_sku(),
        "name": f"{random.choice(_ADJECTIVES)} {random.choice(_NOUNS)}",
        "category": random.choice(_CATEGORIES),
        "price": round(random.uniform(1.99, 999.99), 2),
        "stock": random.randint(0, 500),
        "tags": random.sample(_TAGS, k=random.randint(0, 3)),
        "revision": 0,
        "createdAt": now,
        "updatedAt": now,
    }


def open_source_collection() -> tuple[AsyncIOMotorClient, AsyncIOMotorCollection]:
    config = ReplicatorConfig.from_env()
    client = AsyncIOMotorClient(config.source_uri)
    coll = client[config.source_db][config.source_collection]
    return client, coll
