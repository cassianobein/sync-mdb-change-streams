"""Update N random documents in the source collection.

Picks N documents at random via $sample and mutates their price/stock fields,
bumps a `revision` counter, and refreshes `updatedAt`. Each update produces
an `update` event on the change stream.

    python -m scripts.update_random
    python -m scripts.update_random --count 5
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
from datetime import datetime, timezone

from ._common import open_source_collection

logger = logging.getLogger(__name__)


async def update_random(count: int) -> None:
    if count <= 0:
        raise ValueError("--count must be a positive integer.")

    client, coll = open_source_collection()
    try:
        sample_cursor = coll.aggregate(
            [{"$sample": {"size": count}}, {"$project": {"_id": 1}}]
        )
        sampled = await sample_cursor.to_list(length=count)
        if not sampled:
            logger.warning(
                "Source collection %s.%s is empty; nothing to update.",
                coll.database.name,
                coll.name,
            )
            return

        now = datetime.now(timezone.utc)
        updated = 0
        for doc in sampled:
            result = await coll.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "price": round(random.uniform(1.99, 999.99), 2),
                        "stock": random.randint(0, 500),
                        "updatedAt": now,
                    },
                    "$inc": {"revision": 1},
                },
            )
            updated += result.modified_count

        logger.info(
            "Updated %d of %d sampled documents in %s.%s",
            updated,
            len(sampled),
            coll.database.name,
            coll.name,
        )
    finally:
        client.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=5)
    args = parser.parse_args()
    asyncio.run(update_random(args.count))


if __name__ == "__main__":
    main()
