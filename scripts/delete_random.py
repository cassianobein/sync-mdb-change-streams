"""Delete N random documents from the source collection.

Picks N documents at random via $sample and removes them. Each deletion
produces a `delete` event on the change stream.

    python -m scripts.delete_random
    python -m scripts.delete_random --count 3
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from ._common import open_source_collection

logger = logging.getLogger(__name__)


async def delete_random(count: int) -> None:
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
                "Source collection %s.%s is empty; nothing to delete.",
                coll.database.name,
                coll.name,
            )
            return

        ids = [doc["_id"] for doc in sampled]
        result = await coll.delete_many({"_id": {"$in": ids}})
        logger.info(
            "Deleted %d of %d sampled documents from %s.%s",
            result.deleted_count,
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
    parser.add_argument("--count", type=int, default=3)
    args = parser.parse_args()
    asyncio.run(delete_random(args.count))


if __name__ == "__main__":
    main()
