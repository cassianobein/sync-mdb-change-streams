"""Insert N additional random product documents into the source collection.

Default: 10 documents. Intended to be run while the replicator is tailing so
you can watch inserts propagate to the target.

    python -m scripts.insert_random
    python -m scripts.insert_random --count 25
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from pymongo import InsertOne

from ._common import open_source_collection, random_product

logger = logging.getLogger(__name__)


async def insert_random(count: int) -> None:
    if count <= 0:
        raise ValueError("--count must be a positive integer.")

    client, coll = open_source_collection()
    try:
        ops = [InsertOne(random_product()) for _ in range(count)]
        result = await coll.bulk_write(ops, ordered=False)
        logger.info(
            "Inserted %d new documents into %s.%s",
            result.inserted_count,
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
    parser.add_argument("--count", type=int, default=10)
    args = parser.parse_args()
    asyncio.run(insert_random(args.count))


if __name__ == "__main__":
    main()
