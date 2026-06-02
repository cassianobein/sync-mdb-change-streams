"""Bulk-seed the source collection with N freshly-generated product documents.

Default: 100 documents. Run any time to add another batch.

    python -m scripts.seed_products
    python -m scripts.seed_products --count 250
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from pymongo import InsertOne

from ._common import open_source_collection, random_product

logger = logging.getLogger(__name__)


async def seed(count: int) -> None:
    if count <= 0:
        raise ValueError("--count must be a positive integer.")

    client, coll = open_source_collection()
    try:
        ops = [InsertOne(random_product()) for _ in range(count)]
        result = await coll.bulk_write(ops, ordered=False)
        logger.info(
            "Inserted %d documents into %s.%s",
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
    parser.add_argument("--count", type=int, default=100)
    args = parser.parse_args()
    asyncio.run(seed(args.count))


if __name__ == "__main__":
    main()
