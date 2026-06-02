"""Continuously mirror a MongoDB collection to another cluster via change streams.

Strategy:
1. If no resume token is stored, open a change-stream cursor on the source to
   capture a starting resume token, then perform a one-time bulk copy of the
   existing documents, then begin tailing from the captured token. This avoids
   losing events that happen during the initial copy.
2. If a resume token exists, skip the initial copy and resume tailing.
3. For every change event, apply the equivalent operation on the target and
   persist the new resume token in the target DB so restarts are seamless.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Mapping

from bson.json_util import dumps as bson_dumps
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
)
from pymongo import ReplaceOne
from pymongo.errors import PyMongoError

from .config import ReplicatorConfig

logger = logging.getLogger(__name__)

_BULK_COPY_BATCH_SIZE = 500
_SUPPORTED_OPS = {"insert", "update", "replace", "delete"}


def _format_doc(doc: Mapping[str, Any]) -> str:
    """Serialize a BSON document to a compact JSON string for logging."""
    try:
        return bson_dumps(doc)
    except (TypeError, ValueError) as exc:
        return f"<unserializable doc: {exc}>"


class ChangeStreamReplicator:
    """Mirrors a single source collection to a single target collection."""

    def __init__(self, config: ReplicatorConfig) -> None:
        self._config = config
        self._source_client = AsyncIOMotorClient(config.source_uri)
        self._target_client = AsyncIOMotorClient(config.target_uri)

        self._source: AsyncIOMotorCollection = self._source_client[
            config.source_db
        ][config.source_collection]
        self._target: AsyncIOMotorCollection = self._target_client[
            config.target_db
        ][config.target_collection]
        self._state: AsyncIOMotorCollection = self._target_client[
            config.target_db
        ][config.resume_state_collection]

        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        try:
            resume_token = await self._load_resume_token()

            if resume_token is None:
                logger.info("No resume token found. Bootstrapping pipeline.")
                resume_token = await self._bootstrap()
            else:
                logger.info("Resuming from previously stored token.")

            await self._tail_changes(resume_token)
        finally:
            self._source_client.close()
            self._target_client.close()

    def request_stop(self) -> None:
        self._stop_event.set()

    async def _bootstrap(self) -> Mapping[str, Any]:
        """Capture a starting token, then perform the initial bulk copy."""
        async with self._source.watch(full_document="updateLookup") as stream:
            start_token = stream.resume_token
            if start_token is None:
                raise RuntimeError("Could not obtain initial resume token.")
            logger.info("Captured starting resume token; running initial copy.")

        await self._initial_copy()
        await self._save_resume_token(start_token)
        return start_token

    async def _initial_copy(self) -> None:
        cursor = self._source.find({}, no_cursor_timeout=False)
        batch: list[ReplaceOne] = []
        total = 0
        async for doc in cursor:
            batch.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
            if len(batch) >= _BULK_COPY_BATCH_SIZE:
                total += await self._flush(batch)
        if batch:
            total += await self._flush(batch)
        logger.info("Initial copy complete. Documents copied: %d", total)

    async def _flush(self, batch: list[ReplaceOne]) -> int:
        if not batch:
            return 0
        result = await self._target.bulk_write(batch, ordered=False)
        count = len(batch)
        batch.clear()
        logger.debug(
            "Bulk wrote %d ops (upserts=%d, modified=%d).",
            count,
            result.upserted_count,
            result.modified_count,
        )
        return count

    async def _tail_changes(self, resume_token: Mapping[str, Any]) -> None:
        logger.info("Tailing change stream...")
        while not self._stop_event.is_set():
            try:
                async with self._source.watch(
                    full_document="updateLookup",
                    resume_after=resume_token,
                ) as stream:
                    async for event in stream:
                        await self._apply_event(event)
                        resume_token = event["_id"]
                        await self._save_resume_token(resume_token)
                        if self._stop_event.is_set():
                            break
            except PyMongoError as exc:
                logger.warning("Change stream error: %s. Reconnecting in 5s.", exc)
                await asyncio.sleep(5)

    async def _apply_event(self, event: Mapping[str, Any]) -> None:
        op = event.get("operationType")
        if op not in _SUPPORTED_OPS:
            logger.debug("Ignoring unsupported op: %s", op)
            return

        doc_id = event["documentKey"]["_id"]

        if op == "delete":
            await self._target.delete_one({"_id": doc_id})
            logger.info("Replicated delete _id=%s", doc_id)
            return

        full_doc = event.get("fullDocument")
        if full_doc is None:
            logger.warning("Missing fullDocument for %s _id=%s; skipping.", op, doc_id)
            return

        await self._target.replace_one({"_id": doc_id}, full_doc, upsert=True)
        logger.info("Replicated %s _id=%s doc=%s", op, doc_id, _format_doc(full_doc))

    async def _load_resume_token(self) -> Mapping[str, Any] | None:
        doc = await self._state.find_one({"_id": self._config.pipeline_id})
        return doc.get("token") if doc else None

    async def _save_resume_token(self, token: Mapping[str, Any]) -> None:
        await self._state.update_one(
            {"_id": self._config.pipeline_id},
            {"$set": {"token": token}},
            upsert=True,
        )
