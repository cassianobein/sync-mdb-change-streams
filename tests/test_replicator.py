"""Unit tests for ChangeStreamReplicator dispatch and resume-token persistence.

These tests bypass __init__ to avoid opening real Motor clients; the relevant
collection attributes are injected as AsyncMocks so each branch of
`_apply_event` can be verified in isolation.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from src.replicator import ChangeStreamReplicator


def _make_replicator() -> ChangeStreamReplicator:
    """Construct a replicator with mocked collections (no network I/O)."""
    repl = ChangeStreamReplicator.__new__(ChangeStreamReplicator)
    repl._config = None  # not used by tested methods
    repl._source = AsyncMock()
    repl._target = AsyncMock()
    repl._state = AsyncMock()
    repl._source_client = AsyncMock()
    repl._target_client = AsyncMock()
    return repl


def _event(op: str, doc_id: Any, full_document: Any = ...) -> dict[str, Any]:
    evt: dict[str, Any] = {
        "operationType": op,
        "documentKey": {"_id": doc_id},
    }
    if full_document is not ...:
        evt["fullDocument"] = full_document
    return evt


@pytest.mark.parametrize("op", ["insert", "update", "replace"])
async def test_apply_event_upserts_full_document(op: str) -> None:
    repl = _make_replicator()
    doc = {"_id": "p1", "name": "Widget", "price": 9.99}

    await repl._apply_event(_event(op, "p1", full_document=doc))

    repl._target.replace_one.assert_awaited_once_with(
        {"_id": "p1"}, doc, upsert=True
    )
    repl._target.delete_one.assert_not_called()


async def test_apply_event_delete_removes_target_doc() -> None:
    repl = _make_replicator()

    await repl._apply_event(_event("delete", "p1"))

    repl._target.delete_one.assert_awaited_once_with({"_id": "p1"})
    repl._target.replace_one.assert_not_called()


@pytest.mark.parametrize("op", ["insert", "update", "replace"])
async def test_apply_event_skips_when_full_document_missing(op: str) -> None:
    """If updateLookup couldn't fetch the post-image, we must not write nulls."""
    repl = _make_replicator()

    await repl._apply_event(_event(op, "p1", full_document=None))

    repl._target.replace_one.assert_not_called()
    repl._target.delete_one.assert_not_called()


@pytest.mark.parametrize("op", ["invalidate", "drop", "rename", "dropDatabase"])
async def test_apply_event_ignores_unsupported_ops(op: str) -> None:
    repl = _make_replicator()

    await repl._apply_event({"operationType": op})

    repl._target.replace_one.assert_not_called()
    repl._target.delete_one.assert_not_called()


async def test_load_resume_token_returns_none_when_absent() -> None:
    repl = _make_replicator()
    repl._config = type("C", (), {"pipeline_id": "pid"})()
    repl._state.find_one = AsyncMock(return_value=None)

    assert await repl._load_resume_token() is None
    repl._state.find_one.assert_awaited_once_with({"_id": "pid"})


async def test_load_resume_token_returns_stored_token() -> None:
    repl = _make_replicator()
    repl._config = type("C", (), {"pipeline_id": "pid"})()
    token = {"_data": "abc"}
    repl._state.find_one = AsyncMock(return_value={"_id": "pid", "token": token})

    assert await repl._load_resume_token() == token


async def test_save_resume_token_upserts_state_doc() -> None:
    repl = _make_replicator()
    repl._config = type("C", (), {"pipeline_id": "pid"})()
    token = {"_data": "xyz"}

    await repl._save_resume_token(token)

    repl._state.update_one.assert_awaited_once_with(
        {"_id": "pid"},
        {"$set": {"token": token}},
        upsert=True,
    )
