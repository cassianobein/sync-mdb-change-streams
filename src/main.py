"""Entry point: run the change-stream replicator with graceful shutdown."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from .config import ReplicatorConfig
from .replicator import ChangeStreamReplicator


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


async def _run() -> int:
    try:
        config = ReplicatorConfig.from_env()
    except RuntimeError as exc:
        logging.error("Configuration error: %s", exc)
        return 2

    replicator = ChangeStreamReplicator(config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, replicator.request_stop)
        except NotImplementedError:
            # Signal handlers are not supported on some platforms (e.g. Windows).
            pass

    try:
        await replicator.run()
        return 0
    except Exception:
        logging.exception("Replicator crashed.")
        return 1


def main() -> None:
    _configure_logging()
    sys.exit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
