# sync-mdb-change-streams

Real-time MongoDB collection mirroring via [change streams](https://www.mongodb.com/docs/manual/changeStreams/), with resumable replication and a runnable demo.

A lightweight Python service that tails a source collection's change stream and applies every `insert` / `update` / `replace` / `delete` to a target collection on a separate cluster — with persisted resume tokens for seamless restarts.

## Features

- **Resumable** — change-stream resume tokens are persisted in the target DB, so restarts pick up exactly where they left off (no duplicates, no gaps).
- **Bootstrap-safe** — on first run, a starting token is captured **before** the initial bulk copy, so events that occur during the copy are not lost.
- **Cross-cluster** — source and target can be on different Atlas clusters, regions, or accounts.
- **Async I/O** — built on [`motor`](https://motor.readthedocs.io/) and `asyncio` for a single non-blocking event loop.
- **Graceful shutdown** — `SIGINT` / `SIGTERM` stop the stream cleanly between events.
- **BSON-aware logging** — every replicated document is logged as Extended JSON via `bson.json_util`.

## How It Works

```
┌─────────────────┐    change stream    ┌──────────────────────┐    upsert/delete    ┌─────────────────┐
│ Source cluster  │────────────────────▶│ ChangeStreamReplicator│────────────────────▶│ Target cluster  │
│ (replica set)   │                     │   (asyncio + motor)   │                     │ (replica set)   │
└─────────────────┘                     └──────────────────────┘                     └─────────────────┘
                                                  │
                                                  │ resume token
                                                  ▼
                                        ┌──────────────────────┐
                                        │  _replication_state  │  (in target DB)
                                        └──────────────────────┘
```

1. On startup, the replicator looks for a stored resume token in the target DB.
2. **No token found** → open a change stream to capture a starting token, bulk-copy existing source docs to the target, then begin tailing from the captured token.
3. **Token found** → resume tailing immediately, skipping the bulk copy.
4. For every event, the equivalent op is applied to the target and the new resume token is persisted.

## Project Structure

```
.
├── src/
│   ├── config.py          # Env-backed ReplicatorConfig (loads .env)
│   ├── replicator.py      # ChangeStreamReplicator: bootstrap + tail + apply
│   └── main.py            # Entrypoint with signal-driven graceful shutdown
├── scripts/
│   ├── seed_products.py   # Bulk-seed N random products into the source
│   ├── insert_random.py   # Insert N random docs (drives `insert` events)
│   ├── update_random.py   # Update N $sampled docs (drives `update` events)
│   └── delete_random.py   # Delete N $sampled docs (drives `delete` events)
├── tests/
│   └── test_replicator.py # AsyncMock-based unit tests (no live MongoDB needed)
├── DEMO.md                # End-to-end demo walkthrough
├── .env.example           # Copy to .env and fill in your URIs
├── requirements.txt       # Runtime deps (motor, python-dotenv)
└── requirements-dev.txt   # Adds pytest + pytest-asyncio
```

## Requirements

- **Python 3.11+**
- Two MongoDB **replica set** clusters (Atlas free tier is sufficient). Change streams require a replica set.
- Read/write access from your machine to both clusters.

## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# edit .env with your SOURCE_URI / TARGET_URI / DB / collection names

python -m scripts.seed_products   # seed source with 100 random docs
python -m src.main                # start the replicator (bulk copy + tail)
```

In a second terminal, generate live traffic and watch it replicate:

```bash
python -m scripts.insert_random
python -m scripts.update_random
python -m scripts.delete_random
```

For the full step-by-step walkthrough (including resumability testing and troubleshooting), see [`DEMO.md`](./DEMO.md).

## Configuration

All configuration is environment-driven (loaded from `.env` via `python-dotenv`):

| Variable                  | Required | Default                 | Description                                                       |
| ------------------------- | :------: | ----------------------- | ----------------------------------------------------------------- |
| `SOURCE_URI`              |    ✅    | —                       | MongoDB connection string for the source cluster.                 |
| `TARGET_URI`              |    ✅    | —                       | MongoDB connection string for the target cluster.                 |
| `SOURCE_DB`               |    ✅    | —                       | Database containing the collection to mirror.                     |
| `SOURCE_COLLECTION`       |    ✅    | —                       | Collection to mirror.                                             |
| `TARGET_DB`               |    ✅    | —                       | Target database (will be created if absent).                      |
| `TARGET_COLLECTION`       |    ✅    | —                       | Target collection (will be created if absent).                    |
| `RESUME_STATE_COLLECTION` |          | `_replication_state`    | Collection in the target DB used to persist the resume token.     |
| `PIPELINE_ID`             |          | `products_default`      | Logical id — allows running multiple replicators in parallel.     |

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

The suite uses `AsyncMock`-injected collections, so **no live MongoDB is required**.

## License

[MIT](./LICENSE) © Cassiano Ziegler Bein
