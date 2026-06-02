# Change Stream Replicator — Demo Guide

This guide walks you through a live, end-to-end demo of the replicator: it tails a source MongoDB collection's change stream and mirrors every insert/update/replace/delete into a target collection in near real time.

## What the Demo Shows

1. **Initial bootstrap** — on first run with no saved resume token, the replicator captures a starting token, bulk-copies all existing source documents to the target, then begins tailing.
2. **Live replication** — inserts, updates, and deletes performed against the source are reflected in the target within milliseconds.
3. **Resumability** — stop the replicator (Ctrl+C), mutate the source, restart, and watch it pick up exactly where it left off via the persisted resume token.

## Prerequisites

- **Python 3.11+**
- Two MongoDB **replica set** clusters (Atlas free tier works for both source and target). Change streams require a replica set.
- A MongoDB user on each cluster with read/write access to the demo databases.
- Network access from your machine to both clusters (add your IP to each Atlas project's Network Access list).

## 1. Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -r requirements-dev.txt # optional, for running tests
```

## 2. Configure Environment

Copy the example file and fill in your connection strings:

```bash
cp .env.example .env
```

Edit `.env`:

```dotenv
SOURCE_URI=mongodb+srv://<user>:<pass>@<source-cluster>/?retryWrites=true&w=majority
TARGET_URI=mongodb+srv://<user>:<pass>@<target-cluster>/?retryWrites=true&w=majority

SOURCE_DB=sourceDb
SOURCE_COLLECTION=products

TARGET_DB=targetDb
TARGET_COLLECTION=products

RESUME_STATE_COLLECTION=_replication_state
PIPELINE_ID=products_default
```

> The source and target may live on the same cluster (just use different DB names), but using two clusters showcases cross-cluster replication.

## 3. Seed the Source Collection

Insert 100 random product documents into the source collection. This data will be bulk-copied to the target on the replicator's first run.

```bash
python -m scripts.seed_products              # defaults to 100 docs
python -m scripts.seed_products --count 250  # custom batch size
```

Expected output:

```
... INFO scripts.seed_products - Inserted 100 documents into sourceDb.products
```

## 4. Start the Replicator

In a dedicated terminal, run:

```bash
python -m src.main
```

On the **first** run you should see (roughly, in order):

- `No resume token found; performing initial bulk copy.`
- `Captured starting resume token at <timestamp>`
- `Bulk-copied N documents to targetDb.products`
- `Tailing change stream on sourceDb.products...`

The process now blocks, watching the change stream. Leave it running.

## 5. Generate Live Traffic

Open a **second terminal** (keep the replicator running in the first). Run any of the load-generator scripts and watch the replicator log each event.

### Inserts

```bash
python -m scripts.insert_random              # 10 new docs (default)
python -m scripts.insert_random --count 25
```

### Updates

Picks N random documents via `$sample`, mutates `price`/`stock`, increments `revision`, and refreshes `updatedAt`:

```bash
python -m scripts.update_random              # 5 docs (default)
python -m scripts.update_random --count 20
```

### Deletes

Picks N random documents via `$sample` and removes them:

```bash
python -m scripts.delete_random              # 3 docs (default)
python -m scripts.delete_random --count 10
```

For each operation, the replicator should log a corresponding `insert`, `update`, or `delete` event and apply it to the target.

## 6. Verify Replication

Connect to both clusters (mongosh, Compass, or Atlas Data Explorer) and confirm counts match:

```js
// On source
use sourceDb
db.products.countDocuments()

// On target
use targetDb
db.products.countDocuments()
```

Spot-check a few documents by `sku` to confirm field-level parity.

## 7. Test Resumability

1. In the replicator terminal, press **Ctrl+C** — you should see `Shutdown requested; stopping change stream.`
2. While the replicator is stopped, run `python -m scripts.insert_random --count 5` and `python -m scripts.delete_random --count 2`.
3. Restart the replicator: `python -m src.main`.
4. Look for `Resuming from saved token` (no bulk copy this time). The 5 inserts and 2 deletes performed while it was down are applied immediately, and counts on source/target match again.

## 8. Reset the Demo

To start over with a clean slate, drop the demo collections on both clusters:

```js
use sourceDb; db.products.drop();
use targetDb; db.products.drop(); db._replication_state.drop();
```

Then re-run from step 3.

## Run the Tests

The repository ships with unit tests that exercise the replicator's event-dispatch logic and resume-token persistence using `AsyncMock` collections — no live MongoDB is required.

Install the dev requirements (once):

```bash
pip install -r requirements-dev.txt
```

Run the full suite from the project root:

```bash
pytest
```

`pytest.ini` enables `asyncio_mode = auto` and points `testpaths` at the `tests/` directory, so async tests are discovered and awaited automatically.

Useful variants:

```bash
pytest -v                                  # verbose output, one line per test
pytest tests/test_replicator.py            # run a single file
pytest -k "resume_token"                   # run tests matching an expression
pytest --tb=short                          # shorter tracebacks on failure
```

## Troubleshooting

- **`Missing required environment variables`** — `.env` is missing or incomplete; re-check step 2.
- **`The $changeStream stage is only supported on replica sets`** — your cluster is standalone; use an Atlas cluster or a local replica set.
- **Authentication / network errors** — verify the DB user's roles and that your current IP is allow-listed in Atlas Network Access.
- **Target lagging or empty after bulk copy** — confirm the replicator log shows `Bulk-copied N documents`; if `N` is 0, your source was empty when it started.
