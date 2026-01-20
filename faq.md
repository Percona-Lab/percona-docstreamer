# Frequently Asked Questions

## Q: Where is data stored during the Full Load sync? Will this cause Out-Of-Memory (OOM) errors on my Source or Destination database?
**A:** No, docStreamer is designed to minimize memory pressure on your Source and Destination databases.

Where Data Lives: The data from batches is stored exclusively in the RAM of the machine running docStreamer. It acts as a streaming middleman, holding data in memory only long enough to transfer it from Source to Destination.

Source Database Impact: The impact is minimal. docStreamer uses "cursors" to stream documents one by one and uses efficient range queries (based on _id) to avoid loading large datasets into the Source's memory.

Destination Database Impact: The impact is standard for write operations. Data is sent in bulk batches (default ~48MB, this number can change depending on your settings). The destination database processes these writes normally and does not need to cache the entire migration in RAM.

OOM Risk: The risk of running out of memory lies with the host running docStreamer, not your databases. If that machine is undersized, docStreamer may crash, but your databases will remain safe.


## Q: What happens if I run a new migration (Full Sync) into a destination where the Database and Collection names already exist?
**A:** If you perform a Full Sync into a cluster that already contains data with matching Database and Collection names, docStreamer resolves conflicts based on the unique _id of the documents:
  1. Non-Matching IDs: If the records in the source have completely different _ids than those in the destination, the destination records remain unaffected. The new records from the source are simply inserted (appended) into the collection.
  2. Matching IDs: If a record in the source shares the exact same _id as a record in the destination, the source data will overwrite the destination record.

---

## Q: What happens if the Database already exists at the destination, but the Collection being migrated is new?
**A:** In this scenario, existing collections within the destination database remain untouched. The new collection from the source is fully copied to the destination database without affecting the integrity or availability of any other collections residing in that same database.

***Note:*** This applies only when the collection names differ. If the collection names match, the merge/overwrite logic described in the previous question applies.

---

## Q: What happens if the application crashes during the Full Load phase?
**A:** If docStreamer stops (crash, restart, power loss) during the Full Load, it resumes the migration from the point of the last completed collection. It does *not* start from scratch.

### Detailed Recovery Workflow

#### **The "Anchor" Checkpoint**
- Before copying any data, docStreamer saves an **Anchor Timestamp** (**T₀**) to the target database.
- If docStreamer crashes at 50% completion, this Anchor remains in the database, preserving the original start time of the migration project.

#### **Smart Resume Logic**
- **On Restart:** docStreamer detects the Anchor and recognizes a *Partial Full Load*.
- **Skipping Completed Work:** It scans the checkpoints to determine which collections finished successfully.
- **Resuming Work:** It launches workers only for the remaining (incomplete) collections.

#### **Consistency**
By reusing the original Anchor **T₀** instead of capturing a new timestamp, docStreamer ensures that when the CDC phase starts, it will “rewind” back to the very beginning of the project — capturing any updates that occurred on already-finished collections while docStreamer was down.

#### **How Idempotency Handles Interrupted Collections**
- For the collection being copied when the crash occurred, docStreamer restarts that collection from the **beginning**.
- **No duplicates:** Because workers use Upserts (`ReplaceOne` with `upsert: true`), re-copying simply overwrites documents with identical data. No duplicate errors or cleanup required.

---

## Q: How does the "Resumable Full Load" work?
**A:**
- If docStreamer stops during copying, simply run `start` again.
- docStreamer detects the existing Anchor Timestamp.
- It scans checkpoints to determine which collections completed.
- It skips completed collections and launches workers only for pending ones.
- After all collections finish, it proceeds to CDC using the original **T₀**.

---

## Q: I want to restart from scratch, not resume. How do I force a fresh start?
**A:** Two options:

1. **Use the Destroy Flag**  
   Run with `--destroy` (or `destroy: true` in config).  
   This drops target databases and clears checkpoints.

2. **Manual Cleanup**  
   Drop the `docStreamer` database in the target.  
   The next run will be treated as a new migration.

---

## Q: What are the DocumentDB retention requirements for resuming? (**CRITICAL**)
**A:** Configure **Change Stream Log Retention** to cover the entire duration from the very first start time (**T₀**) until Full Load finishes.

### Example Scenario
- Start Monday → runs 3 days
- Crashes Wednesday → resume and finish Thursday
- When CDC starts Thursday, it must rewind to **Monday (T₀)**

### Risk
If retention is short (e.g., 3 hours or 24 hours), earlier change events will have expired → migration fails due to missing history.

### Recommendation
Set `change_stream_log_retention_duration` to **7 days or longer** for large migrations.

---

## Q: Does resuming create duplicate data?
**A:** No. docStreamer uses **Idempotent Writes (Upserts)**.

- Completed collections are skipped entirely.
- If a worker crashed mid-collection, that collection restarts.
- Upserts overwrite identical data → no duplicates created.

---

## Q: Why doesn't docStreamer use MongoDB Transactions?
**A:** docStreamer is built for **high-throughput eventual consistency** using **Idempotent Writes**, not strict transactional atomicity.

### Key Points

#### **Idempotency > Rollbacks**
- Instead of rollback logic, docStreamer simply reprocesses batches after a crash.
- Upserts ensure re-copying data overwrites identical documents without errors.

#### **Full Load Phase**
- Highly parallel; cannot be wrapped in a single transaction.
- If a worker fails, re-copying is safe due to idempotency.

#### **CDC Phase**
- Uses key-based partitioning for parallel writes.
- Transactions would force serialization → huge performance loss.
- Transactions also have a **16MB modification limit**, which bulk loads frequently exceed.

### Worker Failure Example
1. Worker copies 10,000 docs → crashes.
2. Migration halts.
3. You restart docStreamer.
4. It restarts the collection from the beginning.
5. Upserts overwrite identical docs → no duplicates.

This design makes docStreamer **crash-safe**.

---

## Q: How does docStreamer handle `_id` fields with mixed data types?
**A:** docStreamer auto-detects mixed `_id` types and switches to **Linear Scan Mode** to guarantee consistency.

### Technical Breakdown

#### The Challenge
Databases like DocumentDB may produce unstable sorting for mixed `_id` types without secondary indexes, causing parallel range queries to skip documents.

#### Automatic Detection
During Discovery, docStreamer:
- Samples start/end/random documents
- Checks `_id` types
- Runs test queries

If inconsistency is detected → flagged as **Unsafe for Range Scan**.

#### Linear Scan Mode
- Uses **one worker**
- Reads documents in **natural order**
- Bypasses unstable sorting
- Guarantees no skipped records

---

## Q: Why does docStreamer sometimes log “Strategy Override: Switching to Linear Scan”?
**A:** It’s an automatic safeguard triggered when mixed-type `_id` fields could cause data loss.

### Scenario
- Mixed `_id` BSON types (String/ObjectId/Decimal128/etc.)
- No secondary index
- Range queries become unstable → “invisible data”

### Safety Checks
- Sampling (head/tail/random)
- Type verification
- Test range queries

If unsafe → linear scan is enforced.

### Why Linear Scan?
- Forces natural-order streaming from disk
- Avoids sort-based skips
- Slower, but 100% safe

---

## Q: How strictly does the validation engine check for data discrepancies?
**A:** Extremely strictly — field-by-field, deep equality, type-aware.

### Process
- Fetches full documents (no projection)
- Compares using `reflect.DeepEqual(src, dst)`
- Ensures types match (e.g., `1` vs `1.0` is a mismatch)

**Any difference = flagged mismatch**

---

## Q: Why does the validator report “Active write in progress” or “Skipped”?
**A:** To avoid false positives caused by CDC updates during validation.

### How It Works
- docStreamer tracks “in-flight” CDC writes.
- If validator reads a doc currently being updated, it defers the check.
- It retries after `retry_interval_ms`.

This ensures legitimate mismatches are not confused with race conditions.

---

## Q: Are indexes migrated, and when are they created?
**A:** Yes. Indexes are created on the target **before** data loading for each collection.

### Why Pre-Load Creation?
- Enforces integrity constraints (e.g., unique indexes) from the start
- Prevents corrupt or invalid data from entering the target

### Performance
- Slightly slower inserts due to index maintenance
- Guarantees target schema matches source

### Limitation
Indexes created on the source **after** migration starts (during CDC) are **not** auto-copied and must be applied manually.

---

### Adaptive Flow Control

**Q: Why do I need Flow Control? Can't I just increase the number of workers?**
**A:** Increasing workers speeds up data *reading*, but it can easily overwhelm your target database. If you push data faster than MongoDB can write it to disk, you risk:
1.  **Lock Contention:** Operations pile up, causing latency spikes for other applications using the cluster.
2.  **Memory Saturation:** MongoDB may consume all available RAM, leading to OOM (Out of Memory) crashes by the Operating System.
3.  **Replica Set Lag:** Secondary nodes may fall too far behind, risking data consistency or triggering elections.

Flow Control acts as an intelligent "brake," ensuring migration speed never exceeds the target's physical capacity.

**Q: What exactly happens when the destination is overloaded?**
**A:** When `docStreamer` detects that the target is stressed (high queue depth or memory usage):
1.  **Status Change:** The application enters a `PAUSED` state and logs a warning (e.g., `[WARN] THROTTLING PAUSED`).
2.  **Source Throttle:** It stops fetching new documents from DocumentDB immediately.
3.  **Connection Keep-Alive:** Existing connections remain open, but no new write operations are sent.
4.  **Auto-Resume:** The background monitor continues checking every second. As soon as the target's metrics drop below your configured thresholds, the migration automatically resumes exactly where it left off.

**Q: Does this work with Sharded Clusters?**
**A:** **Yes.** This is a Cluster-Aware feature.
* **Standard tools** often only monitor the `mongos` router, which usually reports "healthy" metrics (0 queues) even when backend shards are struggling.
* **docStreamer** automatically discovers your cluster topology and opens direct monitoring connections to **every backend shard**.
* **Protection:** If *any* single shard becomes overloaded, the entire migration pauses. This prevents a "hot shard" scenario where one specific shard causes a cluster-wide failure.

**Q: I see "targetQueuedOps: 0" in the status output. Is Flow Control working?**
**A:** **Yes, this is normal.** MongoDB is highly optimized. A value of `0` means your database is handling the current write load instantly without any backlog.
* **Healthy:** `0 - 10`
* **Warning:** `10 - 50` (Micro-bursts)
* **Critical:** `> 50` (Sustained saturation)
Flow Control only steps in when it sees the critical values you defined in `config.yaml`.

**Q: Will Flow Control slow down my migration?**
**A:** It might slightly extend the total duration, but it **prevents failure**.
* *Without Flow Control:* You might migrate 20% faster, but risk crashing the production database or forcing a restart due to errors.
* *With Flow Control:* You get the maximum *safe* speed your hardware can handle, with zero manual intervention required to prevent crashes.

---

## Q: What happens if the source environment is a Sharded Amazon DocumentDB cluster? Will docStreamer work?
**A:** docStreamer is designed to work with sharded source environments, as long as the connection details point to the cluster's router/endpoint (equivalent to a mongos instance in a MongoDB sharded cluster).

- Full Load: The initial data copy phase should work correctly. docStreamer connects to the cluster endpoint and issues standard find commands to discover and copy data. The router is responsible for distributing these queries across all shards and aggregating the results for a complete snapshot.

- CDC (Real-Time Replication): The continuous replication phase relies on the DocumentDB cluster supporting cluster-wide change streams. docStreamer explicitly uses and checks for this capability by running the watch operation against the main client, which is intended to capture changes across all databases and collections.

### Caveats and Current Limitations Regarding Sharding

While the architecture supports sharding, there are important caveats, particularly with Amazon DocumentDB:

- Reliance on Cluster-Wide Change Streams: The entire CDC pipeline is dependent on the source cluster endpoint providing a single, comprehensive cluster-wide change stream. If the DocumentDB sharding implementation does not fully support a robust cluster-wide stream, the CDC replication may be incomplete or fail.

- Target Sharding Complexity: If your target self-managed MongoDB instance is also sharded, DDL operations (like Drop or Rename) are handled by running commands against the target database. These operations can require specific administrative commands or special handling in a sharded MongoDB environment, which may not be fully optimized in the current DDL handler. We recommend not to run any DDL operations while the migration is on going to avoid issues.

- DocumentDB Sharding is a Planned Enhancement: While the core functionality is designed for sharding, fully testing and guaranteeing compatibility across all edge cases of DocumentDB's sharded architecture is a planned feature. For maximum safety, large-scale sharded DocumentDB migrations should be considered experimental until the dedicated Support for Sharded DocumentDB clusters enhancement is released.

---

## Q: Can I migrate from a non sharded DocumentDB cluster to a sharded MongoDB cluster?
**A:** Yes, please note that docStreamer does not perform sharding setup operations (such as enableSharding or shardCollection). You must configure the sharding topology manually before starting the migration.

Recommended Workflow:

1. Pre-Create and Shard: Before starting the migration, manually create your target databases and collections, and enable sharding with your desired shard keys.
2. Start Migration: When docStreamer starts, it will detect that the collections already exist and skip the creation step.
3. Data Loading: The tool will insert data through the mongos router, allowing MongoDB to automatically distribute the documents across shards based on your pre-configured setup.

---

## Q: Are new databases and collections migrated to destination if they were created on source while docStreamer is running?
**A:** Yes. New collections and databases created while docStreamer is running (and while it is paused), will be detected and migrated.

***Example***

Source:

```bash
rs0 [direct: primary] ind_1> show dbs
custom_ids  332.83 MiB
cvg_1        15.66 MiB
good_ids    300.77 MiB
ind_1       170.88 MiB
ind_2       148.76 MiB
ind_3       131.40 MiB
sea_1         8.05 MiB
sea_2         7.64 MiB
```

Destination:

```bash
[direct: mongos] ind_1> show dbs
admin            16.77 MiB
airline           2.93 GiB
config            7.84 MiB
custom_ids       19.31 MiB
cvg_1             2.14 MiB
docStreamer    1.09 MiB
generic         144.02 MiB
good_ids         18.36 MiB
ind_1            64.18 MiB
rental          638.08 MiB
sea_1             1.00 MiB
sea_2           956.00 KiB
```

Status:

```bash
./docStreamer status
--- docStreamer Status (Live) ---
PID: 1908370 (Querying http://localhost:8080/status)
{
    "ok": true,
    "state": "running",
    "info": "Change Data Capture",
    "timeSinceLastEventSeconds": 426.268916374,
    "cdcLagSeconds": 0,
    "totalEventsApplied": 165575,
    "validation": {
        "totalChecked": 160949,
        "validCount": 160949,
        "mismatchCount": 0,
        "syncPercent": 100,
        "lastValidatedAt": "2025-12-08T14:34:27Z"
    },
    "lastSourceEventTime": {
        "ts": "1765204339.125",
        "isoDate": "2025-12-08T14:32:19Z"
    },
    "lastAppliedEventTime": {
        "ts": "1765204339.125",
        "isoDate": "2025-12-08T14:32:19Z"
    },
    "lastBatchAppliedAt": "2025-12-08T14:33:44Z",
    "initialSync": {
        "completed": true,
        "completionLagSeconds": 43,
        "cloneCompleted": true,
        "estimatedCloneSizeBytes": 224711630,
        "clonedSizeBytes": 224711630,
        "estimatedCloneSizeHuman": "214.3 MB",
        "clonedSizeHuman": "214.3 MB"
    }
}
```


Create new database and insert record on source:

```bash
rs0 [direct: primary] test> use newDB
switched to db newDB
rs0 [direct: primary] newDB> db.test.insertMany([{"record":1},{"record":2},{"record":3}])
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('6936e457f345f4ceef5a6467'),
    '1': ObjectId('6936e457f345f4ceef5a6468'),
    '2': ObjectId('6936e457f345f4ceef5a6469')
  }
}
rs0 [direct: primary] newDB> db.test.find()
[
  { _id: ObjectId('6936e457f345f4ceef5a6467'), record: 1 },
  { _id: ObjectId('6936e457f345f4ceef5a6468'), record: 2 },
  { _id: ObjectId('6936e457f345f4ceef5a6469'), record: 3 }
]
```

CDC logs:

```bash
{"level":"info","message":"CDC batch applied","s":"cdc","batch_size":1,"elapsed_secs":0.127438667,"namespaces":["newDB.test"],"time":"2025-12-08 09:44:40.292"}
{"level":"info","message":"CDC batch applied","s":"cdc","batch_size":1,"elapsed_secs":0.148425636,"namespaces":["newDB.test"],"time":"2025-12-08 09:44:40.313"}
{"level":"info","message":"CDC batch applied","s":"cdc","batch_size":1,"elapsed_secs":0.148461117,"namespaces":["newDB.test"],"time":"2025-12-08 09:44:40.313"}
```

Status:

```bash
./docStreamer status
--- docStreamer Status (Live) ---
PID: 1908370 (Querying http://localhost:8080/status)
{
    "ok": true,
    "state": "running",
    "info": "Change Data Capture",
    "timeSinceLastEventSeconds": 63.377820916,
    "cdcLagSeconds": 0,
    "totalEventsApplied": 165578,
    "validation": {
        "totalChecked": 160952,
        "validCount": 160952,
        "mismatchCount": 0,
        "syncPercent": 100,
        "lastValidatedAt": "2025-12-08T14:44:40Z"
    },
    "lastSourceEventTime": {
        "ts": "1765205079.12",
        "isoDate": "2025-12-08T14:44:39Z"
    },
    "lastAppliedEventTime": {
        "ts": "1765205079.12",
        "isoDate": "2025-12-08T14:44:39Z"
    },
    "lastBatchAppliedAt": "2025-12-08T14:44:40Z",
    "initialSync": {
        "completed": true,
        "completionLagSeconds": 43,
        "cloneCompleted": true,
        "estimatedCloneSizeBytes": 224711630,
        "clonedSizeBytes": 224711630,
        "estimatedCloneSizeHuman": "214.3 MB",
        "clonedSizeHuman": "214.3 MB"
    }
}
```

Destination:

```bash
[direct: mongos] ind_1> show dbs
admin             16.76 MiB
airline            2.93 GiB
config             7.83 MiB
custom_ids        19.31 MiB
cvg_1              2.14 MiB
docStreamer  1000.00 KiB
generic          144.02 MiB
good_ids          18.36 MiB
ind_1             73.24 MiB
newDB             40.00 KiB
rental           638.08 MiB
sea_1            828.00 KiB
sea_2            928.00 KiB
[direct: mongos] ind_1> use newDB
switched to db newDB
[direct: mongos] newDB> db.test.find()
[
  { _id: ObjectId('6936e457f345f4ceef5a6469'), record: 3 },
  { _id: ObjectId('6936e457f345f4ceef5a6467'), record: 1 },
  { _id: ObjectId('6936e457f345f4ceef5a6468'), record: 2 }
]
[direct: mongos] newDB>
```

The observed retrieval order on the destination is expected behavior and is not a problem for data consistency.
The data is consistent and complete, but the default retrieval method in MongoDB does not guarantee the order of results unless you explicitly request a sort.
To see the documents on the destination in the order they were logically created (based on their _id), you must explicitly add a sort to your query:

```bash
[direct: mongos] newDB> db.test.find().sort({_id: 1})
[
  { _id: ObjectId('6936e457f345f4ceef5a6467'), record: 1 },
  { _id: ObjectId('6936e457f345f4ceef5a6468'), record: 2 },
  { _id: ObjectId('6936e457f345f4ceef5a6469'), record: 3 }
]
```