## How Percona docStreamer Works

docStreamer supports syncing all data, including the following index types:

- single  
- compound  
- multikey  
- sparse  
- unique  

docStreamer operates in 4 distinct stages:

- Validation  
- Discovery  
- Full Data Load  
- CDC (Continuous Sync)  

In addition to the above, docStreamer includes an Event-Driven Validation Engine. Unlike traditional migration tools that just "copy and hope," our tool verifies data integrity in real-time as changes are applied.

### 1. Validation

When you run `./docStreamer start`, docStreamer loads the config.yaml file, prompts for credentials, and connects to both the source DocumentDB and the target MongoDB to verify authentication and connectivity. If migration.destroy is set to true, it will prompt you for confirmation before proceeding.

docStreamer performs a critical check to ensure the DocumentDB Change Stream is enabled (modifyChangeStreams: 1). If not, it provides the specific command required to enable it. It also checks the target database for an existing CDC checkpoint. If no checkpoint is found, it proceeds to the "Discovery" phase.

### 2. Discovery

In this stage, docStreamer scans the source database to identify all valid databases and collections that match your configuration (filtering out system collections or those explicitly excluded). It gathers metadata, such as document counts and estimated sizes, to prepare for the Full Load.

### 3. Full Data Load

This is the initial snapshot stage.

**Global Time Capture ($\mathbf{T_0}$):** Before Discovery begins, docStreamer captures the source cluster's current Cluster Time. This timestamp ($\mathbf{T_0}$) becomes the guaranteed starting point for the CDC phase, ensuring strictly zero data loss.

**Parallel Execution:** A collection-level worker pool (configured via `migration.max_concurrent_workers`) orchestrates the migration. Inside each collection, data is processed concurrently by dedicated read and insert workers (`cloner.num_read_workers` and `cloner.num_insert_workers`) to maximize throughput.

**Strict Range Snapshots & CDC Hand-off:** Each worker copies documents using strict range queries ($gte Min AND $lte Max). This approach is critical for supporting collections with mixed data types in the _id field (e.g., Integers, Strings, and ObjectIds co-existing).
 - Strict Boundaries: By enforcing exact boundaries, we ensure that every document falls into exactly one worker's queue, preventing type-sorting errors or skipped records.
 - New Data Handling: Documents inserted after the initial discovery (beyond the "Max Key") are not picked up by the Full Load workers. Instead, they are captured by the CDC Change Stream, which starts from $\mathbf{T_0}$ (the beginning of the migration). This guarantees that no data is missed ("zero data loss") while maintaining strict type safety during the copy phase.

#### Automated Sharding Lifecycle

If a collection is defined in the sharding configuration, docStreamer automates the entire setup before the data copy begins. This ensures the target environment is ready for high-velocity parallel writes.

**1. Sharding Setup**
The tool automatically enables sharding on the target database and creates the required shard key index. It then executes the `shardCollection` command with your defined key and uniqueness constraints.

**2. Automated Pre-Splitting**
To prevent "hot shards" during the Full Load, docStreamer applies a pre-split strategy (e.g., `hex`, `composite_uuid_oid`, or `manual`) to create multiple initial chunks across the shard key space before any data is inserted.

**3. Round-Robin Distribution**
After chunks are created, docStreamer performs a manual distribution cycle. It identifies every shard in the target cluster and moves chunks in a round-robin fashion until the collection is perfectly balanced across all available shards.

### 4. CDC (Continuous Sync)

This phase starts immediately after the Full Load completes (or on startup if a valid checkpoint exists).

**Resume from $\mathbf{T_0}$:** The Change Stream is initialized using the Global $\mathbf{T_0}$ captured before the Full Load began. This ensures the stream covers the entire duration of the Full Load phase.

**Buffering & Flushing:** A processor listens for change events (inserts, updates, deletes, and supported DDLs). Events are buffered into a BulkWriter and flushed to the target in batches based on cdc.batch_size or cdc.batch_interval_ms.

**Idempotency:** Because the CDC stream overlaps with the Full Load (covering the same time period), it handles duplicate keys gracefully by using Upsert/Replace operations. This ensures that the target data always converges to the latest state from the source.

#### Optimized Index Management

docStreamer allows you to prioritize write speed during the Full Load phase by deferring secondary index creation.

* **Index Postponement**: If `cloner.postpone_index_creation` is enabled, only the `_id` and shard key indexes are created initially. This dramatically increases the write throughput of the Full Load workers.
* **Finalization Phase**: Secondary indexes are built on-demand. When the user issues the `docStreamer index` or `docStreamer finalize` command, the application queries the target collection, compares its indexes to the source collection, identifies any missing secondary indexes, and explicitly creates them.

### 5. Continuous Data Validation

CDC is guaranteed to sync the documents, however, docStreamer provides an additional layer of validation. The data validation process in docStreamer is event-driven. Each time a batch of records is written to the destination through CDC, the corresponding document IDs are immediately queued for validation. This process becomes active only after the Full Data Load has completed and CDC is running. To maintain performance while still providing this valuable functionality, the application stores counters (for statistics) and failure records (for debugging).

docStreamer also implements an auto-healing capability. If a record fails validation due to a mismatch but is later updated by the source application, docStreamer automatically removes the associated failure entry because the previous state is no longer relevant. While the validation process never modifies data, it allows you to confirm not only that data has been synchronized, but also that records continue to update correctly as your application remains active prior to cutover. This feature provides peace of mind and ensures that the source and destination datasets are an exact match before the final cutover.

Please keep in mind that in busy systems it is perfectly normal for some records to be temporarily flagged as invalid until CDC has applied the latest changes. This is expected behavior, particularly in heavily used environments where frequent updates—often to the same records—are common. 

#### Self-Healing & Auto-Retry

docStreamer includes active self-healing mechanisms to ensure statistics match reality:

* Startup Reconciliation: On every process start, docStreamer automatically scans the validation collections to fix any drift in statistics caused by previous crashes or race conditions.
* Idle Watchdog: A background process monitors the replication lag. When the system is idle (Lag $\approx$ 0s), it automatically re-queues known validation failures for a check. This clears out "false positives" that occurred because data was being updated during the initial validation check.

#### Validation Self-Healing & Metrics

The validation engine now distinguishes between different types of mismatches to provide a clearer picture of data health.

* **Found vs. Fixed**:
    * **Mismatch Found**: The count of unique documents that have failed a validation check at least once.
    * **Mismatch Fixed**: The count of documents that previously failed but were later confirmed as valid after a CDC update or a re-check.
* **Pending Mismatches**: This represents the actual number of active discrepancies currently stored in the metadata database.

#### Status

The status api provides the current validation status. You can see the live validation statistics by running the status command. Look for the validation block:

```bash
./docStreamer status
```

Output Example (Healthy) -- this is just the section pertinent to the validation:

```bash
....
.....
    "validation": {
        "queuedBatches": 0,
        "totalChecked": 3252342,
        "mismatchFound": 21604,
        "mismatchFixed": 21604,
        "pendingMismatches": 0,
        "hotKeysWaiting": 0,
        "syncPercent": 100,
        "lastValidatedAt": "2026-02-24T21:32:56Z"
    }
```

#### Dedicated Validation API

docStreamer also exposes the validator via its API (default port 8080) to interact with the validator manually. You can use the API to perform the following actions:

1. Check Specific Records

If you suspect specific documents are out of sync, you can check them as shown below, this will run synchronously and returns the result.

```bash
curl -X POST http://localhost:8080/validate \
     -H "Content-Type: application/json" \
     -d '{
           "namespace": "inventory.products",
           "ids": ["64f8a2b1c9e7", "64f8a2b1c9e8"]
         }'
```

Response:

```bash
[
    {
        "docId": "64f8a2b1c9e7",
        "status": "valid"
    },
    {
        "docId": "64f8a2b1c9e8",
        "status": "mismatch",
        "reason": "Field content differs"
    }
]
```

2. Check All Known Failures

If you have fixed an issue or suspect a transient error caused mismatches, you can ask the system to re-check all IDs flagged as a mismatch to find out if they have synced.

```bash
curl -X POST http://localhost:8080/validate/retry
```

Response:

```bash
{
    "status": "accepted",
    "message": "Queued re-validation check for 42 documents. This process updates status but does not repair data."
}
```

3. Get Raw Stats

You can retrieve raw counters for integration with external monitoring tools (Prometheus, Grafana, etc.).

```bash
curl http://localhost:8080/validate/stats
```

Response:

```json
[{"namespace":"lws_1.test_1","validCount":27880,"mismatchCount":0,"totalChecked":27880,"failureCapReached":false},{"namespace":"lws_1.test_2","validCount":24312,"mismatchCount":0,"totalChecked":24312,"failureCapReached":false},{"namespace":"lws_1.test_3","validCount":23695,"mismatchCount":0,"totalChecked":23695,"failureCapReached":false}]
```

4. Smart Reset (Default) 

Use this if you think stats are wrong ("Ghost Mismatches"). It sets mismatch_count to match the actual number of failure documents.

```bash
curl -X POST http://localhost:8080/validate/reset
```


5. Hard Reset (Erase)

Use this to wipe all validation stats to 0.

```bash
curl -X POST http://localhost:8080/validate/reset?mode=erase
```

## Resuming & Checkpointing

State is managed entirely within the target MongoDB cluster using specific documents in the `docStreamer.checkpoints` collection. docStreamer uses a smart resume strategy to handle crashes at any stage.

1. The Anchor Checkpoint (`migration_anchor_timestamp`)
  * Purpose: Saved immediately before the first Full Load attempt begins. It locks in the Global Start Time ($\mathbf{T_0}$).
  * Behavior: If docStreamer crashes during the Full Load, this document remains. On restart, docStreamer finds it, recognizes a "Partial Full Load," loads the list of already-completed collections, and resumes the migration for the remaining collections using this original timestamp.
  
2. The Global Checkpoint (`cdc_resume_timestamp`)
  * Purpose: Marks the successful completion of the entire Full Load phase or tracks progress during CDC.
  * Behavior: Once the Full Load finishes for all collections, the Anchor is deleted and replaced by this Global Checkpoint. On restart, if this exists, docStreamer skips the Full Load entirely and starts CDC.
  
### Resume Logic Summary

docStreamer determines the start state based on which checkpoints exist in the target database:

* **Scenario A (Global Checkpoint exists):**
    * **State:** Full Load is 100% complete.
    * **Action:** Skips Discovery and Full Load. Starts CDC immediately from the recorded timestamp.

* **Scenario B (Anchor Checkpoint exists):**
    * **State:** Full Load was started but crashed/stopped before finishing.
    * **Action:** Loads the original Start Time ($T_0$) from the Anchor. Filters out collections that were already completed. Resumes the Full Load for the remaining collections.

* **Scenario C (No Checkpoints):**
    * **State:** Fresh run.
    * **Action:** Cleans up any stale metadata. Captures a new $T_0$ and saves it as the Anchor. Starts the Full Load from scratch.

## Data Migration Strategy

docStreamer uses a mandatory two-phase approach—Full Load followed by Change Data Capture (CDC)—to achieve a non-blocking, consistent migration from Amazon DocumentDB.

### Phase 1: Full Load (Snapshot)

The Full Load phase is designed for throughput and efficiency. It does not attempt to create a consistent point-in-time snapshot on the destination (which would require locking the source). Instead, it copies data as fast as possible while the source remains live.

#### Handling Live Data and Consistency

Because the source is live, data on the destination will be logically inconsistent during this phase (e.g., a record might be updated on the source after the worker copied it). We solve this using two specific strategies:

#### The "Live Tailing" 

Standard segmentation logic often misses documents inserted "at the end" of a collection during a long-running copy. docStreamer uses dynamic segmentation where the final query for every collection is open-ended (`_id >= Last_Min`). This guarantees that we capture every document present at the moment the worker finishes.

#### Idempotent Writes (Upserts)

To prevent fatal errors during the transition to CDC, all writes in docStreamer are Idempotent.

- **Operation:** ReplaceOne with Upsert: true.  
- **Benefit:**  
  - If the Full Load copies a document, and the CDC stream later tries to apply an update to that same document, the operation succeeds.  
  - If the CDC stream tries to insert a document that the Full Load already copied, it simply replaces it with the authoritative version.

### Phase 2: Continuous Sync (CDC)

The CDC phase serves as the Source of Truth for restoring consistency. It replays history to bring the destination into eventual consistency with the source.

#### Global Time Capture ($\mathbf{T_0}$) & Anchor

Before Discovery begins, docStreamer captures the source cluster's current Cluster Time. This timestamp ($\mathbf{T_0}$) is immediately persisted to the target database as an "Anchor. This guarantees that if the Full Load process crashes and restarts multiple times, the starting point for the eventual CDC phase remains fixed at the very beginning of the migration. This ensures strictly zero data loss, even if the Full Load takes days to complete across multiple restarts.

#### Chronological Correction

DocumentDB Change Streams deliver operations in strict chronological order. By replaying this stream from $\mathbf{T_0}$, docStreamer corrects any inconsistencies left by the parallel Full Load.

#### Example: Out-of-Order Writes during Full Load

| Source Event | Time | Action                           | Final Result                           |
|--------------|------|----------------------------------|-----------------------------------------|
| Full Load    | T1   | Worker copies Doc A (v1).        | Target has Doc A (v1).                  |
| Source Update| T2   | App updates Doc A to (v2).       | Worker missed this (already passed).    |
| CDC Stream   | T3   | Stream reaches T2 event.         | Target updates Doc A to (v2).           |

The CDC phase guarantees that the destination eventually matches the source perfectly.

#### Determining the Global Checkpoint

The transition to CDC requires a single, cluster-wide moment in time—the Checkpoint—to start tracking changes from.

1. **Global Capture ($\mathbf{T_0}$):**  
    Before the Full Load phase begins for any collection, the application queries the source DocumentDB for its current cluster time (`$clusterTime`).

2. **Anchor Persistence:**  
    This timestamp ($\mathbf{T_0}$) is immediately saved to the `migration_anchor_timestamp` checkpoint in the target database. It serves as the fixed reference point for the duration of the Full Load.
    - If the load succeeds: The Anchor is promoted to the permanent `cdc_resume_timestamp` checkpoint.
    - If the load fails: The Anchor remains in the database, allowing docStreamer to find it on restart and resume exactly where it left off (using the original time) instead of starting over.

2. **Tailing Start:**  
    When the CDC phase begins (after the Full Load completes), it opens the Change Stream starting exactly at $\mathbf{T_0}$.

**Note:**  
This means the CDC stream will replay events that occurred during the Full Load. This is intentional and necessary to ensure that no data is missed ("zero data loss"), and the idempotent write logic handles these replayed events gracefully.

<details>
<summary>***DocumentDB exposes the following, which we can leverage for checkpointing***</summary>

```sql
rs0 [direct: primary] test> db.hello()
{
  hosts: [
    'docdb-********-test.-********-.docdb.amazonaws.com:27017',
    'docdb-********-test2.-********-.docdb.amazonaws.com:27017',
    'docdb-********-test3.-********-.docdb.amazonaws.com:27017'
  ],
  setName: 'rs0',
  secondary: false,
  primary: 'docdb-********-test.-********-.docdb.amazonaws.com:27017',
  me: 'docdb-********-test.-********-.docdb.amazonaws.com:27017',
  electionId: ObjectId('7fffffff0000000000000001'),
  readOnly: false,
  lastWrite: {
    opTime: { ts: Timestamp({ t: 1763486394, i: 2 }), t: Long('1') },
    lastWriteDate: ISODate('2025-11-18T17:19:54.000Z')
  },
  isWritablePrimary: true,
  maxBsonObjectSize: 16777216,
  maxMessageSizeBytes: 48000000,
  maxWriteBatchSize: 100000,
  localTime: ISODate('2025-11-18T17:19:54.000Z'),
  logicalSessionTimeoutMinutes: 30,
  maxWireVersion: 13,
  minWireVersion: 0,
  operationTime: Timestamp({ t: 1763486394, i: 2 }),
  ok: 1
}
```
</details>

#### Maintaining Chronological Order and Correction

DocumentDB Change Streams deliver operations in strict chronological order based on their OpTime. The CDC phase applies these ordered operations to override and correct the potentially inconsistent snapshot created by the parallel Full Load.

#### Scenario: Out-of-Order Writes

Consider a scenario where Document A is inserted (Insert 1 at TS_1), deleted (at TS_2), and then re-inserted (Insert 2 at TS_3)—all while the Full Load is running.

- Insert 1 at TS_1  
- Delete at TS_2  
- Insert 2 at TS_3  

| Source Event | Source OpTime | Full Load Action           | CDC Stream Action               | Final State             |
|--------------|----------------|-----------------------------|----------------------------------|--------------------------|
| Insert 1     | TS_1           | Worker copies "Doc A".      | Stream replays Insert 1 (Upsert).| Doc A exists.            |
| Delete       | TS_2           | Worker missed this.         | Stream replays Delete.           | Doc A deleted.           |
| Insert 2     | TS_3           | Worker missed this.         | Stream replays Insert 2 (Upsert).| Doc A exists (Correct).  |

Regardless of the state the Full Load left behind, the CDC process replays history from $\mathbf{T_0}$ in strict order. The final event (Insert 2) is always the last operation applied, ensuring the destination matches the source perfectly.

## Internal Workflow

Here is a more detailed but simple explanation of the inner workings of docStreamer for each of the 2 stages (Full Load and CDC).

### Full Sync (Load)

Think of the Full Sync as a massive "Divide and Conquer" operation. Instead of trying to read the entire database at once (which would be slow and memory-heavy), docStreamer splits the work into tiny, manageable chunks and processes them in a pipeline.

1. "Divide and Conquer" (Segmentation)\
  Before copying any data, docStreamer looks at the collection to figure out how to split it up:
    - Find the Boundaries: docStreamer asks the source for the very first _id (Minimum) and the very last _id (Maximum) currently in the collection.
    - Create Segments : docStreamer doesn't list all documents. Instead, it calculates logical ranges of _ids. For example, if you have 1 million documents and `cloner.segment_size_docs` is 10,000, it logically creates 100 "tickets" (segments).

2. The "Find" Logic (Read Workers)
  Several Read Workers run in parallel, grabbing those "tickets" and querying the source.
    - Strict Range Queries: A worker runs a specific range query for its segment using strict boundaries ($gte Min AND $lte Max).
    - Mixed Type Safety: By enforcing exact boundaries rather than using open-ended queries, the tool correctly handles collections where _id fields contain mixed data types (e.g., Integers, Strings, and ObjectIds).
    - New Data: Any documents inserted after the initial discovery (beyond the "Max Key") are not picked up by the Full Load workers. Instead, they are captured by the CDC Change Stream, which starts from $\mathbf{T_0}$ (the beginning of the migration). This guarantees zero data loss while ensuring strict type safety during the copy phase.

3. The Pipeline (Buffering)\
  The Read Workers don't write to MongoDB directly. They are just "fetchers." 
    - Read: A worker fetches a batch of documents (e.g., 1,000 at a time) from DocumentDB.
    - Pack: It then wraps these documents into a "write model" (instructions for MongoDB).
    - Queue: It then pushes this batch into a channel (a safe waiting line in memory).

4. Writing to Target (Insert Workers)\
  On the other side of the queue, Insert Workers are waiting to do the following:
    - Grab: They pick up a batch from the queue.
    - Write: They send the batch to MongoDB using BulkWrite.
    - Idempotency (Safety): They use ReplaceOne with Upsert: True.
      - Why not Insert? 
        - If you stop and restart the migration, or if the CDC phase tries to update a document you just copied, a standard "Insert" would fail with a "Duplicate Key Error."
      - Upsert strategy 
        - This tells MongoDB: "If this document exists, replace it with this version. If it doesn't exist, create it." This makes the process crash-proof and safe to retry.

### CDC

Think of CDC as "Live Tailing." Instead of reading static files, the docStreamer subscribes to a live news feed of every single action happening on the DocumentDB cluster and replays it on the destination.

1. The Starting Line "Time Zero" ($T_0$)\
  The most critical part of CDC is knowing where to start.
    - No Gaps
        - It doesn't start "now." It starts exactly at the timestamp captured before the Full Sync began.
    - The Overlap
        - This means it replays events that happened while the Full Sync was running. This intentional overlap ensures absolutely no data is missing.
        
2. The Watcher (The Listener)\
  This component connects to the Source (DocumentDB) and opens a Change Stream.
    - The Feed 
        - It tells DocumentDB: "Send me a notification for every Insert, Update, Delete, or Drop that happens after time $T_0$."
    - The Pipeline
      - As these notifications (events) arrive, the Watcher pushes them instantly into a high-speed internal queue.
        
3. The Processor (The Router)\
  A processor sits at the end of that queue and routes the events.
    - Consistent Hashing: Instead of a random pool, the processor calculates a hash of the document's _id.
    - Routing: Based on this hash, the event is assigned to a specific Worker Partition.
      - Example: All updates for "Doc A" are always routed to "Worker 1". All updates for "Doc B" go to "Worker 2".
      - Benefit: This guarantees that the timeline of changes for any single document remains strictly serial, preventing race conditions where a newer update overwrites an older one.
    - DDL Events (Schema Changes):
      - If a schema change occurs (e.g., Drop Collection), the processor pauses.
      - It flushes all partition buffers to ensure global consistency.
      - It executes the DDL and saves a checkpoint.

4. The Writers (Partitioned Execution)\
  We use Write Workers (configured by `cdc.max_write_workers`), but they operate independently.
    - Dedicated Buffers: Each worker has its own dedicated buffer. When a worker's buffer is full (or time elapses), it flushes only its assigned documents.
    - Ordered Writes: Writes are sent to MongoDB with ordered: true. This ensures that within a batch, Update 1 is applied before Update 2, maintaining strict chronological history.
    - Translation (Idempotency): The worker translates operations (Insert -> Replace/Upsert) to handle the overlap with the Full Load phase safely.
          
5. The Safety Net (Checkpoints)\
  While all this is happening, the docStreamer is constantly saving its place.
    - Tracking
      - Every time a batch is successfully applied, docStreamer updates its internal tracker.
    - Saving 
      - It periodically writes the timestamp of the last successful event to the `docStreamer.checkpoints` collection in MongoDB.
    - Crash Recovery 
      - If the server crashes or docStreamer is stopped, docStreamer restarts, reads that timestamp, and resumes the change stream from the exact millisecond it left off.
