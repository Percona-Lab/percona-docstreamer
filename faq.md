# Frequently Asked Questions

## Q: What happens if the application crashes during the Full Load phase?
**A:** If docMongoStream stops (crash, restart, power loss) during the Full Load, it resumes the migration from the point of the last completed collection. It does *not* start from scratch.

### Detailed Recovery Workflow

#### **The "Anchor" Checkpoint**
- Before copying any data, the tool saves an **Anchor Timestamp** (**T₀**) to the target database.
- If the tool crashes at 50% completion, this Anchor remains in the database, preserving the original start time of the migration project.

#### **Smart Resume Logic**
- **On Restart:** The tool detects the Anchor and recognizes a *Partial Full Load*.
- **Skipping Completed Work:** It scans the checkpoints to determine which collections finished successfully.
- **Resuming Work:** It launches workers only for the remaining (incomplete) collections.

#### **Consistency**
By reusing the original Anchor **T₀** instead of capturing a new timestamp, the tool ensures that when the CDC phase starts, it will “rewind” back to the very beginning of the project — capturing any updates that occurred on already-finished collections while the tool was down.

#### **How Idempotency Handles Interrupted Collections**
- For the collection being copied when the crash occurred, the tool restarts that collection from the **beginning**.
- **No duplicates:** Because workers use Upserts (`ReplaceOne` with `upsert: true`), re-copying simply overwrites documents with identical data. No duplicate errors or cleanup required.

---

## Q: How does the "Resumable Full Load" work?
**A:**
- If docMongoStream stops during copying, simply run `start` again.
- The tool detects the existing Anchor Timestamp.
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
   Drop the `docMongoStream` database in the target.  
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
**A:** No. The tool uses **Idempotent Writes (Upserts)**.

- Completed collections are skipped entirely.
- If a worker crashed mid-collection, that collection restarts.
- Upserts overwrite identical data → no duplicates created.

---

## Q: Why doesn't the tool use MongoDB Transactions?
**A:** docMongoStream is built for **high-throughput eventual consistency** using **Idempotent Writes**, not strict transactional atomicity.

### Key Points

#### **Idempotency > Rollbacks**
- Instead of rollback logic, the tool simply reprocesses batches after a crash.
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
3. You restart the tool.
4. It restarts the collection from the beginning.
5. Upserts overwrite identical docs → no duplicates.

This design makes the tool **crash-safe**.

---

## Q: How does the tool handle `_id` fields with mixed data types?
**A:** docMongoStream auto-detects mixed `_id` types and switches to **Linear Scan Mode** to guarantee consistency.

### Technical Breakdown

#### The Challenge
Databases like DocumentDB may produce unstable sorting for mixed `_id` types without secondary indexes, causing parallel range queries to skip documents.

#### Automatic Detection
During Discovery, the tool:
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

## Q: Why does docMongoStream sometimes log “Strategy Override: Switching to Linear Scan”?
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
- The tool tracks “in-flight” CDC writes.
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

