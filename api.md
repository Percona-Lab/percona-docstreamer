# docStreamer API Reference

The **docStreamer API** provides endpoints for monitoring the migration status and managing on-demand data validation.  
All endpoints are hosted by the internal API server (default: **port 8080**, configurable via `config.yaml` or environment variables).

## 1. Status and Monitoring Endpoints

### 1.1. Get Application Status

Retrieves a detailed snapshot of the migration's current state, including full load progress, CDC lag, and high-level validation summaries.

| Detail | Value |
|--------|--------|
| Path | `/status` |
| Method | `GET` |
| Use Case | Health checks, dashboard integration, and general migration progress monitoring |

**Example**

```bash
curl -X GET http://localhost:8080/status
```

**Response Fields (Partial):**

- `ok`: `true` unless the state is error.  
- `state`: Current state (e.g., `running`, `starting`, `error`).  
- `info`: Detailed message about the state.  
- `cdcLagSeconds`: Delay between the source event time and the applied time on the target.  
- `initialSync.cloneCompleted`: `true` if the initial data load is finished.  

### 1.2. Trigger Deferred Index Creation

Triggers the background creation of any secondary indexes that were deferred during the Full Load phase (`postpone_index_creation: true`). This runs asynchronously, allowing the CDC stream to continue operating without interruption.

| Detail | Value |
|--------|--------|
| Path | `/index` |
| Method | `POST` |
| Mandatory Prerequisite | Full Load (Clone) must be completed. |

**Example**
```bash
curl -X POST http://localhost:8080/index
```

### 1.3. Finalize Migration

Initiates a graceful shutdown of the CDC stream. It waits for the stream to safely drain all in-flight operations, builds any deferred secondary indexes, and permanently marks the migration as "Finalized" in the metadata.

| Detail | Value |
|--------|--------|
| Path | `/finalize` |
| Method | `POST` |
| Mandatory Prerequisite | Full Load (Clone) must be completed. |

**Example**
```bash
curl -X POST http://localhost:8080/finalize
```

## 2. Validation Endpoints

Endpoints for managing the data consistency check process.


### 2.1. Ad-Hoc On-Demand Validation

Performs a synchronous, real-time comparison for a specific set of documents between the source and target databases.

| Detail | Value |
|--------|--------|
| Path | `/validate/adhoc` |
| Method | `POST` |
| Mandatory Prerequisite | Full Load (Clone) must be completed — otherwise returns **403 Forbidden** |
| Use Case | Manually verifying consistency of key documents after a known operation or error |

**Request Body**

| Field | Type | Description |
|-------|------|-------------|
| `namespace` | string | The collection namespace (e.g., `my_db.users`) |
| `ids` | array<string> | List of document `_id` values to check |

**Example**

```bash    
    curl -X POST http://localhost:8080/validate/adhoc \
      -H "Content-Type: application/json" \
      -d '{
        "namespace": "ecommerce.orders",
        "ids": [
            "ObjectId(\"656e18471f468903c18b63a6\")",
            "order_12345"
        ]
    }'
```

**Response Format**

Returns an array of `ValidationResult` objects:

```bash
    [
      {
        "DocID": "order_12345",
        "Status": "mismatch",
        "Reason": "Field content differs"
      }
    ]
```

### 2.2. Get Validation Stats

Retrieves the current aggregated counters for the entire validation process.

| Detail | Value |
|--------|--------|
| Path | `/validate/stats` |
| Method | `GET` |
| Use Case | Monitoring validation throughput and cumulative mismatch counts |

**Example**

```bash    
curl -X GET http://localhost:8080/validate/stats
```

**Response Format (Simplified)**

```bash
    [
      {
        "namespace": "db.collection",
        "totalChecked": 100000,
        "validCount": 99990,
        "mismatchCount": 10,
        "lastValidatedAt": "2025-12-05T18:00:00Z"
      }
    ]
```

### 2.3. Retry All Failures

Triggers a background job to re-validate every document currently recorded as a failure.

| Detail | Value |
|--------|--------|
| Path | `/validate/retry` |
| Method | `POST` |
| Use Case | Recalculate mismatch status after cleaning or patching data |

**Example**
    
```bash
curl -X POST http://localhost:8080/validate/retry
```

**Response Format**

```bash
    {
      "status": "accepted",
      "message": "Queued re-validation check for 15 documents. This process updates status but does not repair data."
    }
```

### 2.4. Reset Validation State

Resets or reconciles validation statistics and failure logs.

| Detail | Value |
|--------|--------|
| Path | `/validate/reset` |
| Method | `POST` |
| Use Case | Correcting drift in validation counters or clearing all validation history |

#### Modes

| Mode | Query Parameter | Description |
|------|----------------|-------------|
| Reconcile (Default) | *(none)* | Re-calculates cumulative stats based on the current count of records in `validation_failures`. |
| Erase (Hard Reset) | `?mode=erase` | Wipes all validation stats and failure records from the metadata DB. |

**Example (Default – Reconcile)**

```bash    
curl -X POST http://localhost:8080/validate/reset
```

**Example (Hard Reset)**

```bash    
curl -X POST http://localhost:8080/validate/reset?mode=erase
```

## 3. Expanded Status Response

The `/status` endpoint now provides granular metrics for CDC operations and the Flow Control system.

**New Response Fields:**
- `flowControl`:
    - `isPaused`: Boolean indicating if docStreamer is currently throttling writes.
    - `pauseReason`: The specific threshold (Queued Ops or Resident MB) that triggered the pause.
    - `currentQueuedOps`: The highest global lock queue depth detected across the cluster.
- `insertedDocs` / `updatedDocs` / `deletedDocs`: Individual counters for each operation type applied during CDC.
- `validation.pendingMismatches`: The real-time count of document IDs currently flagged as out of sync.
- `migrationFinalized`: `true` if the migration has been successfully finalized.
- `indexing`: An object tracking the live progress of index creation (`isIndexing`, `currentNamespace`, `completedCollections`, `totalCollections`, `completed`).

---

## 4. Advanced Validation Endpoints

### 4.1. Get Active Failures
Retrieves a list of all document IDs currently recorded as validation mismatches.

| Detail | Value |
| :--- | :--- |
| Path | `/validate/failures` |
| Method | `GET` |

```bash
# Example Request
curl -X GET http://localhost:8080/validate/failures
```

### 4.2. Get Queue Status
Returns real-time metrics on the internal validation buffer and its backpressure state.

| Detail | Value |
| :--- | :--- |
| Path | `/validate/queue` |
| Method | `GET` |

```bash
# Example Response
{
    "queue_used": 150,
    "queue_capacity": 2000,
    "is_throttled": false,
    "status_message": "Healthy"
}
```

### 4.3. Trigger Collection Scan
Triggers a full background scan of a collection to find mismatches or "orphans" (records that exist on the target but not the source).

| Detail | Value |
| :--- | :--- |
| Path | `/validate/scan` |
| Method | `POST` |

**Request Body:**
- `namespace`: The collection to scan (e.g., `db.users`).
- `scan_type`: `"source"` (Standard check) or `"orphans"` (Finds extra records on target).

```bash
# Example: Orphan Scan
curl -X POST http://localhost:8080/validate/scan \
     -H "Content-Type: application/json" \
     -d '{
           "namespace": "inventory.products",
           "scan_type": "orphans"
         }'
```

### 4.4. Complex Shard Key Validation
The `/validate/adhoc` endpoint now supports complex shard keys via the `keys` array.

```bash
# Example: Validating by Shard Key
curl -X POST http://localhost:8080/validate/adhoc \
     -H "Content-Type: application/json" \
     -d '{
           "namespace": "ecommerce.orders",
           "keys": [
               { "_id": "order_123", "region": "US" },
               { "_id": "order_456", "region": "EU" }
           ]
         }'
```