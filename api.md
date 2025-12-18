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