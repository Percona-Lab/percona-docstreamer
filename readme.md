# docMongoStream: Migration & Sync Tool

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
________            ______  ___                                ____________                               
___  __ \______________   |/  /___________________ ______      __  ___/_  /__________________ _______ ___ 
__  / / /  __ \  ___/_  /|_/ /_  __ \_  __ \_  __ `/  __ \     _____ \_  __/_  ___/  _ \  __ `/_  __ `__ \
_  /_/ // /_/ / /__ _  /  / / / /_/ /  / / /  /_/ // /_/ /     ____/ // /_ _  /   /  __/ /_/ /_  / / / / /
/_____/ \____/\___/ /_/  /_/  \____//_/ /_/_\__, / \____/      /____/ \__/ /_/    \___/\__,_/ /_/ /_/ /_/ 
                                           /____/      
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 

docMongoStream automates the complete, end-to-end migration from an Amazon DocumentDB cluster to any self-managed MongoDB instance. It is a high-performance tool written in Go for performing a full data load and continuous data capture (CDC) migrations. It provides a resilient, two-phase migration process:

* Full Sync: A parallelized, high-speed copy of all existing data from source collections.
* Continuous Sync (CDC): Opens a change stream on the source DocumentDB to capture all inserts, updates, deletes and DDLs (with a few exceptions), applying them in batches to the target MongoDB for real-time synchronization.

## Prerequisites

### DocumentDB Pre-Setup

You MUST enable change streams on your source DocumentDB cluster, change streams might not be enabled by default. To enable them, you must modify your DocumentDB cluster parameter group: 

1. Find your Cluster Parameter Group:
    * Go to the AWS DocumentDB console.
    * Click on your cluster's name.
    * On the "Configuration" tab, find the "Cluster parameter group" 

2. Modify the Parameter Group:
    * Go to the "Parameter groups" section in the DocumentDB console.
    * Click on the parameter group your cluster is using.
    * Select the change_stream_log_retention_duration parameter and click "Modify".
    * Set the "Value" to the desired retention time. AWS recommends 24 hours or more. This value is in milliseconds.
        * 24 hours: 86400000
        * 7 days: 604800000
    * Save the changes.

![img](./documentdb_parameter_groups.png)

| Database | Action Required | Command (Mongo Shell) |
| :--- | :--- | :--- |
| **DocumentDB (Source)** | **A. Download CA Certificate** | Go to your AWS account and download the CA Cert |
| **Source and Target** | **B. Ensure both are running and you are able to connect** | Ensure the service is active and listening on the configured IP/port. Make sure any firewall rules have also been configured accordingly and you are able to connect to both clusters from the host running docMongoStream |
| **DocumentDB (Source)** | **C. ENABLE CHANGE STREAMS IN DOCUMENTDB** | This might need to be done for each collection depending on your use case. |

You can obtain the DocumentDB AWS CA cert for your cluster by going to the AWS console and browsing to DocumentDB --> Clusters --> <cluster_name_here> and then click on the `Connectivity & Security` tab (sample screenshot below). This is also where you need to gather your DocumentDB URI, in order to configure it in the `config.yaml`.

![docdb](docdb.png)

#### How to enable change streams in DocumentDB

docMongoStream was designed to run these checks for you automatically, but it does not make these changes for you. 
DocumentDB requires change streams to be explicitly enabled. Once you have modified the parameter groups accordingly you can then proceed to enabling the Cluster Stream. Connect to your DocumentDB cluster via a mongo shell and run the command below to enable the cluster-wide stream in your DocumentDB cluster:

```bash
use admin;
db.adminCommand({
  modifyChangeStreams: 1,
  database: "",
  collection: "",
  enable: true
});
```

The command below would only enable them for the 3 collections shown:

```bash
use admin;
db.adminCommand({modifyChangeStreams: 1, database: "percona_db_1", collection: "test_1", enable: true});
db.adminCommand({modifyChangeStreams: 1, database: "percona_db_1", collection: "test_2", enable: true});
db.adminCommand({modifyChangeStreams: 1, database: "percona_db_1", collection: "test_3", enable: true});
```

## Installing docMongoStream

### The easy way

All you need to do is follow 3 steps:

1. Download the binary (compiled for linux amd64) from the [bin](./bin) folder
2. Download the [config.yaml](./config.yaml) 
3. Follow the rest of the instructions below

### The hard way

You might want to compile docMongoStream for a different architecture (not tested) other than linux, so in order to do that you will just need to follow a few steps:

1. Clone this repo
2. Make whatever changes to the application you want (not required)
3. Build it for your specific architecture, examples below:

Build for linux

```bash
GOOS=linux GOARCH=amd64 go build -o ./bin/docMongoStream ./cmd/docMongoStream/
```

Build for your current OS and Architecture
```bash
go build -o ./bin/docMongoStream ./cmd/docMongoStream/
```

## Configure Users

You need to create users in both source and destination environments, you can name them whatever you like:

***Source (DocumentDB)***

```sql
db.getSiblingDB('admin').createUser({
  user: 'streamer',
  pwd: 'superSecretPassword',
  roles: ['clusterMonitor', 'readAnyDatabase']
});
```

***Destination (MongoDB)***

```sql
db.getSiblingDB('admin').createUser({
   user: 'streamer',
   pwd: 'superSecretPassword',
   roles: ['restore', 'clusterMonitor', 'clusterManager','readWriteAnyDatabase','dbAdminAnyDatabase']
  });
```  

## Configuring docMongoStream

The application is configured via the [config.yaml](./config.yaml) file in the application's root directory. You will need to at the very least edit the source and destination parameters. 

```yaml
# Source DocumentDB
docdb:
  endpoint: "localhost"
  port: "7777"
  ca_file: "/home/daniel.almeida/global-bundle.pem"
  # If true, tlsAllowInvalidHostnames=true will be added to the connection string.
  tls_allow_invalid_hostnames: true
  extra_params: ""

# Target MongoDB
mongo:
  endpoint: "dan-ps-lab-mongos00.tp.int.percona.com"
  port: "27017"
  ca_file: ""
  tls_allow_invalid_hostnames: true 
  extra_params: ""
```  

docMongoStream configuration options are self explanatory and documented within the configuration file itself. The only parameters you have to pass to the application at runtime are the usernames for the source and destination environments, the passwords for each are interactive and you will be prompted for it accordingly. You can also configure environment variables so you don't have to type them if you prefer, the choice is yours.

### Credentials

Credentials for the source and target databases are required. They can be provided in three ways, in order of priority:

1. Flags (Highest Priority):
    * `--docdb-user <user>`
    * `--mongo-user <user>`

2. Environment Variables:
    * `MIGRATION_DOCDB_USER`: Username for the source DocumentDB.
    * `MIGRATION_DOCDB_PASS`: Password for the source DocumentDB user.
    * `MIGRATION_MONGO_USER`: Username for the target MongoDB.    
    * `MIGRATION_MONGO_PASS`: Password for the target MongoDB user.

    ***Note about env vars*** You can change the prefix (`MIGRATION`) by setting `migration.env_prefix` in the `config.yaml`.

3. Interactive Prompt (Lowest Priority):
    * If passwords (`MIGRATION_DOCDB_PASS`, `MIGRATION_MONGO_PASS`) are not set as environment variables, the start command will securely prompt you to enter them. This is the recommended approach. You can not provide passwords as command line arguments for security purposes.

```bash
./docMongoStream help
docMongoStream is a tool for performing a full load and continuous data
capture (CDC) migration from AWS DocumentDB to MongoDB.

Usage:
  docMongoStream [command]

Available Commands:
  help        Help about any command
  restart     Restarts the application
  start       Starts the full load and CDC migration
  status      Checks and prints the current status of the migration
  stop        Finds the running application and stops it

Flags:
      --docdb-user string   Source DocumentDB Username
  -h, --help                help for docMongoStream
      --mongo-user string   Target MongoDB Username

Use "docMongoStream [command] --help" for more information about a command.
```

### Customization

docMongoStream can fully synchronize the source and destination clusters, and it also allows you to configure some aspects of the migration through its config.yaml file.

1. Exclude databases

You are able to exclude entire databases (the ones below are recommended and should not be synced). You can add any other database to the list below and it will be skipped:

```yaml
  exclude_dbs:
    - "admin"
    - "local"
    - "config"
```

2. Exclude collections

You can exclude specific collections from the migration; however, if you intend to skip all collections within a particular database, use exclude_dbs instead.
Use the format "dbname.collname", and separate multiple entries with commas or in separate lines as shown below.

Do not exclude any collections:

```yaml
exclude_collections: []  
```

Exclude some collections:

```yaml
  exclude_collections:
    - "dbnamehere.collection1"
    - "anotherdbhere.collection3"
    - "somedb.collection2"
```

3. Destroy destination databases

Set this value to true if you want to restart the migration from scratch. Doing so will drop all databases and collections in the destination environment except for the admin, local, and config databases, as well as any other databases that were not originally synchronized from the source. In other words, if a database does not exist on the source, it will not be dropped.

```yaml
destroy: False
```

4. Dry run mode

Set the following to True if you do not want to make any changes and just want to perform the initial validation process.

```yaml
dry_run: False
```

5. Additional configuration

You can modify any configuration through the [config.yaml](./config.yaml) file, including log locations and performance-related parameters. All options are clearly documented, and you are free to adjust them as needed.

## How to Use docMongoStream

docMongoStream runs as a background process that is controlled through a small set of simple commands, making its operation straightforward. After updating the configuration file to match your environment, you can execute the appropriate commands for each specific use case as shown below.

In general, the data-migration workflow from source to destination follows these steps:

1. Configure docMongoStream as explained above
2. Run `docMongoStream start`
3. When ready to cutover, run `docMongoStream stop`

### Start

The start command can be used to start a brand new migration and to resume a migration that has been stopped. docMongoStream will check if a full migration has already completed and it will resume from the last checkpoint.  

```bash
./docMongoStream start --docdb-user=your_docdb_user --mongo-user=your_mongo_user
```
<details>
<summary>Sample output:</summary>

```bash
2025/11/20 19:14:44 [INFO] CDC Operations logger initialized (file: logs/cdc.log)
2025/11/20 19:14:44 [INFO] Full Load logger initialized (file: logs/full_load.log)
2025/11/20 19:14:44
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
________            ______  ___                                ____________
___  __ \______________   |/  /___________________ ______      __  ___/_  /__________________ _______ ___
__  / / /  __ \  ___/_  /|_/ /_  __ \_  __ \_  __ `/  __ \     _____ \_  __/_  ___/  _ \  __ `/_  __ `__ \
_  /_/ // /_/ / /__ _  /  / / / /_/ /  / / /  /_/ // /_/ /     ____/ // /_ _  /   /  __/ /_/ /_  / / / / /
/_____/ \____/\___/ /_/  /_/  \____//_/ /_/\__, / \____/      /____/ \__/ /_/    \___/\__,_/ /_/ /_/ /_/
                                           /____/
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

2025/11/20 19:14:44 --- docMongoStream Application Start ---
2025/11/20 19:14:44
--- Phase 1: VALIDATION ---
2025/11/20 19:14:44 [TASK] Connecting to source DocumentDB...
2025/11/20 19:14:44 [TASK] Connecting to target MongoDB...
2025/11/20 19:14:45 [OK]   Connections successful.
2025/11/20 19:14:45 [TASK] Validating DocumentDB Change Stream configuration...
2025/11/20 19:14:45 [INFO] [VALIDATE] Running $listChangeStreams on admin DB...
2025/11/20 19:14:45 [INFO] [VALIDATE] Found 15 enabled change streams:
2025/11/20 19:14:45 [INFO] [VALIDATE]   - my_awesome_app.*
2025/11/20 19:14:45 [INFO] [VALIDATE]   - my_awesome_app.users
2025/11/20 19:14:45 [INFO] [VALIDATE]   - percona_db_1.test_1
2025/11/20 19:14:45 [INFO] [VALIDATE]   - percona_db_1.test_5
2025/11/20 19:14:45 [INFO] [VALIDATE]   - percona_db_1.test_2
2025/11/20 19:14:45 [INFO] [VALIDATE]   - percona_db_1.test_3
2025/11/20 19:14:45 [INFO] [VALIDATE]   - percona_db_1.test_4
2025/11/20 19:14:45 [INFO] [VALIDATE]   - percona_db_2.test_1
2025/11/20 19:14:45 [INFO] [VALIDATE]   - percona_db_2.test_2
2025/11/20 19:14:45 [INFO] [VALIDATE]   - percona_db_2.test_3
2025/11/20 19:14:45 [INFO] [VALIDATE]   - percona_db_2.test_4
2025/11/20 19:14:45 [INFO] [VALIDATE]   - percona_db_2.test_5
2025/11/20 19:14:45 [INFO] [VALIDATE]   - tobeignored.skipme_1
2025/11/20 19:14:45 [INFO] [VALIDATE]   - alpha.test_1
2025/11/20 19:14:45 [INFO] [VALIDATE]   - CLUSTER_WIDE (*.*)
2025/11/20 19:14:45 [OK]   DocumentDB Change Stream configuration is valid.
2025/11/20 19:14:45
--- Phase 2: LAUNCHING BACKGROUND PROCESS ---
2025/11/20 19:14:45 [OK]   Application started in background with PID: 1841063
2025/11/20 19:14:45 [INFO] CDC Operations logger initialized (file: logs/cdc.log)
2025/11/20 19:14:45 [INFO] Full Load logger initialized (file: logs/full_load.log)
2025/11/20 19:14:45 [INFO] Writing PID 1841063 to ./docMongoStream.pid
2025/11/20 19:14:45 [INFO] Status manager initialized (collection: docMongoStream.status)
2025/11/20 19:14:45 [INFO] [STATUS] State changed to: connecting (Connections established. Pinging...)
2025/11/20 19:14:45 [INFO] Starting status HTTP server on :8080/status
2025/11/20 19:14:46 [INFO] Checkpoint manager initialized (collection: docMongoStream.checkpoints)
2025/11/20 19:14:46 [INFO] [CDC cdc_resume_timestamp] Found resume timestamp: {1763682329 1002}
2025/11/20 19:14:46
--- Phase 1: DISCOVERY (SKIPPED) ---
2025/11/20 19:14:46
--- Phase 2: FULL DATA LOAD (SKIPPED) ---
2025/11/20 19:14:46 [INFO] Resuming CDC from global checkpoint: {1763682329 1002}
2025/11/20 19:14:46
--- Phase 3: CONTINUOUS SYNC (CDC) ---
2025/11/20 19:14:46 [INFO] [STATUS] State changed to: running (Change Data Capture)
2025/11/20 19:14:46 [INFO] [CDC cdc_resume_timestamp] Found resume timestamp: {1763682329 1002}
2025/11/20 17:59:58 [TASK] [lws_1.test_4] Processed batch: 1000 inserted, 0 replaced. (310 kB) in 563.608945ms
2025/11/20 19:14:46 [INFO] [CDC] Resuming event count from 440000
2025/11/20 19:14:46 [INFO] Starting cluster-wide CDC... Resuming from checkpoint: {1763682329 1002}
2025/11/20 19:14:46 [INFO] [CDC] Starting 4 concurrent write workers...
2025/11/20 19:14:46 [TASK] [CDC] Starting cluster-wide change stream watcher...
```
</details>

### Stop

```bash
./docMongoStream stop
```

<details>
<summary>Sample Output:</summary>

```bash
2025/11/20 21:55:16
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
________            ______  ___                                ____________
___  __ \______________   |/  /___________________ ______      __  ___/_  /__________________ _______ ___
__  / / /  __ \  ___/_  /|_/ /_  __ \_  __ \_  __ `/  __ \     _____ \_  __/_  ___/  _ \  __ `/_  __ `__ \
_  /_/ // /_/ / /__ _  /  / / / /_/ /  / / /  /_/ // /_/ /     ____/ // /_ _  /   /  __/ /_/ /_  / / / / /
/_____/ \____/\___/ /_/  /_/  \____//_/ /_/\__, / \____/      /____/ \__/ /_/    \___/\__,_/ /_/ /_/ /_/
                                           /____/
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

2025/11/20 21:55:16
--- Phase X: STOPPING APPLICATION ---
2025/11/20 21:55:16 [OK]   Stop signal sent.
2025/11/20 21:55:16 [OK]   Stop signal sent.
2025/11/20 21:55:16 [WARN] Received signal: terminated. Initiating graceful shutdown...
2025/11/20 21:55:16 [INFO] !!! PLEASE WAIT: Flushing final CDC batches to destination...
2025/11/20 21:55:16 [INFO] !!! DO NOT FORCE QUIT (Ctrl+C), or data may be lost.
2025/11/20 21:55:16 [INFO] [STATUS] State changed to: stopping (Flushing pending events... Please wait.)
2025/11/20 21:55:16 [INFO] [CDC] Processor shutting down. Flushing final batch...
2025/11/20 21:55:16 [INFO] [Worker 1] Shutting down.
2025/11/20 21:55:16 [INFO] [Worker 2] Shutting down.
2025/11/20 21:55:16 [INFO] [Worker 3] Shutting down.
2025/11/20 21:55:16 [INFO] [Worker 0] Shutting down.
2025/11/20 21:55:16 [INFO] [CDC] Watcher stopped. Waiting for processor to finalize...
2025/11/20 21:55:16 [INFO] [CDC cdc_resume_timestamp] Found resume timestamp: {1763682329 1002}
2025/11/20 21:55:16 [INFO] [CDC] Processor finalized. Shutdown complete.
2025/11/20 21:55:16 [INFO] CDC process stopped. Exiting.
```

</details>

### Restart

You can use this command when you need to apply configuration changes and then restart the existing migration. This is particularly useful after making optimization adjustments to ensure the migration reloads and restarts with the updated settings.

```bash
./docMongoStream restart
```

<details>
<summary>Sample output:</summary>

```bash
./docMongoStream restart
2025/11/24 22:22:42 [INFO] CDC Operations logger initialized (file: logs/cdc.log)
2025/11/24 22:22:42 [INFO] Full Load logger initialized (file: logs/full_load.log)
2025/11/24 22:22:42
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
________            ______  ___                                ____________
___  __ \______________   |/  /___________________ ______      __  ___/_  /__________________ _______ ___
__  / / /  __ \  ___/_  /|_/ /_  __ \_  __ \_  __ `/  __ \     _____ \_  __/_  ___/  _ \  __ `/_  __ `__ \
_  /_/ // /_/ / /__ _  /  / / / /_/ /  / / /  /_/ // /_/ /     ____/ // /_ _  /   /  __/ /_/ /_  / / / / /
/_____/ \____/\___/ /_/  /_/  \____//_/ /_/\__, / \____/      /____/ \__/ /_/    \___/\__,_/ /_/ /_/ /_/
                                           /____/
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

2025/11/24 22:22:42
--- Phase X: STOPPING FOR RESTART ---
2025/11/24 22:22:42 [OK]   Stop signal sent.
2025/11/24 22:22:42 [OK]   Stop signal sent.
2025/11/24 22:22:42 [WARN] Received signal: terminated. Initiating graceful shutdown...
2025/11/24 22:22:42 [INFO] !!! PLEASE WAIT: Flushing final CDC batches to destination...
2025/11/24 22:22:42 [INFO] !!! DO NOT FORCE QUIT (Ctrl+C), or data may be lost.
2025/11/24 22:22:42 [INFO] [STATUS] State changed to: stopping (Flushing pending events... Please wait.)
2025/11/24 22:22:42 [INFO] Stopping API Server...
2025/11/24 22:22:42 [INFO] [CDC] Watcher stopped. Waiting for processor to finalize...
2025/11/24 22:22:42 [INFO] [CDC cdc_resume_timestamp] Found resume timestamp: {1764038958 3}
2025/11/24 22:22:42 [INFO] [VAL] Shutting down validation workers...
2025/11/24 22:22:42 [INFO] [VAL] Validation workers stopped.
2025/11/24 22:22:42 [INFO] CDC process stopped. Exiting.
2025/11/24 22:22:42 [OK]   Application stopped.
2025/11/24 22:22:43
--- Phase 1: VALIDATION ---
2025/11/24 22:22:43 [TASK] Connecting to source DocumentDB...
2025/11/24 22:22:44 [TASK] Connecting to target MongoDB...
2025/11/24 22:22:44 [OK]   Connections successful.
2025/11/24 22:22:44 [TASK] Validating DocumentDB Change Stream configuration...
2025/11/24 22:22:44 [INFO] [VALIDATE] Running $listChangeStreams on admin DB...
2025/11/24 22:22:44 [INFO] [VALIDATE] Found 15 enabled change streams:
2025/11/24 22:22:44 [INFO] [VALIDATE]   - my_awesome_app.*
2025/11/24 22:22:44 [INFO] [VALIDATE]   - my_awesome_app.users
2025/11/24 22:22:44 [INFO] [VALIDATE]   - percona_db_1.test_1
2025/11/24 22:22:44 [INFO] [VALIDATE]   - percona_db_1.test_5
2025/11/24 22:22:44 [INFO] [VALIDATE]   - percona_db_1.test_2
2025/11/24 22:22:44 [INFO] [VALIDATE]   - percona_db_1.test_3
2025/11/24 22:22:44 [INFO] [VALIDATE]   - percona_db_1.test_4
2025/11/24 22:22:44 [INFO] [VALIDATE]   - percona_db_2.test_1
2025/11/24 22:22:44 [INFO] [VALIDATE]   - percona_db_2.test_2
2025/11/24 22:22:44 [INFO] [VALIDATE]   - percona_db_2.test_3
2025/11/24 22:22:44 [INFO] [VALIDATE]   - percona_db_2.test_4
2025/11/24 22:22:44 [INFO] [VALIDATE]   - percona_db_2.test_5
2025/11/24 22:22:44 [INFO] [VALIDATE]   - tobeignored.skipme_1
2025/11/24 22:22:44 [INFO] [VALIDATE]   - alpha.test_1
2025/11/24 22:22:44 [INFO] [VALIDATE]   - CLUSTER_WIDE (*.*)
2025/11/24 22:22:44 [OK]   DocumentDB Change Stream configuration is valid.
2025/11/24 22:22:44
--- Phase 2: LAUNCHING BACKGROUND PROCESS ---
2025/11/24 22:22:44 [OK]   Application started in background with PID: 1663258
2025/11/24 22:22:44 [INFO] CDC Operations logger initialized (file: logs/cdc.log)
2025/11/24 22:22:44 [INFO] Full Load logger initialized (file: logs/full_load.log)
2025/11/24 22:22:44 [INFO] Writing PID 1663258 to docMongoStream.pid
2025/11/24 22:22:44 [INFO] Status manager initialized (collection: docMongoStream.status)
2025/11/24 22:22:44 [INFO] [VAL] Starting 4 parallel CDC validation workers...
2025/11/24 22:22:44 [INFO] Checkpoint manager initialized (collection: docMongoStream.checkpoints)
2025/11/24 22:22:44 [INFO] [STATUS] State changed to: connecting (Connections established. Pinging...)
2025/11/24 22:22:44 [INFO] API Server starting on port 8080...
2025/11/24 22:22:45 [INFO] [CDC cdc_resume_timestamp] Found resume timestamp: {1764038958 3}
2025/11/24 22:22:45
--- Phase 1: DISCOVERY (SKIPPED) ---
2025/11/24 22:22:45
--- Phase 2: FULL DATA LOAD (SKIPPED) ---
2025/11/24 22:22:45
--- Phase 3: CONTINUOUS SYNC (CDC) ---
2025/11/24 22:22:45 [INFO] [STATUS] State changed to: running (Change Data Capture)
2025/11/24 22:22:45 [INFO] [CDC cdc_resume_timestamp] Found resume timestamp: {1764038958 3}
2025/11/24 22:22:45 [INFO] [CDC cdc_resume_timestamp] Loaded checkpoint. Resuming from {1764038958 3}
2025/11/24 22:22:45 [INFO] [CDC] Resuming event count from 1817845
2025/11/24 22:22:45 [INFO] Starting cluster-wide CDC... Resuming from checkpoint: {1764038958 3}
2025/11/24 22:22:45 [INFO] [CDC] Starting 4 partition-aware write workers...
2025/11/24 22:22:45 [OK]   Application is healthy (State: running).
```
</details>

### Status

```bash
./docMongoStream status
```

<details>
<summary>Sample output:</summary>

```bash
--- docMongoStream Status (Live) ---
PID: 1318823 (Querying http://localhost:8080/status)
{
    "ok": true,
    "state": "running",
    "info": "Change Data Capture",
    "timeSinceLastEventSeconds": 27.660679778,
    "cdcLagSeconds": 27.364062096,
    "totalEventsApplied": 99192,
    "validation": {
        "totalChecked": 55643,
        "validCount": 55643,
        "mismatchCount": 0,
        "syncPercent": 100,
        "lastValidatedAt": "2025-11-24T19:31:45Z"
    },
    "lastSourceEventTime": {
        "ts": "1764012678.22",
        "isoDate": "2025-11-24T19:31:18Z"
    },
    "lastBatchAppliedAt": "2025-11-24T19:31:45Z",
    "initialSync": {
        "completed": true,
        "completionLagSeconds": 5,
        "cloneCompleted": true,
        "estimatedCloneSizeBytes": 2216505,
        "clonedSizeBytes": 2216505,
        "estimatedCloneSizeHuman": "2.1 MB",
        "clonedSizeHuman": "2.1 MB"
    }
}
```
</details>

#### Understanding the status output

The status command provides real-time metrics on the health and progress of your migration.

* ok
    * true: The application is healthy and operating normally.
    * false: A critical error has occurred (e.g., lost connection), and the process has likely stopped or is in a failed state.

* state: The current phase of the migration. Common states include:
    * starting: The application is initializing (loading configuration, setting up loggers).
    * connecting: Attempting to establish connections to the Source and Target databases.
    * discovering: Scanning the Source database to identify databases and collections to migrate.
    * copying: Synonymous with running during the Full Load phase.
    * running: The main active state. Used for both the Initial Sync (Full Load) and the Continuous Sync (CDC) phases.
    * destroying: Only seen if the --destroy flag is used. docMongoStream is actively dropping target databases before starting.
    * complete: The process has finished its work (only occurs if there were no collections to migrate).
    * error: A fatal error occurred.

* info

    | State       | Info Message                               | Description                                                         |
    |-------------|---------------------------------------------|---------------------------------------------------------------------|
    | connecting  | Connections established. Pinging...         | Connected to DBs, verifying reachability.                           |
    | discovering | Discovering collections to migrate...       | Standard startup; listing collections to sync.                      |
    | discovering | Discovering source DBs to destroy...        | Startup with `--destroy`; listing DBs to drop.                      |
    | destroying  | Dropping target databases...                | Actively deleting data on the target (dangerous).                   |
    | running     | Initial Sync (Full Load)                    | Currently snapshotting existing data.                               |
    | running     | Change Data Capture                         | Sync is live; streaming updates from the source.                    |
    | running     | Applying DDL: `<Op>` on `<NS>`              | Applying a schema change (e.g., drop, rename, create).              |
    | complete    | No collections found to migrate.            | Source was empty or filtered out; nothing to do.                    |
    | error       | error                                       | Check the application logs for the specific fatal error message.    |

* timeSinceLastEventSeconds (Source Idle Time):
    * Meaning: How many seconds have passed since the Source DocumentDB produced a change event.
    * Interpretation: If this number is high but no events are being applied and state is running, it usually means your source database is idle (no changes are happening). This is normal during low-traffic periods.

* cdcLagSeconds (Replication Latency):
    * Meaning: The time difference (latency) between when an event occurred on the Source and when it was successfully applied to the Target.
    * Interpretation: This is your true "lag." It should stay close to 0 (typically < 2 seconds). If this number spikes, it means docMongoStream cannot keep up with the volume of changes. If no events are being applied and state is running, it usually means your source database is idle.

* validation: Tracks the number of documents that are a perfect match between Source and Destination
    * totalChecked: This is the number of total CDC events checked
    * validCount: Number of documents that are an exact match
    * mismatchCount: Number or active discrepancies
    * syncPercent: Percentage of documents that are in perfect sync
    * lastValidatedAt: Last time the records were validated

* totalEventsApplied: The total number of operations replicated since the CDC phase started.

* lastSourceEventTime: The timestamp of the very last operation read from the Source change stream.
    * ts: Internal MongoDB Timestamp format.
    * isoDate: Human-readable UTC time of the event.

* lastBatchAppliedAt: The local wall-clock time when docMongoStream last successfully wrote a batch of data to the Destination MongoDB.

* initialSync: Statistics regarding the Full Load phase. Once the Full load is complete and docMongoStream switches to CDC these numbers will remain static.
    * completed: true if the snapshot phase is finished.
    * completionLagSeconds: How far behind real-time the migration was at the exact moment the Full Load finished.
    * clonedSizeHuman: Total volume of data copied during the Full load phase.


### Logs

docMongoStream generates three separate logs, each of the logs location and name can be configured via [config.yaml](./config.yaml):

1. Application Log (`logs/docMongoStream.log`): Tracks the overall application status and any errors encountered.
2. Full Load Log (`logs/full_load.log`): Dedicated to the initial full synchronization process. This log, together with the status endpoint, helps you monitor the progress of the initial sync.
3. CDC Log (`logs/cdc.log`): Dedicated to Change Data Capture (CDC) operations. These operations begin only after the full sync is complete, so this log will remain empty until that point. Use it, along with the status endpoint, to track CDC progress.

<details>
<summary>Application log sample:</summary>

```bash
2025/11/17 16:13:33 [INFO] CDC Operations logger initialized (file: logs/cdc.log)
2025/11/17 16:13:33 [INFO] Full Load logger initialized (file: logs/full_load.log)
2025/11/17 16:13:33
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
________            ______  ___                                ____________
___  __ \______________   |/  /___________________ ______      __  ___/_  /__________________ _______ ___
__  / / /  __ \  ___/_  /|_/ /_  __ \_  __ \_  __ `/  __ \     _____ \_  __/_  ___/  _ \  __ `/_  __ `__ \
_  /_/ // /_/ / /__ _  /  / / / /_/ /  / / /  /_/ // /_/ /     ____/ // /_ _  /   /  __/ /_/ /_  / / / / /
/_____/ \____/\___/ /_/  /_/  \____//_/ /_/\__, / \____/      /____/ \__/ /_/    \___/\__,_/ /_/ /_/ /_/
                                           /____/
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

2025/11/17 16:13:33 --- docMongoStream Application Start ---
2025/11/17 16:13:33
--- Phase 1: VALIDATION ---
2025/11/17 16:13:41 [TASK] Connecting to source DocumentDB...
2025/11/17 16:13:42 [TASK] Connecting to target MongoDB...
2025/11/17 16:13:42 [OK]   Connections successful.
2025/11/17 16:13:42 [TASK] Validating DocumentDB Change Stream configuration...
2025/11/17 16:13:42 [INFO] [VALIDATE] Running $listChangeStreams on admin DB...
2025/11/17 16:13:42 [INFO] [VALIDATE] Found 15 enabled change streams:
2025/11/17 16:13:42 [INFO] [VALIDATE]   - my_awesome_app.*
2025/11/17 16:13:42 [INFO] [VALIDATE]   - my_awesome_app.users
2025/11/17 16:13:42 [INFO] [VALIDATE]   - percona_db_1.test_1
2025/11/17 16:13:42 [INFO] [VALIDATE]   - percona_db_1.test_5
2025/11/17 16:13:42 [INFO] [VALIDATE]   - percona_db_1.test_2
2025/11/17 16:13:42 [INFO] [VALIDATE]   - percona_db_1.test_3
2025/11/17 16:13:42 [INFO] [VALIDATE]   - percona_db_1.test_4
2025/11/17 16:13:42 [INFO] [VALIDATE]   - percona_db_2.test_1
2025/11/17 16:13:42 [INFO] [VALIDATE]   - percona_db_2.test_2
2025/11/17 16:13:42 [INFO] [VALIDATE]   - percona_db_2.test_3
2025/11/17 16:13:42 [INFO] [VALIDATE]   - percona_db_2.test_4
2025/11/17 16:13:42 [INFO] [VALIDATE]   - percona_db_2.test_5
2025/11/17 16:13:42 [INFO] [VALIDATE]   - tobeignored.skipme_1
2025/11/17 16:13:42 [INFO] [VALIDATE]   - alpha.test_1
2025/11/17 16:13:42 [INFO] [VALIDATE]   - CLUSTER_WIDE (*.*)
2025/11/17 16:13:42 [OK]   DocumentDB Change Stream configuration is valid.
2025/11/17 16:13:42
--- Phase 2: LAUNCHING BACKGROUND PROCESS ---
2025/11/17 16:13:42 [OK]   Application started in background with PID: 3024785
2025/11/17 16:13:42 [INFO] Logs are being written to: logs/docMongoStream.log
2025/11/17 16:13:42 [INFO] To stop the application, run: /home/daniel.almeida/docMongoStream stop
2025/11/17 16:13:42 [INFO] To check status, run: /home/daniel.almeida/docMongoStream status (or GET http://localhost:8080/status)
2025/11/17 16:13:42 [INFO] CDC Operations logger initialized (file: logs/cdc.log)
2025/11/17 16:13:42 [INFO] Full Load logger initialized (file: logs/full_load.log)
2025/11/17 16:13:42 [INFO] Writing PID 3024785 to ./docMongoStream.pid
2025/11/17 16:13:42 [INFO] Status manager initialized (collection: docMongoStream.status)
2025/11/17 16:13:42 [INFO] [STATUS] State changed to: connecting (Connections established. Pinging...)
2025/11/17 16:13:42 [INFO] Starting status HTTP server on :8080/status
2025/11/17 16:13:43 [INFO] Checkpoint manager initialized (collection: docMongoStream.checkpoints)
2025/11/17 16:13:43 [INFO] [CDC cdc_resume_timestamp] No resume timestamp found in checkpoint database.
2025/11/17 16:13:43
--- Phase 1: DISCOVERY ---
2025/11/17 16:13:43 [INFO] [STATUS] State changed to: discovering (Discovering collections to migrate...)
2025/11/17 16:13:43 [TASK] Discovering databases and collections...
2025/11/17 16:13:43 [TASK] Scanning DB: test
2025/11/17 16:13:44 [INFO] - Found: test.car (3 documents, 0 indexes)
2025/11/17 16:13:44 [TASK] Scanning DB: alpha
2025/11/17 16:13:44 [INFO] - Found: alpha.test_1 (9 documents, 0 indexes)
2025/11/17 16:13:44 [INFO] Skipping DB: tobeignored (excluded by configuration)
2025/11/17 16:13:44 [TASK] Scanning DB: cvg_1
2025/11/17 16:13:44 [INFO] Skipping collection: cvg_1.test_1 (excluded by configuration)
2025/11/17 16:13:44 [INFO] Skipping collection: cvg_1.test_3 (excluded by configuration)
2025/11/17 16:13:44 [INFO] - Found: cvg_1.test_2 (2739 documents, 3 indexes)
2025/11/17 16:13:45 [INFO] - Found: cvg_1.test_4 (2000 documents, 3 indexes)
2025/11/17 16:13:45 [INFO] Skipping collection: cvg_1.test_5 (excluded by configuration)
2025/11/17 16:13:45 [OK]   Discovered 4 total collections to migrate.
2025/11/17 16:13:45
--- Phase 2: FULL DATA LOAD ---
2025/11/17 16:13:45 [INFO] [STATUS] State changed to: running (Initial Sync (Full Load))
2025/11/17 16:13:45 [TASK] Starting collection worker pool with 2 concurrent workers...
2025/11/17 16:13:45 [TASK] [Worker 0] Starting full load for test.car
2025/11/17 16:13:45 [INFO] [test.car] Source collection has 3 documents.
2025/11/17 16:13:45 [INFO] [test.car] Creating target collection...
2025/11/17 16:13:45 [TASK] [Worker 1] Starting full load for alpha.test_1
2025/11/17 16:13:45 [INFO] [alpha.test_1] Source collection has 9 documents.
2025/11/17 16:13:45 [INFO] [alpha.test_1] Creating target collection...
2025/11/17 16:13:45 [TASK] [test.car] Starting parallel data load...
2025/11/17 16:13:45 [TASK] [test.car] Read Worker 0 started
2025/11/17 16:13:45 [TASK] [test.car] Read Worker 1 started
2025/11/17 16:13:45 [TASK] [test.car] Read Worker 3 started
2025/11/17 16:13:45 [TASK] [test.car] Read Worker 2 started
2025/11/17 16:13:45 [TASK] [alpha.test_1] Starting parallel data load...
2025/11/17 16:13:45 [TASK] [alpha.test_1] Read Worker 1 started
2025/11/17 16:13:45 [TASK] [alpha.test_1] Read Worker 3 started
2025/11/17 16:13:45 [TASK] [alpha.test_1] Read Worker 0 started
2025/11/17 16:13:45 [TASK] [alpha.test_1] Read Worker 2 started
2025/11/17 16:13:46 [TASK] [test.car] Processed batch: 3 inserted, 0 replaced. (90 B) in 16.420105ms
2025/11/17 16:13:46 [TASK] [alpha.test_1] Processed batch: 9 inserted, 0 replaced. (270 B) in 18.43082ms
2025/11/17 16:13:46 [OK]   [test.car] Data pipeline complete. Copied 3 documents. Finish time: {1763414026 1}
2025/11/17 16:13:46 [OK]   [test.car] Full load COMPLETED: 3 docs copied in 1.165625027s
2025/11/17 16:13:46 [TASK] [Worker 0] Starting full load for cvg_1.test_2
2025/11/17 16:13:46 [INFO] [cvg_1.test_2] Source collection has 2739 documents.
2025/11/17 16:13:46 [INFO] [cvg_1.test_2] Creating target collection...
2025/11/17 16:13:46 [INFO] [cvg_1.test_2] Starting creation of 3 indexes...
2025/11/17 16:13:46 [OK]   [alpha.test_1] Data pipeline complete. Copied 9 documents. Finish time: {1763414026 2}
2025/11/17 16:13:46 [OK]   [alpha.test_1] Full load COMPLETED: 9 docs copied in 1.279312794s
2025/11/17 16:13:46 [TASK] [Worker 1] Starting full load for cvg_1.test_4
2025/11/17 16:13:46 [INFO] [cvg_1.test_4] Source collection has 2000 documents.
2025/11/17 16:13:46 [INFO] [cvg_1.test_4] Creating target collection...
2025/11/17 16:13:46 [INFO] [cvg_1.test_2] Submitted 3 indexes in 53.175212ms: [email_1 name_1_email_1 status_1]
2025/11/17 16:13:46 [TASK] [cvg_1.test_2] Starting parallel data load...
2025/11/17 16:13:46 [TASK] [cvg_1.test_2] Read Worker 1 started
2025/11/17 16:13:46 [TASK] [cvg_1.test_2] Read Worker 0 started
2025/11/17 16:13:46 [TASK] [cvg_1.test_2] Read Worker 3 started
2025/11/17 16:13:46 [TASK] [cvg_1.test_2] Read Worker 2 started
2025/11/17 16:13:46 [INFO] [cvg_1.test_4] Starting creation of 3 indexes...
2025/11/17 16:13:46 [INFO] [cvg_1.test_4] Submitted 3 indexes in 43.028099ms: [email_1 name_1_email_1 status_1]
2025/11/17 16:13:46 [TASK] [cvg_1.test_4] Starting parallel data load...
2025/11/17 16:13:46 [TASK] [cvg_1.test_4] Read Worker 0 started
2025/11/17 16:13:46 [TASK] [cvg_1.test_4] Read Worker 1 started
2025/11/17 16:13:46 [TASK] [cvg_1.test_4] Read Worker 2 started
2025/11/17 16:13:46 [TASK] [cvg_1.test_4] Read Worker 3 started
2025/11/17 16:13:47 [TASK] [cvg_1.test_2] Processed batch: 1000 inserted, 0 replaced. (189 kB) in 169.740435ms
2025/11/17 16:13:47 [TASK] [cvg_1.test_4] Processed batch: 1000 inserted, 0 replaced. (195 kB) in 163.707015ms
2025/11/17 16:13:47 [TASK] [cvg_1.test_2] Processed batch: 1000 inserted, 0 replaced. (194 kB) in 207.694475ms
2025/11/17 16:13:47 [TASK] [cvg_1.test_4] Processed batch: 1000 inserted, 0 replaced. (195 kB) in 162.618865ms
2025/11/17 16:13:47 [TASK] [cvg_1.test_2] Processed batch: 739 inserted, 0 replaced. (144 kB) in 128.262643ms
2025/11/17 16:13:47 [OK]   [cvg_1.test_4] Data pipeline complete. Copied 2000 documents. Finish time: {1763414027 1}
2025/11/17 16:13:47 [INFO] [cvg_1.test_4] Finalizing 3 indexes...
2025/11/17 16:13:47 [OK]   [0] All indexes confirmed.
2025/11/17 16:13:47 [OK]   [cvg_1.test_4] Full load COMPLETED: 2000 docs copied in 1.241415031s
2025/11/17 16:13:47 [OK]   [cvg_1.test_2] Data pipeline complete. Copied 2739 documents. Finish time: {1763414027 2}
2025/11/17 16:13:47 [INFO] [cvg_1.test_2] Finalizing 3 indexes...
2025/11/17 16:13:47 [OK]   [0] All indexes confirmed.
2025/11/17 16:13:47 [OK]   [cvg_1.test_2] Full load COMPLETED: 2739 docs copied in 1.469129307s
2025/11/17 16:13:47 [OK]   All full load workers complete.
2025/11/17 16:13:47 [INFO] Persisting completed initial sync status...
2025/11/17 16:13:47
--- Phase 3: CONTINUOUS SYNC (CDC) ---
2025/11/17 16:13:47 [INFO] [STATUS] State changed to: running (Change Data Capture)
2025/11/17 16:13:47 [INFO] Starting cluster-wide CDC... Resuming from timestamp: {1763414027 2}
2025/11/17 16:13:47 [INFO] [CDC] Starting 4 concurrent write workers...
2025/11/17 16:13:47 [TASK] [CDC] Starting cluster-wide change stream watcher...
```
</details>

<details>
<summary>full sync log:</summary>

```json
{"level":"info","message":"Full load batch applied","s":"clone_batch","ns":"test.car","doc_count":3,"byte_size":90,"elapsed_secs":0.016270551,"time":"2025-11-17 16:13:46.115"}
{"level":"info","message":"Full load batch applied","s":"clone_batch","ns":"alpha.test_1","doc_count":9,"byte_size":270,"elapsed_secs":0.018374738,"time":"2025-11-17 16:13:46.208"}
{"level":"info","message":"Full load for namespace completed","s":"clone","ns":"test.car","doc_count":3,"elapsed_secs":1.165625332,"time":"2025-11-17 16:13:46.282"}
{"level":"info","message":"Full load for namespace completed","s":"clone","ns":"alpha.test_1","doc_count":9,"elapsed_secs":1.279313376,"time":"2025-11-17 16:13:46.396"}
{"level":"info","message":"Full load batch applied","s":"clone_batch","ns":"cvg_1.test_2","doc_count":1000,"byte_size":188853,"elapsed_secs":0.169654777,"time":"2025-11-17 16:13:47.340"}
{"level":"info","message":"Full load batch applied","s":"clone_batch","ns":"cvg_1.test_4","doc_count":1000,"byte_size":194805,"elapsed_secs":0.163625278,"time":"2025-11-17 16:13:47.404"}
{"level":"info","message":"Full load batch applied","s":"clone_batch","ns":"cvg_1.test_2","doc_count":1000,"byte_size":194375,"elapsed_secs":0.207619712,"time":"2025-11-17 16:13:47.529"}
{"level":"info","message":"Full load batch applied","s":"clone_batch","ns":"cvg_1.test_4","doc_count":1000,"byte_size":194769,"elapsed_secs":0.162549268,"time":"2025-11-17 16:13:47.557"}
{"level":"info","message":"Full load batch applied","s":"clone_batch","ns":"cvg_1.test_2","doc_count":739,"byte_size":144165,"elapsed_secs":0.128153083,"time":"2025-11-17 16:13:47.596"}
{"level":"info","message":"Full load for namespace completed","s":"clone","ns":"cvg_1.test_4","doc_count":2000,"elapsed_secs":1.241415552,"time":"2025-11-17 16:13:47.638"}
{"level":"info","message":"Full load for namespace completed","s":"clone","ns":"cvg_1.test_2","doc_count":2739,"elapsed_secs":1.469130041,"time":"2025-11-17 16:13:47.752"}
```
</details>

<details>
<summary>cdc log:</summary>

```json
{"level":"info","message":"CDC batch applied","s":"cdc","batch_size":606,"elapsed_secs":0.066290354,"namespaces":["cvg_1.test_1"],"time":"2025-11-17 16:23:10.392"}
{"level":"info","message":"CDC batch applied","s":"cdc","batch_size":394,"elapsed_secs":0.027184365,"namespaces":["cvg_1.test_1"],"time":"2025-11-17 16:23:10.853"}
{"level":"info","message":"CDC batch applied","s":"cdc","batch_size":505,"elapsed_secs":0.036637442,"namespaces":["cvg_1.test_2"],"time":"2025-11-17 16:23:11.361"}
{"level":"info","message":"CDC batch applied","s":"cdc","batch_size":495,"elapsed_secs":0.036345343,"namespaces":["cvg_1.test_2"],"time":"2025-11-17 16:23:11.862"}
{"level":"info","message":"CDC batch applied","s":"cdc","batch_size":505,"elapsed_secs":0.059210708,"namespaces":["cvg_1.test_3"],"time":"2025-11-17 16:23:12.384"}
{"level":"info","message":"CDC batch applied","s":"cdc","batch_size":707,"elapsed_secs":0.046284463,"namespaces":["cvg_1.test_3","cvg_1.test_4"],"time":"2025-11-17 16:23:12.871"}
{"level":"info","message":"CDC batch applied","s":"cdc","batch_size":505,"elapsed_secs":0.029270988,"namespaces":["cvg_1.test_4"],"time":"2025-11-17 16:23:13.354"}
{"level":"info","message":"CDC batch applied","s":"cdc","batch_size":384,"elapsed_secs":0.069303515,"namespaces":["cvg_1.test_4","cvg_1.test_5"],"time":"2025-11-17 16:23:13.895"}
{"level":"info","message":"CDC batch applied","s":"cdc","batch_size":606,"elapsed_secs":0.038757109,"namespaces":["cvg_1.test_5"],"time":"2025-11-17 16:23:14.364"}
{"level":"info","message":"CDC batch applied","s":"cdc","batch_size":293,"elapsed_secs":0.023002195,"namespaces":["cvg_1.test_5"],"time":"2025-11-17 16:23:14.849"}
```
</details>

## Performance & Optimization

### Full Load Optimization

docMongoStream uses dedicated worker pools for both migration phases, eliminating sequential bottlenecks and maximizing concurrent I/O. The Full Load phase relies on splitting work into as many parallel jobs as possible without overwhelming the source DocumentDB or target MongoDB I/O queues. However, the settings multiply each other and if you configure them too high, you can easily saturate your CPU or network. The formula below might help you tune these settings accordingly:

***Total Threads = migration.max_concurrent_workers * cloner.num_read_workers + cloner.num_insert_workers***

Lets take our default values set in [config.yaml](./config.yaml) 

- migration.max_concurrent_workers: 2
- cloner.num_read_workers: 4
- cloner.num_insert_workers: 8

This will give us 2 collections doing the full migration at once and each of these collections will have 12 workers (4 read + 8 write). Even though we have 12 workers and 2 collections for a total of 24 threads, there is a split depending on how many are read and write. The total of active threads in this case will be the following for each given environment:

***Source***

Total Source Connections = 2 Collections * 4 Read Workers (8 concurrent threads)\
Note: There is also 1 "segmenter" thread per collection calculating ID ranges, so it's technically ~10 threads, but the segmenter load is very light compared to the readers.

***Destination***

Only the Insert Workers connect to the destination.\
Total Destination Connections = 2 Collections * 8 Insert Workers (16 concurrent threads)

| Setting | Purpose |
|--------:|---------|
| `migration.max_concurrent_workers` | Maximum number of collections to copy at the same time, this controls how many collections are migrated simultaneously during the full load stage |
| `cloner.segment_size_docs` | Defines the size of each data chunk when splitting large collections. Size (in docs) of a segment for parallel reads. Helps prevent massive collections/long-running queries from timing out or monopolizing cluster resources.  e.g. A collection of 1M docs will be split into 100 segments of 10k docs|
| `cloner.num_read_workers` | Controls how many threads are used to read data for a single collection from the source DocumentDB |
| `cloner.num_insert_workers` | Controls how many threads are used to write data for a single collection into the destination MongoDB. Higher values (e.g., 8–16) improve throughput if the target cluster is capable. |
| `cloner.read_batch_size` | Number of documents per read batch | 
| `cloner.insert_batch_size` | Number of documents per insert batch |
| `cloner.insert_batch_bytes` | Max size (in bytes) of a single insert batch, default 16777216 bytes (16MB) |


### CDC Optimization

Change Data Capture (CDC) performance is largely governed by concurrency and batching efficiency. You can optimize the process using the `max_write_workers` setting, in combination with the batch size. `max_write_workers` controls the number of concurrent background routines that act as "consumers" during the CDC phase. These workers are responsible for processing batched change events and applying them to the target MongoDB.

This setting determines the write pipeline's capacity during live synchronization. A higher value enables greater parallelism when replaying real-time events. You can increase this value to utilize more target resources and improve real-time throughput. This is particularly effective if the source (DocumentDB) has a high volume of changes and the target (MongoDB) has ample CPU and I/O capacity.

**Note:** Setting this value too high may saturate connections or CPU resources on the target MongoDB cluster, potentially degrading the performance of other operations.

**Note on Partitioning:** docMongoStream uses Key-Based Partitioning to guarantee strict data ordering. This means all updates for a specific document are handled by the same worker. In rare cases of "Hot Keys" (a single document receiving massive update volume), one worker may be utilized more than others. This is an intentional trade-off to ensure data integrity.


| Setting | Purpose |
|--------:|---------|
| `cdc.max_write_workers` | Number of concurrent background workers that act as CDC consumers. Increase this value to utilize more target MongoDB resources and improve real-time throughput. (Default: 4) |
| `cdc.batch_size` | Number of operations (inserts/updates/deletes) grouped into a single network request. Larger batches reduce network overhead per operation. |
| `cdc.batch_interval_ms` | Maximum wait time before flushing an incomplete batch. Lower values reduce latency; higher values increase overall throughput. This ensures low-volume changes are still applied quickly |
| `cdc.max_await_time_ms` | Max time (in ms) for the change stream to wait for new events |


### Validation Optimization

The data validation engine is highly configurable to balance performance impact against data integrity assurance. You can tune these settings via [config.yaml](./config.yaml) under the `validation` section.

| Setting | Default | Description |
|--------:|--------:|-------------|
| enabled | true | Master switch for the validation engine. If false, final document verification after CDC writes are skipped. CDC is guaranteed to sync the documents; this is an optional additional validation check. |
| batch_size | 100 | Network vs. Memory Trade-off. Controls how many document IDs are bundled into a single database lookup. Larger batches reduce network round-trips but increase memory usage. |
| max_validation_workers | 4 | Concurrency Control. The number of parallel worker threads fetching and comparing documents. Increase this if you have spare CPU/Network capacity and notice validation lagging behind CDC. |
| queue_size | 2000 | Buffer Capacity. The size of the channel buffering CDC events before validation. If the CDC writer is faster than the validator and this buffer fills up, validation requests will be dropped to prevent slowing down the replication stream. |
| retry_interval_ms | 500 | Hot Key Handling. If a record fails validation because it is actively being modified (detected via dirty tracking), the validator waits this long before re-checking it. |
| max_retries | 3 | Persistence. How many times to retry a "Hot Key" before giving up. After this many attempts, the record is marked as a mismatch/skipped to move on. |

## Limitations & Important Notes

### DocumentDB Cursor Rate Limiting

AWS DocumentDB enforces service quotas, including limits on the number of cursors and the rate of getMore operations, which are fundamental to how change streams work.

Symptom: If the migration falls too far behind (e.g., after being stopped for a long time) or if there is a massive burst of write activity, the docMongoStream tool may hit these rate limits. This can cause the change stream to fail or be terminated by AWS.

Behavior: docMongoStream is designed to be resilient and will attempt to retry and resume the stream. However, persistent rate-limiting from the DocumentDB side may require intervention (e.g., scaling your DocumentDB instance or running the migration during off-peak hours).

### DDL Operation Support

docMongoStream has support for replicating most DDL operations. 

Supported: drop (collection), dropDatabase, rename (collection), create (collection). 

NOT Supported: createIndexes, dropIndexes. These operations will be detected and logged, but skipped. You must manually recreate or drop indexes on the target cluster.

## Additional Documentation

We have created a page dedicated to a more in [depth explanation of how docMongoStream works](./details.md) as well as a [frequently asked questions](./faq.md) page.




