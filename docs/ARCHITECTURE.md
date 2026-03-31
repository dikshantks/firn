# Fern - Iceberg Maintenance Platform Architecture

## Table of Contents

1. [Overview](#overview)
2. [Current System Architecture](#current-system-architecture)
3. [Data Model](#data-model)
4. [Component Details](#component-details)
5. [API Endpoints](#api-endpoints)
6. [Data Flow](#data-flow)
7. [Future Architecture](#future-architecture)
8. [Policy-Based Maintenance](#policy-based-maintenance)
9. [Roadmap](#roadmap)

---

## Overview

**Fern** is an Apache Iceberg metadata visualization and maintenance platform. It provides:

- **Catalog Management**: Connect to Hive Metastore or AWS Glue catalogs
- **Health Monitoring**: Analyze table health metrics (snapshots, small files, delete files)
- **Maintenance Recommendations**: Generate actionable maintenance commands
- **Real-time Streaming**: SSE-based progress updates for large catalog scans
- **Caching**: SQLite-based health data caching for instant dashboard loads

### Technology Stack

| Layer | Technology |
|-------|------------|
| Frontend | React, TypeScript, TailwindCSS, React Query |
| Backend | Python, FastAPI, Pydantic |
| Catalog Integration | PyIceberg, boto3 |
| Storage | SQLite (cache), In-memory (catalog registry) |
| External Systems | AWS Glue, Hive Metastore, S3/MinIO |

---

## Current System Architecture

```mermaid
flowchart TB
    subgraph frontend [Frontend - React]
        UI[Health Dashboard]
        TableView[Table Explorer]
        Stream[SSE Stream Handler]
    end
    
    subgraph backend [Backend - FastAPI]
        API[REST API Gateway]
        HealthRouter["/api/health/*"]
        CatalogRouter["/api/catalogs/*"]
        JobRouter["/api/jobs/*"]
        TableRouter["/api/tables/*"]
    end
    
    subgraph services [Services Layer]
        HealthService[HealthService]
        CatalogService[CatalogService]
        JobService[JobService]
        MetadataService[MetadataService]
    end
    
    subgraph storage [Storage Layer]
        HealthCache[(SQLite Cache<br/>health_cache.db)]
        InMemory[(In-Memory<br/>Catalog Registry)]
    end
    
    subgraph external [External Systems]
        Glue[AWS Glue Catalog]
        Hive[Hive Metastore]
        S3[S3 / MinIO<br/>Iceberg Data]
    end
    
    UI --> API
    TableView --> API
    Stream -.->|SSE| HealthRouter
    
    API --> HealthRouter
    API --> CatalogRouter
    API --> JobRouter
    API --> TableRouter
    
    HealthRouter --> HealthService
    CatalogRouter --> CatalogService
    JobRouter --> JobService
    TableRouter --> MetadataService
    
    HealthService --> HealthCache
    HealthService --> JobService
    HealthService -.->|PyIceberg| S3
    
    CatalogService --> InMemory
    CatalogService -.->|PyIceberg| Glue
    CatalogService -.->|PyIceberg| Hive
    
    MetadataService -.->|PyIceberg| S3
```

### Architecture Principles

1. **Stateless API**: Backend is stateless except for in-memory catalog connections
2. **Cache-First**: Health data is cached in SQLite for instant retrieval
3. **Streaming for Scale**: Large catalogs use SSE streaming for real-time progress
4. **External Execution**: Fern does not execute maintenance; it generates commands for Spark/Trino

---

## Data Model

### Current Schema (SQLite Cache)

```mermaid
erDiagram
    catalog_summary ||--o{ table_health : "1:N on catalog"
    table_health ||--|| metrics : "embedded"
    table_health ||--o{ recommendations : "JSON array"
    
    catalog_summary {
        INTEGER id PK
        TEXT catalog UK "Unique catalog name"
        INTEGER total_tables
        INTEGER healthy_tables
        INTEGER warning_tables
        INTEGER critical_tables
        INTEGER tables_needing_snapshot_expiration
        INTEGER tables_needing_compaction
        INTEGER tables_needing_manifest_rewrite
        INTEGER tables_with_delete_files
        REAL total_wasted_storage_gb
        TEXT scan_mode "light or full"
        TIMESTAMP scanned_at
    }
    
    table_health {
        INTEGER id PK
        TEXT catalog FK "Links to catalog_summary"
        TEXT namespace
        TEXT table_name
        TEXT status "healthy|warning|critical"
        INTEGER health_score "0-100"
        INTEGER total_snapshots
        INTEGER total_data_files
        INTEGER total_delete_files
        INTEGER small_files_count
        REAL total_size_gb
        REAL avg_file_size_mb
        REAL oldest_snapshot_age_days
        REAL days_since_last_write
        INTEGER issues_count
        INTEGER warnings_count
        TEXT recommendations_json "JSON array"
        TEXT scan_mode
        TIMESTAMP scanned_at
    }
    
    metrics {
        INTEGER total_snapshots
        INTEGER snapshots_last_7_days
        INTEGER snapshots_last_30_days
        INTEGER total_data_files
        INTEGER total_delete_files
        INTEGER small_files_count
        REAL avg_file_size_mb
        REAL total_size_gb
        INTEGER total_manifests
        INTEGER small_manifests_count
    }
    
    recommendations {
        TEXT type "expire_snapshots|compact_data_files|..."
        TEXT priority "high|medium|low"
        TEXT reason
        TEXT estimated_impact
        TEXT command_example
    }
```

### Pydantic Models (API Layer)

```mermaid
classDiagram
    class TableHealth {
        +str catalog
        +str namespace
        +str table_name
        +HealthStatus status
        +int health_score
        +TableHealthMetrics metrics
        +List~MaintenanceRecommendation~ recommendations
        +int issues_count
        +int warnings_count
        +datetime last_checked
    }
    
    class TableHealthMetrics {
        +int total_snapshots
        +float oldest_snapshot_age_days
        +int snapshots_last_7_days
        +int snapshots_last_30_days
        +int total_data_files
        +int total_delete_files
        +int small_files_count
        +float avg_file_size_mb
        +float total_size_gb
        +int total_manifests
        +int small_manifests_count
        +float days_since_last_write
    }
    
    class MaintenanceRecommendation {
        +MaintenanceType type
        +str priority
        +str reason
        +str estimated_impact
        +str command_example
    }
    
    class TableHealthSummary {
        +int total_tables
        +int healthy_tables
        +int warning_tables
        +int critical_tables
        +int tables_needing_snapshot_expiration
        +int tables_needing_compaction
        +int tables_needing_manifest_rewrite
        +int tables_with_delete_files
        +float total_wasted_storage_gb
        +str scan_mode
        +datetime cached_at
        +int cache_age_minutes
    }
    
    class HealthThresholds {
        +int snapshot_warning_threshold
        +int snapshot_critical_threshold
        +int snapshot_age_warning_days
        +int snapshot_age_critical_days
        +int small_file_size_mb
        +int small_file_warning_threshold
        +int small_file_critical_threshold
        +int delete_file_warning_threshold
        +int delete_file_critical_threshold
    }
    
    TableHealth --> TableHealthMetrics
    TableHealth --> MaintenanceRecommendation
    
    class HealthStatus {
        <<enumeration>>
        HEALTHY
        WARNING
        CRITICAL
    }
    
    class MaintenanceType {
        <<enumeration>>
        EXPIRE_SNAPSHOTS
        COMPACT_DATA_FILES
        REWRITE_MANIFESTS
        DELETE_ORPHAN_FILES
        REWRITE_DELETE_FILES
    }
```

---

## Component Details

### CatalogService

**Location**: `backend/app/services/catalog_service.py`

**Purpose**: Manages connections to Iceberg catalogs (Glue, Hive)

**Storage**: In-memory dictionaries (`_catalogs`, `_configs`)

**Key Operations**:
| Method | Description |
|--------|-------------|
| `register_catalog()` | Connect to a new catalog, normalize credentials |
| `get_catalog()` | Get PyIceberg Catalog instance |
| `list_catalogs()` | List all registered catalogs with optional stats |
| `test_catalog()` | Verify connectivity by listing namespaces |
| `remove_catalog()` | Disconnect and remove from registry |

**Credential Handling**:
- Glue: Maps boto3-style keys to PyIceberg `client.*` format
- S3: Auto-propagates Glue credentials to `s3.*` properties
- MinIO: Sets `s3.path-style-access=true` when endpoint is configured

### HealthService

**Location**: `backend/app/services/health_service.py`

**Purpose**: Analyzes table health and generates maintenance recommendations

**Scan Modes**:
| Mode | Speed | S3 Calls | Metrics Available |
|------|-------|----------|-------------------|
| `light` | Fast | None | Snapshots, file counts from metadata.json |
| `full` | Slow | Many | All metrics including small file detection |

**Key Operations**:
| Method | Description |
|--------|-------------|
| `analyze_table_health()` | Single table health analysis |
| `scan_all_tables()` | Batch scan with progress updates |
| `scan_all_tables_streaming()` | Generator yielding namespace-by-namespace results |
| `run_background_scan()` | Full scan with caching |

**Health Score Calculation**:
```
score = 100
score -= (high_priority_issues * 20)
score -= (medium_priority_issues * 10)
score -= (low_priority_issues * 5)

status = HEALTHY if score >= 80
status = WARNING if score >= 60
status = CRITICAL if score < 60
```

### HealthCache

**Location**: `backend/app/services/health_cache.py`

**Purpose**: SQLite-based persistence for health scan results

**Database**: `backend/app/data/health_cache.db`

**Tables**:
- `table_health`: Per-table health data with metrics
- `catalog_summary`: Aggregate statistics per catalog

**Key Operations**:
| Method | Description |
|--------|-------------|
| `save_table_health()` | Upsert table health (INSERT OR REPLACE) |
| `save_catalog_summary()` | Upsert catalog summary |
| `get_cached_summary()` | Get summary if fresh (within max_age_minutes) |
| `get_tables_by_status()` | Query tables filtered by health status |
| `get_tables_needing_maintenance()` | Query tables by maintenance criteria |

### JobService

**Location**: `backend/app/services/job_service.py`

**Purpose**: In-process background task execution

**Executor**: `ThreadPoolExecutor` with 4 workers

**Storage**: In-memory dictionary (jobs lost on restart)

**Job Lifecycle**:
```
PENDING → RUNNING → COMPLETED
                  → FAILED
```

**Key Operations**:
| Method | Description |
|--------|-------------|
| `create_job()` | Create new job with UUID |
| `run_in_background()` | Submit task to thread pool |
| `update_job()` | Update progress/message (called by workers) |
| `get_job()` | Get job status |
| `cleanup_old_jobs()` | Remove completed jobs older than threshold |

---

## API Endpoints

### Health Endpoints (`/api/health`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/summary` | Get cached health summary (mode: cached/light/full) |
| GET | `/summary/stream` | SSE streaming health scan |
| GET | `/tables/{ns}/{table}` | Single table health |
| GET | `/tables/cached` | Query cached table health with filters |
| GET | `/cache/info` | Cache status (age, table count) |
| DELETE | `/cache` | Clear cached data for catalog |
| POST | `/scan/trigger` | Trigger background scan |

### Catalog Endpoints (`/api/catalogs`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | List all catalogs |
| POST | `/` | Register new catalog |
| POST | `/async` | Register catalog with progress tracking |
| GET | `/{name}` | Get catalog info |
| GET | `/{name}/test` | Test catalog connectivity |
| DELETE | `/{name}` | Remove catalog |

### Job Endpoints (`/api/jobs`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | List all jobs |
| GET | `/{id}` | Get job status |
| GET | `/{id}/stream` | SSE job progress stream |
| DELETE | `/{id}` | Delete job |
| POST | `/cleanup` | Remove old completed jobs |

---

## Data Flow

### Health Scan Flow (Streaming)

```mermaid
sequenceDiagram
    participant UI as Frontend
    participant API as /health/summary/stream
    participant HS as HealthService
    participant Cache as HealthCache
    participant Catalog as PyIceberg Catalog
    participant S3 as S3/Glue
    
    UI->>API: GET /health/summary/stream?catalog=glue&mode=light
    API->>HS: scan_all_tables_streaming()
    
    HS->>Catalog: list_namespaces()
    Catalog->>S3: Glue API call
    S3-->>Catalog: namespace list
    Catalog-->>HS: namespaces
    
    HS-->>API: SSE: progress event
    API-->>UI: {"type": "progress", "namespaces_total": 100}
    
    loop For each namespace
        HS->>Catalog: list_tables(namespace)
        loop For each table
            HS->>Catalog: load_table()
            Catalog->>S3: Read metadata.json
            S3-->>Catalog: table metadata
            HS->>HS: analyze_table_health()
            HS->>Cache: save_table_health()
        end
        HS-->>API: SSE: namespace_complete event
        API-->>UI: {"type": "namespace_complete", "namespace": "db1", ...}
    end
    
    HS->>Cache: save_catalog_summary()
    HS-->>API: SSE: scan_complete event
    API-->>UI: {"type": "scan_complete", "summary": {...}}
```

### Cached Request Flow

```mermaid
sequenceDiagram
    participant UI as Frontend
    participant API as /health/summary
    participant Cache as HealthCache
    
    UI->>API: GET /health/summary?catalog=glue&mode=cached
    API->>Cache: get_cached_summary(catalog, max_age=60)
    
    alt Cache hit (fresh data)
        Cache-->>API: TableHealthSummary
        API-->>UI: 200 OK with summary
    else Cache miss (no data or stale)
        Cache-->>API: None
        API-->>UI: 404 "No cached data, use /stream or /scan/trigger"
    end
```

---

## Future Architecture

### Scheduler Integration

```mermaid
flowchart TB
    subgraph scheduler [Scheduler Layer]
        Airflow[Apache Airflow]
        Cron[Cron / Systemd Timers]
        EventBridge[AWS EventBridge]
    end
    
    subgraph fern [Fern Platform]
        API[REST API]
        PolicyEngine[Policy Engine]
        PolicyDB[(Policy Database<br/>PostgreSQL)]
        HealthCache[(Health Cache)]
        CommandGen[Command Generator]
    end
    
    subgraph execution [Execution Layer]
        Spark[Spark on EMR/Databricks]
        Trino[Trino Procedures]
        PyIceberg[PyIceberg Direct]
    end
    
    subgraph monitoring [Observability]
        Logs[Execution Logs]
        Metrics[Prometheus Metrics]
        Alerts[Alert Manager]
    end
    
    subgraph iceberg [Iceberg Tables]
        Tables[(Iceberg Tables<br/>on S3)]
    end
    
    Airflow -->|"1. Trigger health scan"| API
    API -->|"2. Return health data"| Airflow
    Airflow -->|"3. Evaluate policies"| PolicyEngine
    PolicyEngine -->|"4. Read rules"| PolicyDB
    PolicyEngine -->|"5. Generate maintenance plan"| CommandGen
    CommandGen -->|"6. Return commands"| Airflow
    
    Airflow -->|"7a. Execute via Spark"| Spark
    Airflow -->|"7b. Execute via Trino"| Trino
    Airflow -->|"7c. Execute via PyIceberg"| PyIceberg
    
    Spark --> Tables
    Trino --> Tables
    PyIceberg --> Tables
    
    Spark --> Logs
    Trino --> Logs
    Logs --> Metrics
    Metrics --> Alerts
```

### Airflow DAG Example (Future)

```python
# Example DAG for automated Iceberg maintenance
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-platform',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'iceberg_maintenance',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    # Step 1: Trigger health scan
    trigger_scan = SimpleHttpOperator(
        task_id='trigger_health_scan',
        http_conn_id='fern_api',
        endpoint='/api/health/scan/trigger',
        method='POST',
        data={'catalog': 'production', 'mode': 'light'},
    )
    
    # Step 2: Wait for scan completion
    wait_for_scan = PythonOperator(
        task_id='wait_for_scan',
        python_callable=poll_job_completion,
    )
    
    # Step 3: Get tables needing maintenance
    get_maintenance_tasks = SimpleHttpOperator(
        task_id='get_maintenance_tasks',
        http_conn_id='fern_api',
        endpoint='/api/health/tables/cached',
        method='GET',
        data={'catalog': 'production', 'status_filter': 'critical'},
    )
    
    # Step 4: Execute maintenance via Spark
    run_maintenance = SparkSubmitOperator(
        task_id='run_spark_maintenance',
        application='/opt/spark/maintenance.py',
        conf={'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog'},
    )
    
    trigger_scan >> wait_for_scan >> get_maintenance_tasks >> run_maintenance
```

---

## Policy-Based Maintenance

### Policy Schema (Future)

```mermaid
erDiagram
    MAINTENANCE_POLICY ||--o{ POLICY_RULE : contains
    MAINTENANCE_POLICY ||--o{ POLICY_SCHEDULE : has
    MAINTENANCE_POLICY ||--o{ POLICY_EXECUTION_LOG : generates
    POLICY_RULE ||--o{ RULE_CONDITION : has
    POLICY_EXECUTION_LOG ||--o{ TABLE_EXECUTION_DETAIL : contains
    
    MAINTENANCE_POLICY {
        uuid id PK
        string name "e.g., production-daily-cleanup"
        string description
        string scope_type "catalog|namespace|table|tag"
        string scope_value "e.g., production or db.* or db.table"
        boolean enabled
        int priority "execution order"
        timestamp created_at
        timestamp updated_at
        string created_by
    }
    
    POLICY_RULE {
        uuid id PK
        uuid policy_id FK
        string maintenance_type "expire_snapshots|compact|rewrite_manifests|remove_orphans"
        string execution_backend "spark|trino|pyiceberg"
        json thresholds "custom thresholds for this rule"
        json execution_config "backend-specific config"
        int max_tables_per_run "limit concurrent maintenance"
        boolean dry_run "generate commands only"
    }
    
    RULE_CONDITION {
        uuid id PK
        uuid rule_id FK
        string metric "snapshot_count|small_files|delete_files|age_days|size_gb"
        string operator "gt|lt|gte|lte|eq|between"
        float value
        float value_max "for between operator"
        string logical_op "AND|OR with next condition"
    }
    
    POLICY_SCHEDULE {
        uuid id PK
        uuid policy_id FK
        string schedule_type "cron|interval|manual|event"
        string cron_expression "0 2 * * * for daily 2AM"
        int interval_minutes
        string event_trigger "on_write|on_threshold"
        timestamp next_run_at
        timestamp last_run_at
    }
    
    POLICY_EXECUTION_LOG {
        uuid id PK
        uuid policy_id FK
        uuid schedule_id FK
        timestamp started_at
        timestamp completed_at
        string status "pending|running|success|partial|failed"
        int tables_evaluated
        int tables_maintained
        int tables_skipped
        int tables_failed
        float duration_seconds
        string triggered_by "schedule|manual|event"
        json summary_stats
    }
    
    TABLE_EXECUTION_DETAIL {
        uuid id PK
        uuid execution_log_id FK
        string catalog
        string namespace
        string table_name
        string maintenance_type
        string status "success|failed|skipped"
        string command_executed
        timestamp started_at
        timestamp completed_at
        float duration_seconds
        string error_message
        json before_metrics
        json after_metrics
    }
```

### Policy Examples

#### Example 1: Aggressive Snapshot Cleanup

```json
{
  "name": "aggressive-snapshot-cleanup",
  "description": "Expire snapshots for high-write tables",
  "scope_type": "namespace",
  "scope_value": "analytics.*",
  "enabled": true,
  "rules": [
    {
      "maintenance_type": "expire_snapshots",
      "execution_backend": "trino",
      "conditions": [
        {"metric": "snapshot_count", "operator": "gt", "value": 30},
        {"metric": "days_since_last_write", "operator": "lt", "value": 7, "logical_op": "AND"}
      ],
      "execution_config": {
        "retain_last": 10,
        "older_than_days": 7
      }
    }
  ],
  "schedule": {
    "schedule_type": "cron",
    "cron_expression": "0 3 * * *"
  }
}
```

#### Example 2: Small File Compaction

```json
{
  "name": "small-file-compaction",
  "description": "Compact tables with many small files",
  "scope_type": "catalog",
  "scope_value": "production",
  "enabled": true,
  "rules": [
    {
      "maintenance_type": "compact_data_files",
      "execution_backend": "spark",
      "conditions": [
        {"metric": "small_files", "operator": "gt", "value": 100},
        {"metric": "avg_file_size_mb", "operator": "lt", "value": 64, "logical_op": "AND"}
      ],
      "execution_config": {
        "target_file_size_mb": 512,
        "max_concurrent_file_group_rewrites": 5
      },
      "max_tables_per_run": 10
    }
  ],
  "schedule": {
    "schedule_type": "cron",
    "cron_expression": "0 4 * * 0"
  }
}
```

---

## Roadmap

### Phase 1: Policy Engine (Current Focus)

- [ ] Design policy database schema
- [ ] Implement CRUD APIs for policies
- [ ] Build policy evaluation engine
- [ ] Add policy UI in dashboard

### Phase 2: Scheduler Integration

- [ ] Create Airflow DAG templates
- [ ] Implement webhook triggers for EventBridge
- [ ] Add cron-based internal scheduler option
- [ ] Build execution plan generator

### Phase 3: Execution Backends

- [ ] Spark command generator (expire, compact, rewrite)
- [ ] Trino procedure generator (CALL statements)
- [ ] PyIceberg direct execution (for small tables)
- [ ] Dry-run mode for command preview

### Phase 4: Observability

- [ ] Execution history dashboard
- [ ] Cost tracking (S3 API calls, compute time)
- [ ] Alerting on failed maintenance
- [ ] Trend analysis (health score over time)

### Phase 5: Advanced Features

- [ ] Multi-catalog policy inheritance
- [ ] Tag-based policy targeting
- [ ] Maintenance windows (avoid peak hours)
- [ ] Approval workflows for critical tables

---

## Appendix

### Maintenance Types

| Type | Description | Trino Command | Spark Command |
|------|-------------|---------------|---------------|
| `expire_snapshots` | Remove old snapshots | `CALL system.expire_snapshots(...)` | `table.expireSnapshots().olderThan(ts).commit()` |
| `compact_data_files` | Merge small files | `CALL system.rewrite_data_files(...)` | `Actions.forTable(table).rewriteDataFiles().execute()` |
| `rewrite_manifests` | Consolidate manifests | `CALL system.rewrite_manifests(...)` | `Actions.forTable(table).rewriteManifests().execute()` |
| `remove_orphan_files` | Delete unreferenced files | `CALL system.remove_orphan_files(...)` | `Actions.forTable(table).removeOrphanFiles().execute()` |
| `rewrite_delete_files` | Merge delete files with data | N/A | `Actions.forTable(table).rewriteDataFiles().execute()` |

### Health Thresholds (Defaults)

| Metric | Warning | Critical |
|--------|---------|----------|
| Snapshot Count | 50 | 100 |
| Snapshot Age (days) | 30 | 90 |
| Small Files Count | 100 | 500 |
| Delete Files Count | 10 | 50 |
| Small File Size | < 128 MB | < 128 MB |

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FERN_CACHE_DB_PATH` | SQLite cache location | `app/data/health_cache.db` |
| `FERN_JOB_WORKERS` | Thread pool size | 4 |
| `FERN_DEFAULT_SCAN_MODE` | Default health scan mode | `light` |
| `FERN_CACHE_MAX_AGE_MINUTES` | Cache freshness threshold | 60 |
