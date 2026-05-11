---
name: Iceberg Maintenance Architecture Doc
overview: Create comprehensive architecture documentation for the Iceberg Maintenance system, including ER diagrams for the data model, system architecture diagrams showing current components and future integrations with schedulers (Airflow), Spark, and policy-based maintenance.
todos:
  - id: create-arch-doc
    content: Create docs/ARCHITECTURE.md with all diagrams and documentation
    status: completed
isProject: false
---

# Iceberg Maintenance Architecture Documentation

## Deliverable

Create `docs/ARCHITECTURE.md` with comprehensive documentation covering:

1. **Current System Architecture** - How Fern works today
2. **Data Model (ER Diagram)** - Database schema and Pydantic models
3. **Future Architecture** - Scheduler integration, Spark jobs, policy engine

---

## Document Structure

### Section 1: System Overview

High-level description of Fern as an Iceberg metadata visualization and maintenance platform.

### Section 2: Current Architecture Diagram

```mermaid
flowchart TB
    subgraph frontend [Frontend - React]
        UI[Health Dashboard]
        Stream[SSE Stream Handler]
    end
    
    subgraph backend [Backend - FastAPI]
        API[REST API]
        HealthRouter[Health Router]
        CatalogRouter[Catalog Router]
        JobRouter[Jobs Router]
    end
    
    subgraph services [Services Layer]
        HealthService[Health Service]
        CatalogService[Catalog Service]
        JobService[Job Service]
    end
    
    subgraph storage [Storage Layer]
        HealthCache[(SQLite Cache)]
        InMemory[(In-Memory Registry)]
    end
    
    subgraph external [External Systems]
        Glue[AWS Glue Catalog]
        Hive[Hive Metastore]
        S3[S3 / MinIO]
    end
    
    UI --> API
    Stream --> HealthRouter
    API --> HealthRouter
    API --> CatalogRouter
    API --> JobRouter
    
    HealthRouter --> HealthService
    CatalogRouter --> CatalogService
    JobRouter --> JobService
    
    HealthService --> HealthCache
    HealthService --> JobService
    CatalogService --> InMemory
    
    CatalogService --> Glue
    CatalogService --> Hive
    HealthService --> S3
```



### Section 3: Data Model (ER Diagram)

```mermaid
erDiagram
    CATALOG_SUMMARY ||--o{ TABLE_HEALTH : contains
    TABLE_HEALTH ||--|| TABLE_HEALTH_METRICS : has
    TABLE_HEALTH ||--o{ MAINTENANCE_RECOMMENDATION : generates
    
    CATALOG_SUMMARY {
        int id PK
        string catalog UK
        int total_tables
        int healthy_tables
        int warning_tables
        int critical_tables
        int tables_needing_snapshot_expiration
        int tables_needing_compaction
        int tables_needing_manifest_rewrite
        int tables_with_delete_files
        float total_wasted_storage_gb
        string scan_mode
        timestamp scanned_at
    }
    
    TABLE_HEALTH {
        int id PK
        string catalog FK
        string namespace
        string table_name
        string status
        int health_score
        int issues_count
        int warnings_count
        string scan_mode
        timestamp scanned_at
    }
    
    TABLE_HEALTH_METRICS {
        int total_snapshots
        int total_data_files
        int total_delete_files
        int small_files_count
        float total_size_gb
        float avg_file_size_mb
        float oldest_snapshot_age_days
        float days_since_last_write
    }
    
    MAINTENANCE_RECOMMENDATION {
        string type
        string priority
        string reason
        string estimated_impact
        string command_example
    }
```



### Section 4: Future Architecture - Scheduler Integration

```mermaid
flowchart TB
    subgraph scheduler [Scheduler Layer - Future]
        Airflow[Apache Airflow]
        Cron[Cron Jobs]
        EventBridge[AWS EventBridge]
    end
    
    subgraph fern [Fern Platform]
        PolicyEngine[Policy Engine]
        PolicyDB[(Policy Database)]
        API[REST API]
        HealthCache[(Health Cache)]
    end
    
    subgraph execution [Execution Layer - Future]
        Spark[Spark on EMR/Databricks]
        Trino[Trino Procedures]
        PyIceberg[PyIceberg Direct]
    end
    
    subgraph iceberg [Iceberg Tables]
        Tables[(Iceberg Tables on S3)]
    end
    
    Airflow -->|"1. Trigger scan"| API
    API -->|"2. Return health data"| Airflow
    Airflow -->|"3. Check policies"| PolicyEngine
    PolicyEngine -->|"4. Read rules"| PolicyDB
    PolicyEngine -->|"5. Generate jobs"| Airflow
    Airflow -->|"6. Execute maintenance"| Spark
    Airflow -->|"6. Execute maintenance"| Trino
    Spark --> Tables
    Trino --> Tables
```



### Section 5: Policy-Based Maintenance Schema (Future)

```mermaid
erDiagram
    MAINTENANCE_POLICY ||--o{ POLICY_RULE : contains
    MAINTENANCE_POLICY ||--o{ POLICY_SCHEDULE : has
    MAINTENANCE_POLICY ||--o{ POLICY_EXECUTION_LOG : generates
    POLICY_RULE ||--o{ RULE_CONDITION : has
    
    MAINTENANCE_POLICY {
        uuid id PK
        string name
        string description
        string scope_type "catalog|namespace|table"
        string scope_value
        boolean enabled
        timestamp created_at
        timestamp updated_at
    }
    
    POLICY_RULE {
        uuid id PK
        uuid policy_id FK
        string maintenance_type "expire_snapshots|compact|rewrite_manifests"
        string priority "high|medium|low"
        json thresholds
        json execution_config
    }
    
    RULE_CONDITION {
        uuid id PK
        uuid rule_id FK
        string metric "snapshot_count|small_files|delete_files|age_days"
        string operator "gt|lt|gte|lte|eq"
        float value
    }
    
    POLICY_SCHEDULE {
        uuid id PK
        uuid policy_id FK
        string schedule_type "cron|interval|event"
        string cron_expression
        int interval_minutes
        string event_trigger
    }
    
    POLICY_EXECUTION_LOG {
        uuid id PK
        uuid policy_id FK
        timestamp started_at
        timestamp completed_at
        string status "pending|running|success|failed"
        int tables_processed
        int tables_maintained
        json execution_details
        string error_message
    }
```



### Section 6: Component Descriptions

Document each component:

- **CatalogService**: In-memory registry for Iceberg catalog connections
- **HealthService**: Table health analysis with light/full scan modes
- **HealthCache**: SQLite persistence for health scan results
- **JobService**: In-process background task execution
- **Streaming**: SSE-based real-time progress updates

### Section 7: Future Roadmap

- **Phase 1**: Policy Engine (define rules, store in DB)
- **Phase 2**: Scheduler Integration (Airflow DAG templates)
- **Phase 3**: Execution Backends (Spark/Trino command generation)
- **Phase 4**: Observability (execution history, cost tracking)

---

## Files to Create


| File                   | Description                                  |
| ---------------------- | -------------------------------------------- |
| `docs/ARCHITECTURE.md` | Main architecture document with all diagrams |


## Key Diagrams Included

1. **System Architecture** - Current component layout
2. **ER Diagram - Current** - SQLite cache schema + Pydantic models
3. **Future Architecture** - Scheduler + execution layer
4. **ER Diagram - Future** - Policy-based maintenance schema
5. **Data Flow** - How health scans work end-to-end

