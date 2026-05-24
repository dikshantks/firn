# Fern API And MCP Use Cases

This document maps Fern's current API surface to data lake control-plane use cases and MCP tools for AI agents.

MCP is useful here because it turns Fern from "a UI and REST API humans click through" into "a safe tool surface agents can reason with." An agent can inspect catalog health, compare metadata, watch jobs, and prepare maintenance plans without receiving direct access to MySQL, Glue, S3, or production credentials.

## Why MCP Helps Fern

- Discovery: Agents can find catalogs, namespaces, and tables without knowing Fern's REST shapes.
- Triage: Agents can rank unhealthy tables, explain symptoms, and point to the relevant snapshots/files/manifests.
- Planning: Agents can generate maintenance plans from Fern's recommendations and optimization APIs.
- Job watching: Agents can start long scans, poll durable job state, and summarize completion or failure.
- Governance: Agents can produce human-readable reports for platform reviews, incidents, or weekly operations.
- Safety: Agents can operate through scoped tools that redact secrets, cap large outputs, and keep execution disabled until explicit approval workflows exist.

## Control Plane Boundaries

Fern does not store lake business data. The data lake remains in S3 and the source catalog remains in Glue or Hive. Fern's control-plane state lives in MySQL for production hardening:

- `catalog_registry`: known catalogs and non-secret connection metadata.
- `catalog_summary`: latest catalog-level health rollup.
- `table_health`: latest table-level health findings and recommendations.
- `jobs`: durable background job state for scans and async work.

In production, Glue and S3 access should use the backend pod IAM role through IRSA. Static keys and temporary credentials are local/testing conveniences only.

## Recommended MCP Tool Groups

### Catalog And Inventory

Use cases:
- "Which catalogs are registered?"
- "Is the prod Glue catalog reachable?"
- "List namespaces and tables before planning a scan."

Suggested MCP tools:
- `fern_list_catalogs`
- `fern_get_catalog`
- `fern_test_catalog`
- `fern_list_namespaces`
- `fern_list_tables`

### Metadata Investigation

Use cases:
- "Explain this table's schema, partitioning, and current snapshot."
- "Show the snapshot history before and after a suspicious write."
- "Inspect manifests and data files for table growth."

Suggested MCP tools:
- `fern_get_table_metadata`
- `fern_get_raw_table_metadata`
- `fern_get_snapshot_graph`
- `fern_get_snapshot_details`
- `fern_compare_snapshots`
- `fern_list_manifests`
- `fern_list_manifest_entries`
- `fern_list_data_files`
- `fern_inspect_data_file`
- `fern_sample_data_file`

### Health And Maintenance Triage

Use cases:
- "Find critical tables in the production catalog."
- "Which tables need snapshot expiration?"
- "Which tables have many small files or delete files?"
- "Refresh the health cache and report the result."

Suggested MCP tools:
- `fern_get_cache_info`
- `fern_get_catalog_health_summary`
- `fern_stream_health_summary`
- `fern_list_table_health`
- `fern_get_table_health`
- `fern_list_tables_needing_expiration`
- `fern_list_tables_needing_compaction`
- `fern_list_tables_with_delete_files`
- `fern_trigger_health_scan`

### Optimization Planning

Use cases:
- "Prepare Spark commands for compaction, but do not execute them."
- "Create a maintenance plan for the worst 20 tables."
- "Generate a PySpark script for one table after review."

Suggested MCP tools:
- `fern_get_batch_optimization_plan`
- `fern_get_table_optimization_plan`
- `fern_get_spark_commands`
- `fern_get_optimization_script`

### Jobs And Async Work

Use cases:
- "Watch this scan until it finishes."
- "Summarize failed jobs from the last run."
- "Cancel or clean up stale jobs once RBAC exists."

Suggested MCP tools:
- `fern_list_jobs`
- `fern_get_job`
- `fern_watch_job`
- `fern_delete_job`
- `fern_cleanup_jobs`

For the first MCP release, prefer read-only job tools plus job watching. Deleting/cleanup should require admin RBAC and audit logging.

## API Endpoint Inventory

### System

| Endpoint | Purpose | MCP Use |
| --- | --- | --- |
| `GET /` | API metadata and docs links. | Let agents confirm they reached Fern. |
| `GET /health` | Basic health check. | Simple liveness check. |
| `GET /healthz/live` | Kubernetes liveness probe. | Operational diagnostics. |
| `GET /healthz/ready` | Kubernetes readiness probe. | Confirm DB/API readiness before agent workflows. |

### Catalogs

| Endpoint | Purpose | MCP Tool |
| --- | --- | --- |
| `POST /api/catalogs` | Register a catalog. | `fern_register_catalog` for admin/local use only. |
| `GET /api/catalogs` | List registered catalogs. | `fern_list_catalogs` |
| `GET /api/catalogs/{name}` | Get one catalog. | `fern_get_catalog` |
| `GET /api/catalogs/{name}/test` | Test connectivity. | `fern_test_catalog` |
| `DELETE /api/catalogs/{name}` | Remove catalog. | `fern_delete_catalog` for admin use only. |
| `POST /api/catalogs/async` | Register catalog in background. | `fern_register_catalog_async` for large catalogs/admin use. |

Production note: MCP responses must redact credential-like fields from catalog properties. In prod, catalog config should rely on IRSA, not static AWS keys.

### Tables And Namespaces

| Endpoint | Purpose | MCP Tool |
| --- | --- | --- |
| `GET /api/tables/namespaces` | List namespaces for a catalog. | `fern_list_namespaces` |
| `GET /api/tables` | List tables for a catalog. | `fern_list_tables` |
| `GET /api/tables/{namespace}/{table}` | Get normalized table metadata. | `fern_get_table_metadata` |
| `GET /api/tables/{namespace}/{table}/metadata` | Get raw Iceberg metadata JSON. | `fern_get_raw_table_metadata` |

### Snapshots

| Endpoint | Purpose | MCP Tool |
| --- | --- | --- |
| `GET /api/tables/{namespace}/{table}/snapshots/details` | List detailed snapshots. | `fern_list_snapshot_details` |
| `GET /api/tables/{namespace}/{table}/snapshots` | Get snapshot graph. | `fern_get_snapshot_graph` |
| `GET /api/tables/{namespace}/{table}/snapshots/{snapshot_id}` | Get one snapshot. | `fern_get_snapshot_details` |
| `POST /api/tables/{namespace}/{table}/snapshots/compare` | Compare two snapshots. | `fern_compare_snapshots` |

Agent value: these tools explain table history, recent writes, snapshot growth, and maintenance risk.

### Manifests

| Endpoint | Purpose | MCP Tool |
| --- | --- | --- |
| `GET /api/tables/{namespace}/{table}/snapshots/{snapshot_id}/manifests` | Get manifest list for a snapshot. | `fern_list_manifests` |
| `GET /api/tables/{namespace}/{table}/manifests` | Read entries from a manifest path. | `fern_list_manifest_entries` |

Agent value: helps diagnose file layout, partition skew, and manifest rewrite needs.

### Data Files

| Endpoint | Purpose | MCP Tool |
| --- | --- | --- |
| `GET /api/tables/{namespace}/{table}/snapshots/{snapshot_id}/files` | List data files for a snapshot. | `fern_list_data_files` |
| `GET /api/tables/{namespace}/{table}/files/inspect` | Inspect a data file. | `fern_inspect_data_file` |
| `GET /api/tables/{namespace}/{table}/files/sample` | Sample rows from a file. | `fern_sample_data_file` |

MCP guardrail: cap results and avoid returning sensitive sample data unless explicitly allowed. For most agents, summary-level file statistics are safer than raw row samples.

### Puffin Statistics

| Endpoint | Purpose | MCP Tool |
| --- | --- | --- |
| `GET /api/tables/{namespace}/{table}/statistics` | List Puffin/statistics files. | `fern_list_puffin_files` |
| `GET /api/tables/{namespace}/{table}/statistics/{snapshot_id}` | Get decoded table statistics. | `fern_get_puffin_statistics` |

Agent value: explain whether NDV/statistics are available and useful for optimization decisions.

### Analytics

| Endpoint | Purpose | MCP Tool |
| --- | --- | --- |
| `GET /api/tables/{namespace}/{table}/analytics/storage` | Storage analytics. | `fern_get_storage_analytics` |
| `GET /api/tables/{namespace}/{table}/analytics/history` | Operation history. | `fern_get_operation_history` |

Agent value: produce concise reports on storage distribution, table growth, and write patterns.

### Health

| Endpoint | Purpose | MCP Tool |
| --- | --- | --- |
| `GET /api/health/cache/info` | Check cached health freshness. | `fern_get_cache_info` |
| `DELETE /api/health/cache` | Clear cached health. | `fern_clear_health_cache` for admin use only. |
| `GET /api/health/summary/stream` | Stream catalog health scan. | `fern_stream_health_summary` |
| `GET /api/health/summary` | Get catalog health summary. | `fern_get_catalog_health_summary` |
| `GET /api/health/tables` | Scan/list table health with filters. | `fern_list_table_health` |
| `GET /api/health/tables/{namespace}/{table}` | Get one table's health. | `fern_get_table_health` |
| `GET /api/health/tables/needing-expiration` | Tables needing snapshot expiration. | `fern_list_tables_needing_expiration` |
| `GET /api/health/tables/needing-compaction` | Tables needing compaction. | `fern_list_tables_needing_compaction` |
| `GET /api/health/tables/with-delete-files` | Tables with delete files. | `fern_list_tables_with_delete_files` |
| `GET /api/health/tables/cached` | Query cached table health. | `fern_query_cached_table_health` |
| `POST /api/health/scan/trigger` | Start background scan and cache result. | `fern_trigger_health_scan` |
| `POST /api/health/summary/async` | Start async health summary scan. | `fern_start_summary_scan` |
| `POST /api/health/tables/async` | Start async table health scan. | `fern_start_table_health_scan` |

Agent value: this is the main MCP surface for "what should I fix first?"

### Optimization

| Endpoint | Purpose | MCP Tool |
| --- | --- | --- |
| `GET /api/optimization/batch-optimization-plan` | Plan maintenance for multiple tables. | `fern_get_batch_optimization_plan` |
| `GET /api/optimization/{namespace}/{table}/optimization-plan` | Plan maintenance for one table. | `fern_get_table_optimization_plan` |
| `GET /api/optimization/{namespace}/{table}/spark-commands` | Get Spark commands. | `fern_get_spark_commands` |
| `GET /api/optimization/{namespace}/{table}/optimization-script` | Get full Spark/PySpark script. | `fern_get_optimization_script` |

MCP guardrail: these should return proposals only. Execution belongs behind a later approval/audit workflow.

### Jobs

| Endpoint | Purpose | MCP Tool |
| --- | --- | --- |
| `GET /api/jobs` | List recent jobs. | `fern_list_jobs` |
| `GET /api/jobs/{job_id}` | Get job state. | `fern_get_job` |
| `GET /api/jobs/{job_id}/stream` | Stream job progress. | `fern_watch_job` |
| `DELETE /api/jobs/{job_id}` | Delete/cancel job state. | `fern_delete_job` for admin use only. |
| `POST /api/jobs/cleanup` | Remove old jobs. | `fern_cleanup_jobs` for admin use only. |

Agent value: agents can start scans and report progress without tying up one HTTP request.

## Priority MCP Implementation Plan

Phase 1: Read-only observability
- `fern_list_catalogs`
- `fern_test_catalog`
- `fern_list_tables`
- `fern_get_table_metadata`
- `fern_get_catalog_health_summary`
- `fern_query_cached_table_health`
- `fern_get_table_health`
- `fern_get_job`
- `fern_watch_job`

Phase 2: Controlled scan workflows
- `fern_trigger_health_scan`
- `fern_start_summary_scan`
- `fern_start_table_health_scan`
- `fern_list_jobs`

Phase 3: Planning and recommendations
- `fern_get_batch_optimization_plan`
- `fern_get_table_optimization_plan`
- `fern_get_spark_commands`
- `fern_explain_table_issue`
- `fern_generate_maintenance_plan`

Phase 4: Admin or future execution
- Catalog registration/deletion.
- Cache clearing.
- Job cleanup/deletion.
- Actual maintenance execution only after RBAC, approval, audit log, and idempotency are in place.

## Example Agent Workflows

Catalog review:
1. `fern_list_catalogs`
2. `fern_test_catalog`
3. `fern_get_cache_info`
4. `fern_get_catalog_health_summary`
5. Produce an executive health summary.

Critical table triage:
1. `fern_query_cached_table_health` with `status_filter=critical`
2. `fern_get_table_health` for top tables
3. `fern_get_table_metadata`
4. `fern_get_table_optimization_plan`
5. Produce recommended actions and risk notes.

Fresh scan and report:
1. `fern_trigger_health_scan`
2. `fern_watch_job`
3. `fern_get_catalog_health_summary`
4. `fern_query_cached_table_health`
5. Summarize failures, critical tables, and next actions.

Snapshot investigation:
1. `fern_get_snapshot_graph`
2. `fern_get_snapshot_details`
3. `fern_compare_snapshots`
4. `fern_list_manifests`
5. Explain what changed and whether maintenance is needed.

## Safety Defaults

- Redact secrets in all MCP responses.
- Prefer cached health endpoints for broad queries.
- Require explicit catalog, namespace, and table inputs for targeted inspection.
- Limit large arrays and return summaries before raw details.
- Treat optimization commands as proposed text, not executable actions.
- Add audit logging before exposing admin or state-changing MCP tools.
