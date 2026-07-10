"""Health and maintenance API endpoints."""

import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.models.health import (
    HealthStatus,
    HealthThresholds,
    MaintenanceType,
    TableHealth,
    TableHealthSummary,
)
from app.services import catalog_service
from app.services.health_service import HealthService
from app.services.health_cache import health_cache, CachedTableHealth, CacheMissingError, CacheExpiredError
from app.services.job_service import job_service, JobStatus

logger = logging.getLogger(__name__)
router = APIRouter()
_health_scan_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="health-scan")


class ScanMode(str, Enum):
    """Health scan modes."""
    CACHED = "cached"
    LIGHT = "light"
    FULL = "full"


class CachedTableResponse(BaseModel):
    """Response model for cached table health."""
    catalog: str
    namespace: str
    table_name: str
    status: str
    health_score: int
    total_snapshots: int
    total_data_files: int
    total_delete_files: int
    small_files_count: int
    total_size_gb: float
    avg_file_size_mb: float
    oldest_snapshot_age_days: Optional[float]
    days_since_last_write: Optional[float]
    issues_count: int
    warnings_count: int
    scan_mode: str
    scanned_at: str


class TableSearchResult(BaseModel):
    """Lightweight table search hit from cached health data."""

    catalog: str
    namespace: str
    table_name: str


def _cached_table_response(record: CachedTableHealth) -> CachedTableResponse:
    """Convert cached health records into API responses."""
    return CachedTableResponse(
        catalog=record.catalog,
        namespace=record.namespace,
        table_name=record.table_name,
        status=record.status,
        health_score=record.health_score,
        total_snapshots=record.total_snapshots,
        total_data_files=record.total_data_files,
        total_delete_files=record.total_delete_files,
        small_files_count=record.small_files_count,
        total_size_gb=record.total_size_gb,
        avg_file_size_mb=record.avg_file_size_mb,
        oldest_snapshot_age_days=record.oldest_snapshot_age_days,
        days_since_last_write=record.days_since_last_write,
        issues_count=record.issues_count,
        warnings_count=record.warnings_count,
        scan_mode=record.scan_mode,
        scanned_at=record.scanned_at.isoformat(),
    )


class CacheInfoResponse(BaseModel):
    """Response model for cache info."""
    catalog: str
    cached_tables: int
    cache_age_minutes: Optional[int]
    has_cache: bool


class ActiveHealthScanResponse(BaseModel):
    """Most recent active health-scan job for a catalog."""

    job_id: str
    mode: Literal["light", "full"]
    status: str
    progress: int
    message: str
    started_at: str


@router.get("/cache/info", response_model=CacheInfoResponse)
async def get_cache_info(
    catalog: str = Query(..., description="Catalog name"),
) -> CacheInfoResponse:
    """Get information about cached health data for a catalog."""
    cache_age = health_cache.get_cache_age(catalog)
    cached_tables = health_cache.get_table_count(catalog)
    
    return CacheInfoResponse(
        catalog=catalog,
        cached_tables=cached_tables,
        cache_age_minutes=cache_age,
        has_cache=cached_tables > 0,
    )


@router.delete("/cache")
async def clear_cache(
    catalog: str = Query(..., description="Catalog name"),
) -> dict:
    """Clear cached health data for a catalog."""
    deleted = health_cache.clear_catalog_cache(catalog)
    return {
        "catalog": catalog,
        "deleted_tables": deleted,
        "message": f"Cleared {deleted} cached table health records",
    }


@router.get("/scan/active", response_model=ActiveHealthScanResponse | None)
async def get_active_health_scan(
    catalog: str = Query(..., description="Catalog name"),
) -> ActiveHealthScanResponse | None:
    """Return the latest active health scan job for a catalog, if one exists."""
    job = job_service.find_latest_job(
        job_type="health_scan",
        catalog=catalog,
        statuses=[JobStatus.PENDING, JobStatus.RUNNING],
    )
    if not job:
        return None

    mode = (job.payload or {}).get("mode", "light")
    if mode not in {"light", "full"}:
        mode = "light"

    return ActiveHealthScanResponse(
        job_id=job.id,
        mode=mode,
        status=job.status.value,
        progress=job.progress,
        message=job.message,
        started_at=job.created_at.isoformat(),
    )


@router.get("/summary/stream")
async def stream_health_summary(
    catalog: str = Query(..., description="Catalog name"),
    mode: Literal["light", "full"] = Query(
        "light",
        description="Scan mode: light (metadata only, fast) or full (with S3 manifest reads)"
    ),
    # Snapshot thresholds
    snapshot_warning_threshold: Optional[int] = Query(50, ge=1),
    snapshot_critical_threshold: Optional[int] = Query(100, ge=1),
    snapshot_age_warning_days: Optional[int] = Query(30, ge=1),
    snapshot_age_critical_days: Optional[int] = Query(90, ge=1),
    # File size thresholds
    small_file_size_mb: Optional[int] = Query(128, ge=1),
    small_file_warning_threshold: Optional[int] = Query(100, ge=0),
    small_file_critical_threshold: Optional[int] = Query(500, ge=0),
    # Delete file thresholds
    delete_file_warning_threshold: Optional[int] = Query(10, ge=0),
    delete_file_critical_threshold: Optional[int] = Query(50, ge=0),
    # Manifest thresholds
    small_manifest_file_count: Optional[int] = Query(10, ge=1),
    small_manifest_warning_threshold: Optional[int] = Query(20, ge=0),
) -> StreamingResponse:
    """
    Stream health scan results namespace-by-namespace via Server-Sent Events.
    
    This endpoint provides real-time progress updates as each namespace is scanned,
    allowing the frontend to display partial results immediately instead of waiting
    for the entire scan to complete.
    
    Events:
    - progress: Initial counts (namespaces_total, tables_total)
    - namespace_complete: Results for one namespace (namespace, tables_scanned, healthy, warning, critical)
    - scan_complete: Final summary with all statistics
    - error: If an error occurs during scanning
    
    Example usage with EventSource:
    ```javascript
    const es = new EventSource('/api/health/summary/stream?catalog=glue&mode=light');
    es.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.type === 'namespace_complete') {
            // Update UI with partial results
        }
    };
    ```
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    thresholds = HealthThresholds(
        snapshot_warning_threshold=snapshot_warning_threshold or 50,
        snapshot_critical_threshold=snapshot_critical_threshold or 100,
        snapshot_age_warning_days=snapshot_age_warning_days or 30,
        snapshot_age_critical_days=snapshot_age_critical_days or 90,
        small_file_size_mb=small_file_size_mb or 128,
        small_file_warning_threshold=small_file_warning_threshold or 100,
        small_file_critical_threshold=small_file_critical_threshold or 500,
        delete_file_warning_threshold=delete_file_warning_threshold or 10,
        delete_file_critical_threshold=delete_file_critical_threshold or 50,
        small_manifest_file_count=small_manifest_file_count or 10,
        small_manifest_warning_threshold=small_manifest_warning_threshold or 20,
    )

    async def event_generator():
        """Stream scan events from a worker thread so Glue/DB work does not block the API."""
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[dict | None] = asyncio.Queue()

        def run_scan() -> None:
            try:
                health_service = HealthService(pyiceberg_catalog)
                for event in health_service.scan_all_tables_streaming(
                    catalog_name=catalog,
                    thresholds=thresholds,
                    mode=mode,
                ):
                    future = asyncio.run_coroutine_threadsafe(queue.put(event), loop)
                    future.result()
            except Exception as exc:
                future = asyncio.run_coroutine_threadsafe(
                    queue.put({"type": "error", "error": str(exc)}),
                    loop,
                )
                future.result()
            finally:
                future = asyncio.run_coroutine_threadsafe(queue.put(None), loop)
                future.result()

        scan_future = loop.run_in_executor(_health_scan_executor, run_scan)

        while True:
            event = await queue.get()
            if event is None:
                break
            event_type = event.get("type", "message")
            event_data = json.dumps(event)
            yield f"event: {event_type}\ndata: {event_data}\n\n"

        await scan_future

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/summary", response_model=TableHealthSummary)
async def get_health_summary(
    catalog: str = Query(..., description="Catalog name"),
    mode: ScanMode = Query(
        ScanMode.CACHED,
        description="Scan mode: cached (instant), light (metadata only), full (with S3 manifest reads)"
    ),
    max_cache_age_minutes: int = Query(
        1440,
        ge=1,
        description="Maximum cache age in minutes (for cached mode)"
    ),
    # Snapshot thresholds
    snapshot_warning_threshold: Optional[int] = Query(None, ge=1, description="Warning threshold for snapshot count"),
    snapshot_critical_threshold: Optional[int] = Query(None, ge=1, description="Critical threshold for snapshot count"),
    snapshot_age_warning_days: Optional[int] = Query(None, ge=1, description="Warning threshold for oldest snapshot age in days"),
    snapshot_age_critical_days: Optional[int] = Query(None, ge=1, description="Critical threshold for oldest snapshot age in days"),
    # File size thresholds
    small_file_size_mb: Optional[int] = Query(None, ge=1, description="Size threshold (MB) below which files are considered small"),
    small_file_warning_threshold: Optional[int] = Query(None, ge=0, description="Warning threshold for number of small files"),
    small_file_critical_threshold: Optional[int] = Query(None, ge=0, description="Critical threshold for number of small files"),
    # Delete file thresholds
    delete_file_warning_threshold: Optional[int] = Query(None, ge=0, description="Warning threshold for number of delete files"),
    delete_file_critical_threshold: Optional[int] = Query(None, ge=0, description="Critical threshold for number of delete files"),
    # Manifest thresholds
    small_manifest_file_count: Optional[int] = Query(None, ge=1, description="Number of files below which a manifest is considered small"),
    small_manifest_warning_threshold: Optional[int] = Query(None, ge=0, description="Warning threshold for number of small manifests"),
) -> TableHealthSummary:
    """Get overall health summary for all tables in catalog.
    
    Modes:
    - cached: Return cached results instantly (default). Returns 404 if no cache exists.
    - light: Fresh scan using metadata only (fast, no S3 manifest reads)
    - full: Fresh scan with file-level metrics (slow, reads S3 manifests)
    
    For large catalogs, use GET /health/summary/stream for real-time streaming results,
    or POST /health/scan/trigger to run a background scan that populates the cache.
    
    All threshold parameters are optional. If not provided, default values will be used.
    """
    # Try to return cached data first
    if mode == ScanMode.CACHED:
        try:
            cached = health_cache.get_cached_summary(catalog, max_age_minutes=max_cache_age_minutes)
            if cached:
                return cached
        except CacheMissingError as e:
            logger.warning("Cache missing for catalog '%s'", catalog)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "no_cache",
                    "message": f"No cached health data exists for catalog '{catalog}'. Use /health/summary/stream for streaming results or /health/scan/trigger to run a background scan.",
                    "alternatives": [
                        {"endpoint": "/health/summary/stream", "description": "Stream results namespace-by-namespace"},
                        {"endpoint": "/health/scan/trigger", "description": "Trigger background scan and cache results"},
                    ],
                },
            )
        except CacheExpiredError as e:
            logger.warning(
                "Cache expired for catalog '%s'. Last scanned at %s (%d mins ago, max age is %d mins)",
                catalog, e.scanned_at, e.age_minutes, e.max_age_minutes
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "cache_expired",
                    "message": f"Cached health data for catalog '{catalog}' has expired. Last scanned at {e.scanned_at.isoformat()} ({e.age_minutes} minutes ago, max age is {e.max_age_minutes} minutes). Please trigger a fresh scan.",
                    "scanned_at": e.scanned_at.isoformat(),
                    "age_minutes": e.age_minutes,
                    "max_age_minutes": e.max_age_minutes,
                    "alternatives": [
                        {"endpoint": "/health/scan/trigger", "description": "Trigger background scan to refresh cache"},
                        {"endpoint": "/health/summary?mode=light", "description": "Run an on-demand light metadata scan"},
                    ],
                },
            )
    
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    # Build thresholds object from query parameters
    thresholds = HealthThresholds(
        snapshot_warning_threshold=snapshot_warning_threshold or 50,
        snapshot_critical_threshold=snapshot_critical_threshold or 100,
        snapshot_age_warning_days=snapshot_age_warning_days or 30,
        snapshot_age_critical_days=snapshot_age_critical_days or 90,
        small_file_size_mb=small_file_size_mb or 128,
        small_file_warning_threshold=small_file_warning_threshold or 100,
        small_file_critical_threshold=small_file_critical_threshold or 500,
        delete_file_warning_threshold=delete_file_warning_threshold or 10,
        delete_file_critical_threshold=delete_file_critical_threshold or 50,
        small_manifest_file_count=small_manifest_file_count or 10,
        small_manifest_warning_threshold=small_manifest_warning_threshold or 20,
    )
    
    health_service = HealthService(pyiceberg_catalog)
    scan_mode = "light" if mode == ScanMode.LIGHT else "full"
    
    try:
        return health_service.get_health_summary(
            catalog,
            thresholds=thresholds,
            mode=scan_mode,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting health summary: {str(e)}",
        )


@router.get("/tables", response_model=list[TableHealth])
async def scan_tables_health(
    catalog: str = Query(..., description="Catalog name"),
    min_snapshots: Optional[int] = Query(
        None,
        ge=0,
        description="Filter tables with at least this many snapshots",
    ),
    status_filter: Optional[list[HealthStatus]] = Query(
        None,
        description="Filter by health status (healthy, warning, critical)",
    ),
    needs_maintenance: Optional[list[MaintenanceType]] = Query(
        None,
        description="Filter tables needing specific maintenance types",
    ),
) -> list[TableHealth]:
    """Scan all tables and return health assessments.
    
    This endpoint is useful for finding tables that need maintenance.
    
    Examples:
    - Get all tables with > 50 snapshots: ?min_snapshots=50
    - Get critical tables: ?status_filter=critical
    - Get tables needing compaction: ?needs_maintenance=compact_data_files
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    health_service = HealthService(pyiceberg_catalog)
    
    try:
        # Get all table health data
        results = health_service.scan_all_tables(catalog, min_snapshots=min_snapshots)
        
        # Apply status filter
        if status_filter:
            results = [r for r in results if r.status in status_filter]
        
        # Apply maintenance type filter
        if needs_maintenance:
            results = [
                r for r in results
                if any(
                    rec.type in needs_maintenance
                    for rec in r.recommendations
                )
            ]
        
        # Sort by health score (worst first)
        results.sort(key=lambda x: x.health_score)
        
        return results
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error scanning tables: {str(e)}",
        )


@router.post("/tables/{namespace}/{table}/scan", status_code=status.HTTP_202_ACCEPTED)
async def scan_table_health(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
    mode: Literal["light", "full"] = Query(
        "light",
        description="Scan mode: light (metadata only) or full (with S3 manifest reads)",
    ),
) -> dict:
    """
    Scan one table in the background and cache the result.

    Use this for per-table refresh without re-scanning the entire catalog.
    Track progress via GET /api/jobs/{job_id}.
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )

    job = job_service.create_job(
        initial_message=f"Starting {mode} health scan for {namespace}.{table}...",
        job_type="health_scan_table",
        payload={"mode": mode, "namespace": namespace, "table": table},
        catalog=catalog,
    )

    def scan_single_table(job_id: str):
        health_service = HealthService(pyiceberg_catalog)
        job_service.update_job(
            job_id,
            progress=10,
            message=f"Analyzing {namespace}.{table}...",
        )
        result = health_service.analyze_table_health(
            namespace,
            table,
            catalog,
            mode=mode,
        )
        health_cache.save_table_health(result, scan_mode=mode)
        job_service.update_job(
            job_id,
            progress=100,
            message=f"Health scan complete for {namespace}.{table}",
        )
        return {
            "namespace": namespace,
            "table": table,
            "status": result.status.value,
            "health_score": result.health_score,
            "scan_mode": mode,
            "cached": True,
        }

    job_service.run_in_background(job.id, scan_single_table)

    return {
        "job_id": job.id,
        "mode": mode,
        "namespace": namespace,
        "table": table,
        "message": f"{mode.capitalize()} health scan started for {namespace}.{table}.",
    }


@router.get("/tables/{namespace}/{table}", response_model=TableHealth)
async def get_table_health(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
) -> TableHealth:
    """Get detailed health assessment for a specific table."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    health_service = HealthService(pyiceberg_catalog)
    
    try:
        return health_service.analyze_table_health(namespace, table, catalog)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error analyzing table health: {str(e)}",
        )


@router.get("/tables/needing-expiration", response_model=list[TableHealth])
async def get_tables_needing_expiration(
    catalog: str = Query(..., description="Catalog name"),
    min_snapshots: int = Query(50, ge=1, description="Minimum snapshots threshold"),
) -> list[TableHealth]:
    """Get tables that need snapshot expiration.
    
    Convenience endpoint that filters for tables with:
    - More than min_snapshots snapshots
    - Recommendations for snapshot expiration
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    health_service = HealthService(pyiceberg_catalog)
    
    try:
        all_health = health_service.scan_all_tables(catalog, min_snapshots=min_snapshots)
        
        # Filter for tables needing expiration
        results = [
            h for h in all_health
            if any(r.type == MaintenanceType.EXPIRE_SNAPSHOTS for r in h.recommendations)
        ]
        
        # Sort by snapshot count (most first)
        results.sort(key=lambda x: x.metrics.total_snapshots, reverse=True)
        
        return results
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting tables needing expiration: {str(e)}",
        )


@router.get("/tables/needing-compaction", response_model=list[TableHealth])
async def get_tables_needing_compaction(
    catalog: str = Query(..., description="Catalog name"),
    min_small_files: int = Query(100, ge=1, description="Minimum small files threshold"),
) -> list[TableHealth]:
    """Get tables that need compaction (file rewriting).
    
    Returns tables with many small files that would benefit from compaction.
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    health_service = HealthService(pyiceberg_catalog)
    
    try:
        all_health = health_service.scan_all_tables(catalog)
        
        # Filter for tables needing compaction
        results = [
            h for h in all_health
            if h.metrics.small_files_count >= min_small_files
            or any(r.type == MaintenanceType.COMPACT_DATA_FILES for r in h.recommendations)
        ]
        
        # Sort by small file count (most first)
        results.sort(key=lambda x: x.metrics.small_files_count, reverse=True)
        
        return results
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting tables needing compaction: {str(e)}",
        )


@router.get("/tables/with-delete-files", response_model=list[TableHealth])
async def get_tables_with_delete_files(
    catalog: str = Query(..., description="Catalog name"),
) -> list[TableHealth]:
    """Get tables with delete files that may need rewriting.
    
    Delete files can impact read performance, especially when there are many of them.
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    health_service = HealthService(pyiceberg_catalog)
    
    try:
        all_health = health_service.scan_all_tables(catalog)
        
        # Filter for tables with delete files
        results = [
            h for h in all_health
            if h.metrics.total_delete_files > 0
        ]
        
        # Sort by delete file count (most first)
        results.sort(key=lambda x: x.metrics.total_delete_files, reverse=True)
        
        return results
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting tables with delete files: {str(e)}",
        )


@router.get("/tables/cached", response_model=list[CachedTableResponse])
async def get_cached_tables(
    catalog: str = Query(..., description="Catalog name"),
    status_filter: Optional[HealthStatus] = Query(None, description="Filter by health status"),
    min_snapshots: Optional[int] = Query(None, ge=0, description="Minimum snapshot count"),
    min_delete_files: Optional[int] = Query(None, ge=0, description="Minimum delete file count"),
    min_small_files: Optional[int] = Query(None, ge=0, description="Minimum small file count"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
) -> list[CachedTableResponse]:
    """
    Query cached table health with filters (instant response).
    
    This endpoint returns cached health data without making any S3 calls.
    Use POST /scan/trigger to refresh the cache.
    
    Filters can be combined:
    - status_filter: Filter by health status (healthy, warning, critical)
    - min_snapshots: Tables with at least this many snapshots
    - min_delete_files: Tables with at least this many delete files
    - min_small_files: Tables with at least this many small files
    """
    if status_filter:
        results = health_cache.get_tables_by_status(
            catalog,
            status=status_filter,
            limit=limit,
            offset=offset,
        )
    elif min_snapshots or min_delete_files or min_small_files:
        results = health_cache.get_tables_needing_maintenance(
            catalog,
            min_snapshots=min_snapshots,
            min_delete_files=min_delete_files,
            min_small_files=min_small_files,
            limit=limit,
            offset=offset,
        )
    else:
        results = health_cache.get_tables_by_status(
            catalog,
            limit=limit,
            offset=offset,
        )
    
    return [
        _cached_table_response(r)
        for r in results
    ]


@router.get("/tables/cached/{namespace}/{table}", response_model=Optional[CachedTableResponse])
async def get_cached_table(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
) -> Optional[CachedTableResponse]:
    """
    Get cached health for one table without triggering a health scan.

    Returns null when the table has not been scanned yet.
    """
    result = health_cache.get_table(catalog, namespace, table)
    return _cached_table_response(result) if result else None


@router.get("/tables/search", response_model=list[TableSearchResult])
async def search_cached_tables(
    catalog: str = Query(..., description="Catalog name"),
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(50, ge=1, le=200, description="Maximum results"),
) -> list[TableSearchResult]:
    """
    Search scanned tables by name from the health cache.

    Uses MySQL/SQLite only (no Glue calls), so it stays fast while a health
    scan is running and populating the cache incrementally.
    """
    results = await asyncio.to_thread(health_cache.search_tables, catalog, q, limit)
    return [
        TableSearchResult(
            catalog=row.catalog,
            namespace=row.namespace,
            table_name=row.table_name,
        )
        for row in results
    ]


@router.post("/scan/trigger", status_code=status.HTTP_202_ACCEPTED)
async def trigger_health_scan(
    catalog: str = Query(..., description="Catalog name"),
    mode: Literal["light", "full"] = Query(
        "light",
        description="Scan mode: light (metadata only, fast) or full (with S3 manifest reads)"
    ),
    snapshot_warning_threshold: Optional[int] = Query(50, ge=1),
    snapshot_critical_threshold: Optional[int] = Query(100, ge=1),
) -> dict:
    """
    Trigger a background health scan and cache results.
    
    Use this endpoint from Airflow/cron to keep the cache fresh.
    Returns a job_id for progress tracking via:
    - GET /api/jobs/{job_id} - Get current status
    - GET /api/jobs/{job_id}/stream - SSE stream for real-time updates
    
    Modes:
    - light: Metadata only scan (fast, no S3 manifest reads). Good for regular scheduled scans.
    - full: Full scan with file-level metrics (slow, reads S3 manifests). Use for detailed analysis.
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    existing_job = job_service.find_latest_job(
        job_type="health_scan",
        catalog=catalog,
        statuses=[JobStatus.PENDING, JobStatus.RUNNING],
    )
    if existing_job:
        return {
            "job_id": existing_job.id,
            "mode": (existing_job.payload or {}).get("mode", mode),
            "message": "A health scan is already running for this catalog.",
        }

    job = job_service.create_job(
        initial_message=f"Starting {mode} health scan...",
        job_type="health_scan",
        payload={"mode": mode},
        catalog=catalog,
    )
    
    thresholds = HealthThresholds(
        snapshot_warning_threshold=snapshot_warning_threshold,
        snapshot_critical_threshold=snapshot_critical_threshold,
    )
    
    def scan_and_cache(job_id: str):
        health_service = HealthService(pyiceberg_catalog)
        result = health_service.run_background_scan(
            catalog_name=catalog,
            mode=mode,
            thresholds=thresholds,
            job_id=job_id,
        )
        
        return {
            "total_tables": result.total_tables,
            "healthy_tables": result.healthy_tables,
            "warning_tables": result.warning_tables,
            "critical_tables": result.critical_tables,
            "tables_needing_snapshot_expiration": result.tables_needing_snapshot_expiration,
            "tables_needing_compaction": result.tables_needing_compaction,
            "scan_mode": result.scan_mode,
            "cached": True,
        }
    
    job_service.run_in_background(job.id, scan_and_cache)
    
    return {
        "job_id": job.id,
        "mode": mode,
        "message": f"{mode.capitalize()} health scan started. Results will be cached. Use the job_id to track progress.",
    }


@router.post("/summary/async", status_code=status.HTTP_202_ACCEPTED)
async def get_health_summary_async(
    catalog: str = Query(..., description="Catalog name"),
    mode: Literal["light", "full"] = Query(
        "light",
        description="Scan mode: light (metadata only) or full (with S3 manifest reads)"
    ),
    snapshot_warning_threshold: Optional[int] = Query(50, ge=1),
    snapshot_critical_threshold: Optional[int] = Query(100, ge=1),
) -> dict:
    """
    Get health summary in the background with progress tracking.
    
    Returns a job ID that can be used to track progress via:
    - GET /api/jobs/{job_id} - Get current status
    - GET /api/jobs/{job_id}/stream - SSE stream for real-time updates
    
    This is useful for large catalogs where health scanning may take
    a long time. Results are cached for instant retrieval.
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    job = job_service.create_job(
        initial_message=f"Starting {mode} health scan...",
        job_type="health_scan",
        payload={"mode": mode},
        catalog=catalog,
    )
    
    thresholds = HealthThresholds(
        snapshot_warning_threshold=snapshot_warning_threshold,
        snapshot_critical_threshold=snapshot_critical_threshold,
    )
    
    def scan_with_progress(job_id: str):
        health_service = HealthService(pyiceberg_catalog)
        
        result = health_service.run_background_scan(
            catalog_name=catalog,
            mode=mode,
            thresholds=thresholds,
            job_id=job_id,
        )
        
        return {
            "total_tables": result.total_tables,
            "healthy_tables": result.healthy_tables,
            "warning_tables": result.warning_tables,
            "critical_tables": result.critical_tables,
            "tables_needing_snapshot_expiration": result.tables_needing_snapshot_expiration,
            "tables_needing_compaction": result.tables_needing_compaction,
            "scan_mode": result.scan_mode,
        }
    
    job_service.run_in_background(job.id, scan_with_progress)
    
    return {
        "job_id": job.id,
        "mode": mode,
        "message": f"{mode.capitalize()} health scan started. Use the job_id to track progress.",
    }


@router.post("/tables/async", status_code=status.HTTP_202_ACCEPTED)
async def scan_tables_health_async(
    catalog: str = Query(..., description="Catalog name"),
    mode: Literal["light", "full"] = Query(
        "light",
        description="Scan mode: light (metadata only) or full (with S3 manifest reads)"
    ),
    min_snapshots: Optional[int] = Query(None, ge=0),
) -> dict:
    """
    Scan all tables health in the background with progress tracking.
    
    Returns a job ID for tracking progress. This is useful for large
    catalogs where scanning all tables may take a long time.
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    job = job_service.create_job(
        initial_message=f"Starting {mode} table health scan...",
        job_type="health_scan_tables",
        payload={"mode": mode, "min_snapshots": min_snapshots},
        catalog=catalog,
    )
    
    def scan_with_progress(job_id: str):
        health_service = HealthService(pyiceberg_catalog)
        
        results = health_service.scan_all_tables(
            catalog,
            min_snapshots=min_snapshots,
            mode=mode,
            job_id=job_id,
        )
        
        return {
            "total_tables_scanned": len(results),
            "healthy": sum(1 for r in results if r.status == HealthStatus.HEALTHY),
            "warning": sum(1 for r in results if r.status == HealthStatus.WARNING),
            "critical": sum(1 for r in results if r.status == HealthStatus.CRITICAL),
            "scan_mode": mode,
        }
    
    job_service.run_in_background(job.id, scan_with_progress)
    
    return {
        "job_id": job.id,
        "mode": mode,
        "message": f"{mode.capitalize()} table health scan started. Use the job_id to track progress.",
    }
