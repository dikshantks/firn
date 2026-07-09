"""Service for analyzing table health and generating maintenance recommendations."""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Generator, Literal, Optional

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

logger = logging.getLogger(__name__)

from app.models.health import (
    HealthStatus,
    HealthThresholds,
    MaintenanceRecommendation,
    MaintenanceType,
    TableHealth,
    TableHealthMetrics,
    TableHealthSummary,
)

ScanMode = Literal["light", "full"]


class HealthService:
    """Service for analyzing table health and maintenance needs."""
    
    # Number of tables to scan concurrently. Can be overridden via SCAN_CONCURRENCY env var.
    SCAN_CONCURRENCY: int = int(os.environ.get("SCAN_CONCURRENCY", "20"))

    # Configurable thresholds
    SNAPSHOT_WARNING_THRESHOLD = 50
    SNAPSHOT_CRITICAL_THRESHOLD = 100
    SNAPSHOT_AGE_WARNING_DAYS = 30
    SNAPSHOT_AGE_CRITICAL_DAYS = 90
    
    SMALL_FILE_SIZE_MB = 128
    SMALL_FILE_WARNING_THRESHOLD = 100
    SMALL_FILE_CRITICAL_THRESHOLD = 500
    
    DELETE_FILE_WARNING_THRESHOLD = 10
    DELETE_FILE_CRITICAL_THRESHOLD = 50
    
    SMALL_MANIFEST_FILE_COUNT = 10
    SMALL_MANIFEST_WARNING_THRESHOLD = 20
    
    def __init__(self, catalog: Catalog):
        """Initialize health service.
        
        Args:
            catalog: PyIceberg catalog instance
        """
        self.catalog = catalog
    
    def analyze_table_health(
        self,
        namespace: str,
        table_name: str,
        catalog_name: str,
        thresholds: Optional[HealthThresholds] = None,
        mode: ScanMode = "full",
    ) -> TableHealth:
        """Analyze health of a specific table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            catalog_name: Catalog name for identification
            thresholds: Optional custom thresholds (uses defaults if not provided)
            mode: "light" (metadata only, fast) or "full" (with S3 manifest reads)
            
        Returns:
            TableHealth with metrics and recommendations
        """
        table = self.catalog.load_table((namespace, table_name))
        
        # Use provided thresholds or defaults
        if thresholds is None:
            thresholds = HealthThresholds()
        
        # Collect metrics based on mode
        if mode == "light":
            metrics = self._collect_metrics_light(table)
        else:
            metrics = self._collect_metrics_full(table, thresholds)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(metrics, table, thresholds)
        
        # Calculate health score and status
        health_score, status = self._calculate_health(metrics, recommendations)
        
        # Count issues
        issues_count = sum(1 for r in recommendations if r.priority == "high")
        warnings_count = sum(1 for r in recommendations if r.priority == "medium")
        
        return TableHealth(
            catalog=catalog_name,
            namespace=namespace,
            table_name=table_name,
            status=status,
            health_score=health_score,
            metrics=metrics,
            recommendations=recommendations,
            issues_count=issues_count,
            warnings_count=warnings_count,
        )
    
    def scan_all_tables(
        self,
        catalog_name: str,
        min_snapshots: Optional[int] = None,
        thresholds: Optional[HealthThresholds] = None,
        mode: ScanMode = "full",
        job_id: Optional[str] = None,
    ) -> list[TableHealth]:
        """Scan all tables in catalog for health issues.
        
        Args:
            catalog_name: Catalog name
            min_snapshots: Only include tables with >= this many snapshots
            thresholds: Optional custom thresholds (uses defaults if not provided)
            mode: "light" (metadata only, fast) or "full" (with S3 manifest reads)
            job_id: Optional job ID for progress updates
            
        Returns:
            List of table health assessments
        """
        from app.services.job_service import job_service
        
        results = []
        namespaces = list(self.catalog.list_namespaces())
        total_namespaces = len(namespaces)
        
        # Count total tables for progress calculation
        total_tables = 0
        tables_by_namespace: dict[tuple, list] = {}
        for ns in namespaces:
            tables = list(self.catalog.list_tables(ns))
            tables_by_namespace[ns] = tables
            total_tables += len(tables)
        
        if job_id:
            job_service.update_job(
                job_id,
                progress=10,
                message=f"Found {total_namespaces} namespaces, {total_tables} tables. Starting {mode} scan..."
            )
        logger.info(
            "Found %d namespaces, %d tables. Starting %s scan (concurrency=%d)...",
            total_namespaces, total_tables, mode, self.SCAN_CONCURRENCY,
        )

        processed = 0

        def _analyze(namespace_str: str, table_name: str):
            return self.analyze_table_health(
                namespace_str,
                table_name,
                catalog_name,
                thresholds=thresholds,
                mode=mode,
            )

        # Build a flat list of (namespace_str, table_name) tasks across all namespaces
        all_tasks: list[tuple[str, str]] = []
        for namespace in namespaces:
            namespace_str = ".".join(namespace)
            for table_identifier in tables_by_namespace[namespace]:
                all_tasks.append((namespace_str, table_identifier[-1]))

        with ThreadPoolExecutor(max_workers=self.SCAN_CONCURRENCY) as pool:
            future_to_task = {
                pool.submit(_analyze, ns_str, tbl): (ns_str, tbl)
                for ns_str, tbl in all_tasks
            }
            for future in as_completed(future_to_task):
                ns_str, tbl = future_to_task[future]
                try:
                    health = future.result()
                    if min_snapshots is None or health.metrics.total_snapshots >= min_snapshots:
                        results.append(health)
                except Exception as exc:
                    logger.error("Error analyzing %s.%s: %s", ns_str, tbl, exc)
                finally:
                    processed += 1
                    # Update progress every 100 tables or at end of scan
                    if job_id and (processed % 100 == 0 or processed == total_tables):
                        progress = 10 + int((processed / total_tables) * 85)
                        job_service.update_job(
                            job_id,
                            progress=progress,
                            message=f"Scanned {processed}/{total_tables} tables..."
                        )

        if job_id:
            job_service.update_job(
                job_id,
                progress=95,
                message=f"Scan complete. Processing {len(results)} results..."
            )
        logger.info("Scan complete. %d tables processed, %d results collected.", processed, len(results))
        return results
    
    def scan_all_tables_streaming(
        self,
        catalog_name: str,
        thresholds: Optional[HealthThresholds] = None,
        mode: ScanMode = "light",
    ) -> Generator[dict, None, None]:
        """
        Stream health scan results namespace-by-namespace.
        
        This generator yields results incrementally as each namespace is scanned,
        allowing the frontend to display partial results immediately.
        
        Args:
            catalog_name: Catalog name
            thresholds: Optional custom thresholds
            mode: "light" (metadata only, fast) or "full" (with S3 manifest reads)
            
        Yields:
            - {"type": "progress", "namespaces_total": N, "tables_total": N}
            - {"type": "namespace_complete", "namespace": "...", "tables_scanned": N, 
               "healthy": N, "warning": N, "critical": N, "namespace_health": [...]}
            - {"type": "scan_complete", "summary": {...}}
        """
        from app.services.health_cache import health_cache
        
        namespaces = list(self.catalog.list_namespaces())
        total_namespaces = len(namespaces)
        
        # Count total tables for progress calculation
        total_tables = 0
        tables_by_namespace: dict[tuple, list] = {}
        for ns in namespaces:
            tables = list(self.catalog.list_tables(ns))
            tables_by_namespace[ns] = tables
            total_tables += len(tables)
        
        # Yield initial progress info
        yield {
            "type": "progress",
            "namespaces_total": total_namespaces,
            "tables_total": total_tables,
            "mode": mode,
        }
        logger.info(
            "Streaming scan: %d namespaces, %d tables, mode=%s, concurrency=%d",
            total_namespaces, total_tables, mode, self.SCAN_CONCURRENCY,
        )

        # Clear old cache for this catalog before starting
        health_cache.clear_catalog_cache(catalog_name)

        all_health: list[TableHealth] = []
        processed_tables = 0

        for ns_idx, namespace in enumerate(namespaces):
            namespace_str = ".".join(namespace)
            tables = tables_by_namespace[namespace]
            namespace_health: list[TableHealth] = []

            def _analyze_ns(table_identifier, _ns=namespace_str):
                table_name = table_identifier[-1]
                return self.analyze_table_health(
                    _ns,
                    table_name,
                    catalog_name,
                    thresholds=thresholds,
                    mode=mode,
                )

            with ThreadPoolExecutor(max_workers=self.SCAN_CONCURRENCY) as pool:
                future_to_id = {pool.submit(_analyze_ns, tid): tid for tid in tables}
                for future in as_completed(future_to_id):
                    tid = future_to_id[future]
                    try:
                        health = future.result()
                        namespace_health.append(health)
                        all_health.append(health)
                        health_cache.save_table_health(health, scan_mode=mode)
                    except Exception as exc:
                        logger.error("Error analyzing %s: %s", tid, exc)
                    finally:
                        processed_tables += 1

            # Compute namespace-level summary
            ns_healthy = sum(1 for h in namespace_health if h.status == HealthStatus.HEALTHY)
            ns_warning = sum(1 for h in namespace_health if h.status == HealthStatus.WARNING)
            ns_critical = sum(1 for h in namespace_health if h.status == HealthStatus.CRITICAL)

            # Yield namespace completion event
            yield {
                "type": "namespace_complete",
                "namespace": namespace_str,
                "namespace_index": ns_idx + 1,
                "namespaces_total": total_namespaces,
                "tables_scanned": len(namespace_health),
                "tables_total_scanned": processed_tables,
                "tables_total": total_tables,
                "healthy": ns_healthy,
                "warning": ns_warning,
                "critical": ns_critical,
                "progress_percent": int((processed_tables / total_tables) * 100) if total_tables > 0 else 100,
            }
        
        # Compute and cache final summary
        summary = self._compute_summary(all_health, mode=mode)
        health_cache.save_catalog_summary(catalog_name, summary, scan_mode=mode)
        
        # Yield final summary
        yield {
            "type": "scan_complete",
            "summary": {
                "total_tables": summary.total_tables,
                "healthy_tables": summary.healthy_tables,
                "warning_tables": summary.warning_tables,
                "critical_tables": summary.critical_tables,
                "tables_needing_snapshot_expiration": summary.tables_needing_snapshot_expiration,
                "tables_needing_compaction": summary.tables_needing_compaction,
                "tables_needing_manifest_rewrite": summary.tables_needing_manifest_rewrite,
                "tables_with_delete_files": summary.tables_with_delete_files,
                "total_wasted_storage_gb": summary.total_wasted_storage_gb,
                "scan_mode": summary.scan_mode,
            },
        }
    
    def get_health_summary(
        self,
        catalog_name: str,
        thresholds: Optional[HealthThresholds] = None,
        mode: ScanMode = "full",
        job_id: Optional[str] = None,
    ) -> TableHealthSummary:
        """Get summary of health across all tables.
        
        Args:
            catalog_name: Catalog name
            thresholds: Optional custom thresholds (uses defaults if not provided)
            mode: "light" (metadata only, fast) or "full" (with S3 manifest reads)
            job_id: Optional job ID for progress updates
            
        Returns:
            Summary statistics
        """
        all_health = self.scan_all_tables(
            catalog_name,
            thresholds=thresholds,
            mode=mode,
            job_id=job_id,
        )
        
        return self._compute_summary(all_health, mode=mode)
    
    def _compute_summary(
        self,
        all_health: list[TableHealth],
        mode: ScanMode = "full",
    ) -> TableHealthSummary:
        """Compute summary statistics from health results.
        
        Args:
            all_health: List of table health assessments
            mode: Scan mode used
            
        Returns:
            Summary statistics
        """
        healthy = sum(1 for h in all_health if h.status == HealthStatus.HEALTHY)
        warning = sum(1 for h in all_health if h.status == HealthStatus.WARNING)
        critical = sum(1 for h in all_health if h.status == HealthStatus.CRITICAL)
        
        needs_expiration = sum(
            1 for h in all_health
            if any(r.type == MaintenanceType.EXPIRE_SNAPSHOTS for r in h.recommendations)
        )
        
        needs_compaction = sum(
            1 for h in all_health
            if any(r.type == MaintenanceType.COMPACT_DATA_FILES for r in h.recommendations)
        )
        
        needs_manifest_rewrite = sum(
            1 for h in all_health
            if any(r.type == MaintenanceType.REWRITE_MANIFESTS for r in h.recommendations)
        )
        
        with_delete_files = sum(
            1 for h in all_health
            if h.metrics.total_delete_files > 0
        )
        
        # Estimate wasted storage (very rough estimate)
        wasted_storage = sum(
            h.metrics.small_files_count * 0.064  # Assume avg 64MB wasted per small file
            for h in all_health
        )
        
        return TableHealthSummary(
            total_tables=len(all_health),
            healthy_tables=healthy,
            warning_tables=warning,
            critical_tables=critical,
            tables_needing_snapshot_expiration=needs_expiration,
            tables_needing_compaction=needs_compaction,
            tables_needing_manifest_rewrite=needs_manifest_rewrite,
            tables_with_delete_files=with_delete_files,
            total_wasted_storage_gb=wasted_storage,
            scan_mode=mode,
        )
    
    def run_background_scan(
        self,
        catalog_name: str,
        mode: ScanMode = "light",
        thresholds: Optional[HealthThresholds] = None,
        job_id: Optional[str] = None,
    ) -> TableHealthSummary:
        """
        Run a health scan and cache results.
        
        This method is designed to be called from a background job.
        It scans all tables, caches individual results, and stores
        the catalog summary for instant retrieval.
        
        Args:
            catalog_name: Catalog name
            mode: "light" (metadata only) or "full" (with S3 manifest reads)
            thresholds: Optional custom thresholds
            job_id: Job ID for progress updates
            
        Returns:
            TableHealthSummary
        """
        from app.services.health_cache import health_cache
        from app.services.job_service import job_service
        
        if job_id:
            job_service.update_job(
                job_id,
                progress=5,
                message=f"Starting {mode} health scan for catalog '{catalog_name}'..."
            )
        
        # Clear old cache for this catalog
        health_cache.clear_catalog_cache(catalog_name)
        
        # Run the scan
        all_health = self.scan_all_tables(
            catalog_name,
            thresholds=thresholds,
            mode=mode,
            job_id=job_id,
        )
        
        if job_id:
            job_service.update_job(
                job_id,
                progress=96,
                message=f"Caching {len(all_health)} table health results..."
            )
        
        # Cache individual table health
        for health in all_health:
            health_cache.save_table_health(health, scan_mode=mode)
        
        # Compute and cache summary
        summary = self._compute_summary(all_health, mode=mode)
        health_cache.save_catalog_summary(catalog_name, summary, scan_mode=mode)
        
        if job_id:
            job_service.update_job(
                job_id,
                progress=100,
                message=f"Scan complete. {len(all_health)} tables cached."
            )
        
        return summary
    
    def _collect_metrics_light(self, table: Table) -> TableHealthMetrics:
        """
        Collect health metrics from metadata only - NO S3 manifest reads.
        
        This is much faster than full metrics collection as it only uses
        data already present in the table metadata (metadata.json).
        
        Args:
            table: PyIceberg table
            
        Returns:
            Health metrics (file-level metrics will be estimates from snapshot summary)
        """
        metadata = table.metadata
        snapshots = list(metadata.snapshots)
        
        # Snapshot metrics (from metadata - no S3 calls)
        total_snapshots = len(snapshots)
        
        oldest_snapshot_age_days = None
        if snapshots:
            oldest_timestamp = min(s.timestamp_ms for s in snapshots)
            oldest_snapshot_age_days = (
                datetime.now() - datetime.fromtimestamp(oldest_timestamp / 1000)
            ).days
        
        now = datetime.now()
        snapshots_last_7_days = sum(
            1 for s in snapshots
            if (now - datetime.fromtimestamp(s.timestamp_ms / 1000)).days <= 7
        )
        snapshots_last_30_days = sum(
            1 for s in snapshots
            if (now - datetime.fromtimestamp(s.timestamp_ms / 1000)).days <= 30
        )
        
        # File metrics from snapshot summary (already in metadata.json)
        total_data_files = 0
        total_delete_files = 0
        total_size_bytes = 0
        
        current_snapshot = table.current_snapshot()
        if current_snapshot and current_snapshot.summary:
            summary = current_snapshot.summary
            total_data_files = int(summary.get("total-data-files", 0))
            total_delete_files = int(summary.get("total-delete-files", 0))
            total_size_bytes = int(summary.get("total-files-size", 0))
        
        avg_file_size_mb = (
            (total_size_bytes / total_data_files / 1024 / 1024)
            if total_data_files > 0
            else 0
        )
        total_size_gb = total_size_bytes / 1024 / 1024 / 1024
        
        # Days since last write
        days_since_last_write = None
        if snapshots:
            latest_timestamp = max(s.timestamp_ms for s in snapshots)
            days_since_last_write = (
                now - datetime.fromtimestamp(latest_timestamp / 1000)
            ).days
        
        return TableHealthMetrics(
            total_snapshots=total_snapshots,
            oldest_snapshot_age_days=oldest_snapshot_age_days,
            snapshots_last_7_days=snapshots_last_7_days,
            snapshots_last_30_days=snapshots_last_30_days,
            total_data_files=total_data_files,
            total_delete_files=total_delete_files,
            small_files_count=0,  # Not available without manifest scan
            avg_file_size_mb=avg_file_size_mb,
            total_size_gb=total_size_gb,
            total_manifests=0,  # Not available without manifest scan
            small_manifests_count=0,  # Not available without manifest scan
            days_since_last_write=days_since_last_write,
        )
    
    def _collect_metrics_full(self, table: Table, thresholds: Optional[HealthThresholds] = None) -> TableHealthMetrics:
        """
        Collect full health metrics including file-level analysis.
        
        This reads manifest files from S3 to get accurate file counts and sizes.
        Much slower than light mode but provides small file counts.
        
        Args:
            table: PyIceberg table
            thresholds: Thresholds for small file detection
            
        Returns:
            Health metrics with full file-level details
        """
        metadata = table.metadata
        snapshots = list(metadata.snapshots)
        
        # Snapshot metrics
        total_snapshots = len(snapshots)
        
        oldest_snapshot_age_days = None
        if snapshots:
            oldest_timestamp = min(s.timestamp_ms for s in snapshots)
            oldest_snapshot_age_days = (
                datetime.now() - datetime.fromtimestamp(oldest_timestamp / 1000)
            ).days
        
        now = datetime.now()
        snapshots_last_7_days = sum(
            1 for s in snapshots
            if (now - datetime.fromtimestamp(s.timestamp_ms / 1000)).days <= 7
        )
        snapshots_last_30_days = sum(
            1 for s in snapshots
            if (now - datetime.fromtimestamp(s.timestamp_ms / 1000)).days <= 30
        )
        
        # File metrics - scan current snapshot (reads manifests from S3)
        total_data_files = 0
        total_delete_files = 0
        small_files_count = 0
        total_size_bytes = 0
        
        try:
            scan = table.scan()
            for task in scan.plan_files():
                file = task.file
                total_data_files += 1
                total_size_bytes += file.file_size_in_bytes
                
                # Check if small file
                small_file_size_bytes = (
                    (thresholds.small_file_size_mb if thresholds else self.SMALL_FILE_SIZE_MB) * 1024 * 1024
                )
                if file.file_size_in_bytes < small_file_size_bytes:
                    small_files_count += 1
        except Exception as exc:
            logger.error("Error scanning files for table: %s", exc)
        
        # Get delete file count from snapshot summary as fallback
        current_snapshot = table.current_snapshot()
        if current_snapshot and current_snapshot.summary:
            total_delete_files = int(current_snapshot.summary.get("total-delete-files", 0))
        
        avg_file_size_mb = (
            (total_size_bytes / total_data_files / 1024 / 1024)
            if total_data_files > 0
            else 0
        )
        total_size_gb = total_size_bytes / 1024 / 1024 / 1024
        
        # Manifest metrics - simplified
        total_manifests = 0
        small_manifests_count = 0
        
        # Days since last write
        days_since_last_write = None
        if snapshots:
            latest_timestamp = max(s.timestamp_ms for s in snapshots)
            days_since_last_write = (
                now - datetime.fromtimestamp(latest_timestamp / 1000)
            ).days
        
        return TableHealthMetrics(
            total_snapshots=total_snapshots,
            oldest_snapshot_age_days=oldest_snapshot_age_days,
            snapshots_last_7_days=snapshots_last_7_days,
            snapshots_last_30_days=snapshots_last_30_days,
            total_data_files=total_data_files,
            total_delete_files=total_delete_files,
            small_files_count=small_files_count,
            avg_file_size_mb=avg_file_size_mb,
            total_size_gb=total_size_gb,
            total_manifests=total_manifests,
            small_manifests_count=small_manifests_count,
            days_since_last_write=days_since_last_write,
        )
    
    def _collect_metrics(self, table: Table, thresholds: Optional[HealthThresholds] = None) -> TableHealthMetrics:
        """Collect health metrics from table (full mode for backward compatibility)."""
        return self._collect_metrics_full(table, thresholds)
    
    def _generate_recommendations(
        self,
        metrics: TableHealthMetrics,
        table: Table,
        thresholds: Optional[HealthThresholds] = None,
    ) -> list[MaintenanceRecommendation]:
        """Generate maintenance recommendations based on metrics.
        
        Args:
            metrics: Collected metrics
            table: PyIceberg table for context
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        # Use provided thresholds or defaults
        if thresholds is None:
            thresholds = HealthThresholds()
        
        # Check snapshot count
        if metrics.total_snapshots >= thresholds.snapshot_critical_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.EXPIRE_SNAPSHOTS,
                    priority="high",
                    reason=f"Table has {metrics.total_snapshots} snapshots (threshold: {thresholds.snapshot_critical_threshold})",
                    estimated_impact=f"Will remove old snapshots and free up metadata storage",
                    command_example=f"table.expire_snapshots(older_than='30 days ago')",
                )
            )
        elif metrics.total_snapshots >= thresholds.snapshot_warning_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.EXPIRE_SNAPSHOTS,
                    priority="medium",
                    reason=f"Table has {metrics.total_snapshots} snapshots (threshold: {thresholds.snapshot_warning_threshold})",
                    estimated_impact=f"Will remove old snapshots and free up metadata storage",
                    command_example=f"table.expire_snapshots(older_than='30 days ago')",
                )
            )
        
        # Check snapshot age
        if (
            metrics.oldest_snapshot_age_days
            and metrics.oldest_snapshot_age_days >= thresholds.snapshot_age_critical_days
        ):
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.EXPIRE_SNAPSHOTS,
                    priority="high",
                    reason=f"Oldest snapshot is {metrics.oldest_snapshot_age_days:.0f} days old (threshold: {thresholds.snapshot_age_critical_days})",
                    estimated_impact="Remove very old snapshots that are likely not needed",
                    command_example=f"table.expire_snapshots(older_than='90 days ago')",
                )
            )
        elif (
            metrics.oldest_snapshot_age_days
            and metrics.oldest_snapshot_age_days >= thresholds.snapshot_age_warning_days
        ):
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.EXPIRE_SNAPSHOTS,
                    priority="medium",
                    reason=f"Oldest snapshot is {metrics.oldest_snapshot_age_days:.0f} days old (threshold: {thresholds.snapshot_age_warning_days})",
                    estimated_impact="Remove old snapshots to reduce metadata overhead",
                    command_example=f"table.expire_snapshots(older_than='30 days ago')",
                )
            )
        
        # Check small files
        if metrics.small_files_count >= thresholds.small_file_critical_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.COMPACT_DATA_FILES,
                    priority="high",
                    reason=f"Table has {metrics.small_files_count} small files (< {thresholds.small_file_size_mb}MB each, threshold: {thresholds.small_file_critical_threshold})",
                    estimated_impact=f"Combine small files into larger ones, improve query performance by ~30-50%",
                    command_example=f"table.rewrite_data_files(target_size_bytes=512*1024*1024)",
                )
            )
        elif metrics.small_files_count >= thresholds.small_file_warning_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.COMPACT_DATA_FILES,
                    priority="medium",
                    reason=f"Table has {metrics.small_files_count} small files (< {thresholds.small_file_size_mb}MB each, threshold: {thresholds.small_file_warning_threshold})",
                    estimated_impact=f"Combine small files into larger ones, improve query performance",
                    command_example=f"table.rewrite_data_files(target_size_bytes=512*1024*1024)",
                )
            )
        
        # Check delete files
        if metrics.total_delete_files >= thresholds.delete_file_critical_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.REWRITE_DELETE_FILES,
                    priority="high",
                    reason=f"Table has {metrics.total_delete_files} delete files (threshold: {thresholds.delete_file_critical_threshold})",
                    estimated_impact="Merge delete files with data files, improve read performance significantly",
                    command_example=f"table.rewrite_data_files()",
                )
            )
        elif metrics.total_delete_files >= thresholds.delete_file_warning_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.REWRITE_DELETE_FILES,
                    priority="medium",
                    reason=f"Table has {metrics.total_delete_files} delete files (threshold: {thresholds.delete_file_warning_threshold})",
                    estimated_impact="Merge delete files with data files, improve read performance",
                    command_example=f"table.rewrite_data_files()",
                )
            )
        
        # Check small manifests
        if metrics.small_manifests_count >= thresholds.small_manifest_warning_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.REWRITE_MANIFESTS,
                    priority="medium",
                    reason=f"Table has {metrics.small_manifests_count} small manifests (threshold: {thresholds.small_manifest_warning_threshold})",
                    estimated_impact="Consolidate manifest files, reduce planning overhead",
                    command_example=f"table.rewrite_manifests()",
                )
            )
        
        # Check for stale tables (no writes in a while) with many snapshots
        if (
            metrics.days_since_last_write
            and metrics.days_since_last_write > 7
            and metrics.total_snapshots > 10
        ):
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.EXPIRE_SNAPSHOTS,
                    priority="low",
                    reason=f"Table hasn't been written to in {metrics.days_since_last_write:.0f} days but has {metrics.total_snapshots} snapshots",
                    estimated_impact="Clean up snapshots for inactive table",
                    command_example=f"table.expire_snapshots(older_than='7 days ago')",
                )
            )
        
        return recommendations
    
    def _calculate_health(
        self,
        metrics: TableHealthMetrics,
        recommendations: list[MaintenanceRecommendation],
    ) -> tuple[int, HealthStatus]:
        """Calculate health score and status.
        
        Args:
            metrics: Health metrics
            recommendations: Generated recommendations
            
        Returns:
            Tuple of (health_score, status)
        """
        score = 100
        
        # Deduct points for issues
        high_priority = sum(1 for r in recommendations if r.priority == "high")
        medium_priority = sum(1 for r in recommendations if r.priority == "medium")
        low_priority = sum(1 for r in recommendations if r.priority == "low")
        
        score -= high_priority * 20
        score -= medium_priority * 10
        score -= low_priority * 5
        
        score = max(0, min(100, score))
        
        # Determine status
        if score >= 80:
            status = HealthStatus.HEALTHY
        elif score >= 60:
            status = HealthStatus.WARNING
        else:
            status = HealthStatus.CRITICAL
        
        return score, status