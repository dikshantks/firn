"""SQLite-based cache for table health scan results."""

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from app.models.health import (
    HealthStatus,
    TableHealth,
    TableHealthMetrics,
    TableHealthSummary,
    MaintenanceRecommendation,
)


@dataclass
class CachedTableHealth:
    """Cached table health data for quick queries."""
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
    recommendations_json: str
    scan_mode: str
    scanned_at: datetime


class HealthCache:
    """
    SQLite-based cache for health scan results.
    
    Stores table-level health data and catalog summaries for instant queries.
    Supports filtering by status, thresholds, and maintenance needs.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the health cache.
        
        Args:
            db_path: Path to SQLite database. Defaults to app data directory.
        """
        if db_path is None:
            cache_dir = Path(__file__).parent.parent / "data"
            cache_dir.mkdir(exist_ok=True)
            db_path = str(cache_dir / "health_cache.db")
        
        self.db_path = db_path
        self._init_db()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _init_db(self) -> None:
        """Initialize database schema."""
        conn = self._get_connection()
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS table_health (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    catalog TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    health_score INTEGER NOT NULL,
                    total_snapshots INTEGER NOT NULL,
                    total_data_files INTEGER NOT NULL,
                    total_delete_files INTEGER NOT NULL,
                    small_files_count INTEGER NOT NULL,
                    total_size_gb REAL NOT NULL,
                    avg_file_size_mb REAL NOT NULL,
                    oldest_snapshot_age_days REAL,
                    days_since_last_write REAL,
                    issues_count INTEGER NOT NULL,
                    warnings_count INTEGER NOT NULL,
                    recommendations_json TEXT,
                    scan_mode TEXT NOT NULL,
                    scanned_at TIMESTAMP NOT NULL,
                    UNIQUE(catalog, namespace, table_name)
                );
                
                CREATE INDEX IF NOT EXISTS idx_table_health_catalog 
                    ON table_health(catalog);
                CREATE INDEX IF NOT EXISTS idx_table_health_status 
                    ON table_health(catalog, status);
                CREATE INDEX IF NOT EXISTS idx_table_health_snapshots 
                    ON table_health(catalog, total_snapshots);
                CREATE INDEX IF NOT EXISTS idx_table_health_delete_files 
                    ON table_health(catalog, total_delete_files);
                CREATE INDEX IF NOT EXISTS idx_table_health_scanned 
                    ON table_health(catalog, scanned_at);
                
                CREATE TABLE IF NOT EXISTS catalog_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    catalog TEXT NOT NULL UNIQUE,
                    total_tables INTEGER NOT NULL,
                    healthy_tables INTEGER NOT NULL,
                    warning_tables INTEGER NOT NULL,
                    critical_tables INTEGER NOT NULL,
                    tables_needing_snapshot_expiration INTEGER NOT NULL,
                    tables_needing_compaction INTEGER NOT NULL,
                    tables_needing_manifest_rewrite INTEGER NOT NULL,
                    tables_with_delete_files INTEGER NOT NULL,
                    total_wasted_storage_gb REAL NOT NULL,
                    scan_mode TEXT NOT NULL,
                    scanned_at TIMESTAMP NOT NULL
                );
            """)
            conn.commit()
        finally:
            conn.close()
    
    def save_table_health(self, health: TableHealth, scan_mode: str = "full") -> None:
        """
        Save or update table health data.
        
        Args:
            health: TableHealth object to cache
            scan_mode: "light" or "full"
        """
        conn = self._get_connection()
        try:
            recommendations_json = json.dumps([
                {
                    "type": r.type.value,
                    "priority": r.priority,
                    "reason": r.reason,
                    "estimated_impact": r.estimated_impact,
                    "command_example": r.command_example,
                }
                for r in health.recommendations
            ])
            
            conn.execute("""
                INSERT OR REPLACE INTO table_health (
                    catalog, namespace, table_name, status, health_score,
                    total_snapshots, total_data_files, total_delete_files,
                    small_files_count, total_size_gb, avg_file_size_mb,
                    oldest_snapshot_age_days, days_since_last_write,
                    issues_count, warnings_count, recommendations_json,
                    scan_mode, scanned_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                health.catalog,
                health.namespace,
                health.table_name,
                health.status.value,
                health.health_score,
                health.metrics.total_snapshots,
                health.metrics.total_data_files,
                health.metrics.total_delete_files,
                health.metrics.small_files_count,
                health.metrics.total_size_gb,
                health.metrics.avg_file_size_mb,
                health.metrics.oldest_snapshot_age_days,
                health.metrics.days_since_last_write,
                health.issues_count,
                health.warnings_count,
                recommendations_json,
                scan_mode,
                datetime.utcnow().isoformat(),
            ))
            conn.commit()
        finally:
            conn.close()
    
    def save_catalog_summary(
        self,
        catalog: str,
        summary: TableHealthSummary,
        scan_mode: str = "full"
    ) -> None:
        """
        Save or update catalog summary.
        
        Args:
            catalog: Catalog name
            summary: TableHealthSummary object
            scan_mode: "light" or "full"
        """
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO catalog_summary (
                    catalog, total_tables, healthy_tables, warning_tables,
                    critical_tables, tables_needing_snapshot_expiration,
                    tables_needing_compaction, tables_needing_manifest_rewrite,
                    tables_with_delete_files, total_wasted_storage_gb,
                    scan_mode, scanned_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                catalog,
                summary.total_tables,
                summary.healthy_tables,
                summary.warning_tables,
                summary.critical_tables,
                summary.tables_needing_snapshot_expiration,
                summary.tables_needing_compaction,
                summary.tables_needing_manifest_rewrite,
                summary.tables_with_delete_files,
                summary.total_wasted_storage_gb,
                scan_mode,
                datetime.utcnow().isoformat(),
            ))
            conn.commit()
        finally:
            conn.close()
    
    def get_cached_summary(
        self,
        catalog: str,
        max_age_minutes: int = 60
    ) -> Optional[TableHealthSummary]:
        """
        Get cached catalog summary if fresh enough.
        
        Args:
            catalog: Catalog name
            max_age_minutes: Maximum cache age in minutes
            
        Returns:
            TableHealthSummary or None if not cached or too old
        """
        conn = self._get_connection()
        try:
            row = conn.execute("""
                SELECT * FROM catalog_summary
                WHERE catalog = ?
                AND datetime(scanned_at) > datetime('now', ?)
            """, (catalog, f"-{max_age_minutes} minutes")).fetchone()
            
            if not row:
                return None
            
            scanned_at = datetime.fromisoformat(row["scanned_at"])
            cache_age = (datetime.utcnow() - scanned_at).total_seconds() / 60
            
            return TableHealthSummary(
                total_tables=row["total_tables"],
                healthy_tables=row["healthy_tables"],
                warning_tables=row["warning_tables"],
                critical_tables=row["critical_tables"],
                tables_needing_snapshot_expiration=row["tables_needing_snapshot_expiration"],
                tables_needing_compaction=row["tables_needing_compaction"],
                tables_needing_manifest_rewrite=row["tables_needing_manifest_rewrite"],
                tables_with_delete_files=row["tables_with_delete_files"],
                total_wasted_storage_gb=row["total_wasted_storage_gb"],
                scan_mode=row["scan_mode"],
                cached_at=scanned_at,
                cache_age_minutes=int(cache_age),
            )
        finally:
            conn.close()
    
    def get_tables_by_status(
        self,
        catalog: str,
        status: Optional[HealthStatus] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[CachedTableHealth]:
        """
        Get cached table health filtered by status.
        
        Args:
            catalog: Catalog name
            status: Filter by health status (optional)
            limit: Maximum results
            offset: Pagination offset
            
        Returns:
            List of CachedTableHealth
        """
        conn = self._get_connection()
        try:
            if status:
                rows = conn.execute("""
                    SELECT * FROM table_health
                    WHERE catalog = ? AND status = ?
                    ORDER BY health_score ASC
                    LIMIT ? OFFSET ?
                """, (catalog, status.value, limit, offset)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT * FROM table_health
                    WHERE catalog = ?
                    ORDER BY health_score ASC
                    LIMIT ? OFFSET ?
                """, (catalog, limit, offset)).fetchall()
            
            return [self._row_to_cached_health(row) for row in rows]
        finally:
            conn.close()
    
    def get_tables_needing_maintenance(
        self,
        catalog: str,
        min_snapshots: Optional[int] = None,
        min_delete_files: Optional[int] = None,
        min_small_files: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[CachedTableHealth]:
        """
        Get tables exceeding maintenance thresholds.
        
        Args:
            catalog: Catalog name
            min_snapshots: Minimum snapshot count threshold
            min_delete_files: Minimum delete file count threshold
            min_small_files: Minimum small file count threshold
            limit: Maximum results
            offset: Pagination offset
            
        Returns:
            List of CachedTableHealth matching criteria
        """
        conn = self._get_connection()
        try:
            conditions = ["catalog = ?"]
            params: list[Any] = [catalog]
            
            if min_snapshots is not None:
                conditions.append("total_snapshots >= ?")
                params.append(min_snapshots)
            
            if min_delete_files is not None:
                conditions.append("total_delete_files >= ?")
                params.append(min_delete_files)
            
            if min_small_files is not None:
                conditions.append("small_files_count >= ?")
                params.append(min_small_files)
            
            where_clause = " AND ".join(conditions)
            params.extend([limit, offset])
            
            rows = conn.execute(f"""
                SELECT * FROM table_health
                WHERE {where_clause}
                ORDER BY health_score ASC
                LIMIT ? OFFSET ?
            """, params).fetchall()
            
            return [self._row_to_cached_health(row) for row in rows]
        finally:
            conn.close()
    
    def get_cache_age(self, catalog: str) -> Optional[int]:
        """
        Get age of cached data in minutes.
        
        Args:
            catalog: Catalog name
            
        Returns:
            Age in minutes or None if no cache
        """
        conn = self._get_connection()
        try:
            row = conn.execute("""
                SELECT scanned_at FROM catalog_summary
                WHERE catalog = ?
            """, (catalog,)).fetchone()
            
            if not row:
                return None
            
            scanned_at = datetime.fromisoformat(row["scanned_at"])
            return int((datetime.utcnow() - scanned_at).total_seconds() / 60)
        finally:
            conn.close()
    
    def get_table_count(self, catalog: str) -> int:
        """Get count of cached tables for a catalog."""
        conn = self._get_connection()
        try:
            row = conn.execute("""
                SELECT COUNT(*) as count FROM table_health
                WHERE catalog = ?
            """, (catalog,)).fetchone()
            return row["count"] if row else 0
        finally:
            conn.close()
    
    def clear_catalog_cache(self, catalog: str) -> int:
        """
        Clear all cached data for a catalog.
        
        Args:
            catalog: Catalog name
            
        Returns:
            Number of rows deleted
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute("""
                DELETE FROM table_health WHERE catalog = ?
            """, (catalog,))
            deleted = cursor.rowcount
            
            conn.execute("""
                DELETE FROM catalog_summary WHERE catalog = ?
            """, (catalog,))
            
            conn.commit()
            return deleted
        finally:
            conn.close()
    
    def _row_to_cached_health(self, row: sqlite3.Row) -> CachedTableHealth:
        """Convert database row to CachedTableHealth."""
        return CachedTableHealth(
            catalog=row["catalog"],
            namespace=row["namespace"],
            table_name=row["table_name"],
            status=row["status"],
            health_score=row["health_score"],
            total_snapshots=row["total_snapshots"],
            total_data_files=row["total_data_files"],
            total_delete_files=row["total_delete_files"],
            small_files_count=row["small_files_count"],
            total_size_gb=row["total_size_gb"],
            avg_file_size_mb=row["avg_file_size_mb"],
            oldest_snapshot_age_days=row["oldest_snapshot_age_days"],
            days_since_last_write=row["days_since_last_write"],
            issues_count=row["issues_count"],
            warnings_count=row["warnings_count"],
            recommendations_json=row["recommendations_json"],
            scan_mode=row["scan_mode"],
            scanned_at=datetime.fromisoformat(row["scanned_at"]),
        )


health_cache = HealthCache()
