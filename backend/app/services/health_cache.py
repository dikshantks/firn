"""Table health cache backed by MySQL when configured, SQLite otherwise."""

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import delete, func, or_, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from app.db import is_database_enabled, session_scope
from app.db.models import CatalogSummary, TableHealthRecord
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
        self.use_database = is_database_enabled()

        if db_path is None:
            cache_dir = Path(__file__).parent.parent / "data"
            cache_dir.mkdir(exist_ok=True)
            db_path = str(cache_dir / "health_cache.db")

        self.db_path = db_path
        if not self.use_database:
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
        if self.use_database:
            self._save_table_health_db(health, scan_mode)
            return

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
        if self.use_database:
            self._save_catalog_summary_db(catalog, summary, scan_mode)
            return

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
        if self.use_database:
            return self._get_cached_summary_db(catalog, max_age_minutes)

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
        if self.use_database:
            return self._get_tables_by_status_db(catalog, status, limit, offset)

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

    def get_table(
        self,
        catalog: str,
        namespace: str,
        table_name: str,
    ) -> Optional[CachedTableHealth]:
        """Get cached health for one table without running a fresh scan."""
        if self.use_database:
            return self._get_table_db(catalog, namespace, table_name)

        conn = self._get_connection()
        try:
            row = conn.execute("""
                SELECT * FROM table_health
                WHERE catalog = ? AND namespace = ? AND table_name = ?
            """, (catalog, namespace, table_name)).fetchone()
            return self._row_to_cached_health(row) if row else None
        finally:
            conn.close()

    def search_tables(
        self,
        catalog: str,
        query: str,
        limit: int = 50,
    ) -> list[CachedTableHealth]:
        """Search cached table names without calling Glue."""
        normalized = query.strip().lower()
        if len(normalized) < 2:
            return []

        if self.use_database:
            return self._search_tables_db(catalog, normalized, limit)

        pattern = f"%{normalized}%"
        conn = self._get_connection()
        try:
            rows = conn.execute(
                """
                SELECT * FROM table_health
                WHERE catalog = ?
                  AND (
                    LOWER(table_name) LIKE ?
                    OR LOWER(namespace) LIKE ?
                    OR LOWER(namespace || '.' || table_name) LIKE ?
                  )
                ORDER BY namespace ASC, table_name ASC
                LIMIT ?
                """,
                (catalog, pattern, pattern, pattern, limit),
            ).fetchall()
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
        if self.use_database:
            return self._get_tables_needing_maintenance_db(
                catalog, min_snapshots, min_delete_files, min_small_files, limit, offset
            )

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
        if self.use_database:
            with session_scope() as session:
                scanned_at = session.scalar(
                    select(CatalogSummary.scanned_at).where(CatalogSummary.catalog == catalog)
                )
            if not scanned_at:
                return None
            return int((datetime.utcnow() - scanned_at).total_seconds() / 60)

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
        if self.use_database:
            with session_scope() as session:
                return int(
                    session.scalar(
                        select(func.count()).select_from(TableHealthRecord).where(
                            TableHealthRecord.catalog == catalog
                        )
                    )
                    or 0
                )

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
        if self.use_database:
            with session_scope() as session:
                deleted = session.execute(
                    delete(TableHealthRecord).where(TableHealthRecord.catalog == catalog)
                ).rowcount or 0
                session.execute(delete(CatalogSummary).where(CatalogSummary.catalog == catalog))
                return deleted

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

    def _save_table_health_db(self, health: TableHealth, scan_mode: str) -> None:
        recommendations = [
            {
                "type": r.type.value,
                "priority": r.priority,
                "reason": r.reason,
                "estimated_impact": r.estimated_impact,
                "command_example": r.command_example,
            }
            for r in health.recommendations
        ]
        values = {
            "catalog": health.catalog,
            "namespace": health.namespace,
            "table_name": health.table_name,
            "status": health.status.value,
            "health_score": health.health_score,
            "total_snapshots": health.metrics.total_snapshots,
            "total_data_files": health.metrics.total_data_files,
            "total_delete_files": health.metrics.total_delete_files,
            "small_files_count": health.metrics.small_files_count,
            "total_size_gb": health.metrics.total_size_gb,
            "avg_file_size_mb": health.metrics.avg_file_size_mb,
            "oldest_snapshot_age_days": health.metrics.oldest_snapshot_age_days,
            "days_since_last_write": health.metrics.days_since_last_write,
            "issues_count": health.issues_count,
            "warnings_count": health.warnings_count,
            "recommendations_json": recommendations,
            "scan_mode": scan_mode,
            "scanned_at": datetime.utcnow(),
        }
        stmt = mysql_insert(TableHealthRecord).values(**values)
        stmt = stmt.on_duplicate_key_update(**values)
        with session_scope() as session:
            session.execute(stmt)

    def _save_catalog_summary_db(
        self, catalog: str, summary: TableHealthSummary, scan_mode: str
    ) -> None:
        values = {
            "catalog": catalog,
            "total_tables": summary.total_tables,
            "healthy_tables": summary.healthy_tables,
            "warning_tables": summary.warning_tables,
            "critical_tables": summary.critical_tables,
            "tables_needing_snapshot_expiration": summary.tables_needing_snapshot_expiration,
            "tables_needing_compaction": summary.tables_needing_compaction,
            "tables_needing_manifest_rewrite": summary.tables_needing_manifest_rewrite,
            "tables_with_delete_files": summary.tables_with_delete_files,
            "total_wasted_storage_gb": summary.total_wasted_storage_gb,
            "scan_mode": scan_mode,
            "scanned_at": datetime.utcnow(),
        }
        stmt = mysql_insert(CatalogSummary).values(**values)
        stmt = stmt.on_duplicate_key_update(**values)
        with session_scope() as session:
            session.execute(stmt)

    def _get_cached_summary_db(
        self, catalog: str, max_age_minutes: int
    ) -> Optional[TableHealthSummary]:
        with session_scope() as session:
            row = session.execute(
                select(CatalogSummary).where(
                    CatalogSummary.catalog == catalog,
                    CatalogSummary.scanned_at > datetime.utcnow() - timedelta(minutes=max_age_minutes),
                )
            ).scalar_one_or_none()
        if not row:
            return None
        cache_age = (datetime.utcnow() - row.scanned_at).total_seconds() / 60
        return TableHealthSummary(
            total_tables=row.total_tables,
            healthy_tables=row.healthy_tables,
            warning_tables=row.warning_tables,
            critical_tables=row.critical_tables,
            tables_needing_snapshot_expiration=row.tables_needing_snapshot_expiration,
            tables_needing_compaction=row.tables_needing_compaction,
            tables_needing_manifest_rewrite=row.tables_needing_manifest_rewrite,
            tables_with_delete_files=row.tables_with_delete_files,
            total_wasted_storage_gb=row.total_wasted_storage_gb,
            scan_mode=row.scan_mode,
            cached_at=row.scanned_at,
            cache_age_minutes=int(cache_age),
        )

    def _get_tables_by_status_db(
        self,
        catalog: str,
        status: Optional[HealthStatus],
        limit: int,
        offset: int,
    ) -> list[CachedTableHealth]:
        query = select(TableHealthRecord).where(TableHealthRecord.catalog == catalog)
        if status:
            query = query.where(TableHealthRecord.status == status.value)
        query = query.order_by(TableHealthRecord.health_score.asc()).limit(limit).offset(offset)
        with session_scope() as session:
            rows = session.execute(query).scalars().all()
        return [self._record_to_cached_health(row) for row in rows]

    def _get_table_db(
        self,
        catalog: str,
        namespace: str,
        table_name: str,
    ) -> Optional[CachedTableHealth]:
        with session_scope() as session:
            row = session.execute(
                select(TableHealthRecord).where(
                    TableHealthRecord.catalog == catalog,
                    TableHealthRecord.namespace == namespace,
                    TableHealthRecord.table_name == table_name,
                )
            ).scalar_one_or_none()
        return self._record_to_cached_health(row) if row else None

    def _get_tables_needing_maintenance_db(
        self,
        catalog: str,
        min_snapshots: Optional[int],
        min_delete_files: Optional[int],
        min_small_files: Optional[int],
        limit: int,
        offset: int,
    ) -> list[CachedTableHealth]:
        query = select(TableHealthRecord).where(TableHealthRecord.catalog == catalog)
        filters = []
        if min_snapshots is not None:
            filters.append(TableHealthRecord.total_snapshots >= min_snapshots)
        if min_delete_files is not None:
            filters.append(TableHealthRecord.total_delete_files >= min_delete_files)
        if min_small_files is not None:
            filters.append(TableHealthRecord.small_files_count >= min_small_files)
        if filters:
            query = query.where(or_(*filters))
        query = query.order_by(TableHealthRecord.health_score.asc()).limit(limit).offset(offset)
        with session_scope() as session:
            rows = session.execute(query).scalars().all()
        return [self._record_to_cached_health(row) for row in rows]

    def _search_tables_db(
        self,
        catalog: str,
        query: str,
        limit: int,
    ) -> list[CachedTableHealth]:
        pattern = f"%{query}%"
        stmt = (
            select(TableHealthRecord)
            .where(TableHealthRecord.catalog == catalog)
            .where(
                or_(
                    func.lower(TableHealthRecord.table_name).like(pattern),
                    func.lower(TableHealthRecord.namespace).like(pattern),
                    func.lower(
                        func.concat(TableHealthRecord.namespace, ".", TableHealthRecord.table_name)
                    ).like(pattern),
                )
            )
            .order_by(TableHealthRecord.namespace.asc(), TableHealthRecord.table_name.asc())
            .limit(limit)
        )
        with session_scope() as session:
            rows = session.execute(stmt).scalars().all()
        return [self._record_to_cached_health(row) for row in rows]

    def _record_to_cached_health(self, row: TableHealthRecord) -> CachedTableHealth:
        return CachedTableHealth(
            catalog=row.catalog,
            namespace=row.namespace,
            table_name=row.table_name,
            status=row.status,
            health_score=row.health_score,
            total_snapshots=row.total_snapshots,
            total_data_files=row.total_data_files,
            total_delete_files=row.total_delete_files,
            small_files_count=row.small_files_count,
            total_size_gb=row.total_size_gb,
            avg_file_size_mb=row.avg_file_size_mb,
            oldest_snapshot_age_days=row.oldest_snapshot_age_days,
            days_since_last_write=row.days_since_last_write,
            issues_count=row.issues_count,
            warnings_count=row.warnings_count,
            recommendations_json=json.dumps(row.recommendations_json),
            scan_mode=row.scan_mode,
            scanned_at=row.scanned_at,
        )


health_cache = HealthCache()
