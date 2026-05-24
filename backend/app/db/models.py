"""Database models for Fern persistence."""

from datetime import datetime

from sqlalchemy import JSON, BigInteger, DateTime, Index, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class CatalogRegistry(Base):
    """Registered Iceberg catalogs."""

    __tablename__ = "catalog_registry"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    type: Mapped[str] = mapped_column(String(32), nullable=False)
    config_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )


class CatalogSummary(Base):
    """Cached catalog health summary."""

    __tablename__ = "catalog_summary"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    catalog: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    total_tables: Mapped[int] = mapped_column(Integer, nullable=False)
    healthy_tables: Mapped[int] = mapped_column(Integer, nullable=False)
    warning_tables: Mapped[int] = mapped_column(Integer, nullable=False)
    critical_tables: Mapped[int] = mapped_column(Integer, nullable=False)
    tables_needing_snapshot_expiration: Mapped[int] = mapped_column(Integer, nullable=False)
    tables_needing_compaction: Mapped[int] = mapped_column(Integer, nullable=False)
    tables_needing_manifest_rewrite: Mapped[int] = mapped_column(Integer, nullable=False)
    tables_with_delete_files: Mapped[int] = mapped_column(Integer, nullable=False)
    total_wasted_storage_gb: Mapped[float] = mapped_column(nullable=False)
    scan_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    scanned_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class TableHealthRecord(Base):
    """Cached table-level health assessment."""

    __tablename__ = "table_health"
    __table_args__ = (
        UniqueConstraint("catalog", "namespace", "table_name", name="uq_table_health_name"),
        Index("idx_table_health_catalog_status", "catalog", "status"),
        Index("idx_table_health_catalog_scanned", "catalog", "scanned_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    catalog: Mapped[str] = mapped_column(String(128), nullable=False)
    namespace: Mapped[str] = mapped_column(String(255), nullable=False)
    table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    health_score: Mapped[int] = mapped_column(Integer, nullable=False)
    total_snapshots: Mapped[int] = mapped_column(Integer, nullable=False)
    total_data_files: Mapped[int] = mapped_column(Integer, nullable=False)
    total_delete_files: Mapped[int] = mapped_column(Integer, nullable=False)
    small_files_count: Mapped[int] = mapped_column(Integer, nullable=False)
    total_size_gb: Mapped[float] = mapped_column(nullable=False)
    avg_file_size_mb: Mapped[float] = mapped_column(nullable=False)
    oldest_snapshot_age_days: Mapped[float | None] = mapped_column(nullable=True)
    days_since_last_write: Mapped[float | None] = mapped_column(nullable=True)
    issues_count: Mapped[int] = mapped_column(Integer, nullable=False)
    warnings_count: Mapped[int] = mapped_column(Integer, nullable=False)
    recommendations_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    scan_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    scanned_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class JobRecord(Base):
    """Background job state for replica-safe coordination."""

    __tablename__ = "jobs"
    __table_args__ = (
        Index("idx_jobs_status_heartbeat", "status", "heartbeat_at"),
        Index("idx_jobs_catalog_created", "catalog", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    type: Mapped[str] = mapped_column(String(64), nullable=False, default="generic")
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    progress: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    message: Mapped[str | None] = mapped_column(String(255), nullable=True)
    payload_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    catalog: Mapped[str | None] = mapped_column(String(128), nullable=True)
    owner_replica: Mapped[str | None] = mapped_column(String(64), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    result_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )
