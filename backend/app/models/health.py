"""Models for table health and maintenance recommendations."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class MaintenanceType(str, Enum):
    """Types of maintenance operations."""
    EXPIRE_SNAPSHOTS = "expire_snapshots"
    COMPACT_DATA_FILES = "compact_data_files"
    REWRITE_MANIFESTS = "rewrite_manifests"
    DELETE_ORPHAN_FILES = "delete_orphan_files"
    REWRITE_DELETE_FILES = "rewrite_delete_files"


class HealthStatus(str, Enum):
    """Overall health status of a table."""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"


class MaintenanceRecommendation(BaseModel):
    """A specific maintenance recommendation."""
    type: MaintenanceType
    priority: str = Field(..., description="high, medium, low")
    reason: str
    estimated_impact: str
    command_example: Optional[str] = None


class TableHealthMetrics(BaseModel):
    """Detailed health metrics for a table."""
    # Snapshot metrics
    total_snapshots: int
    oldest_snapshot_age_days: Optional[float] = None
    snapshots_last_7_days: int
    snapshots_last_30_days: int
    
    # File metrics
    total_data_files: int
    total_delete_files: int
    small_files_count: int = Field(0, description="Files < 128MB")
    avg_file_size_mb: float
    total_size_gb: float
    
    # Manifest metrics
    total_manifests: int
    small_manifests_count: int = Field(0, description="Manifests with < 10 files")
    
    # Partition metrics
    partition_count: Optional[int] = None
    
    # Timing metrics
    last_compaction: Optional[datetime] = None
    last_snapshot_expiration: Optional[datetime] = None
    days_since_last_write: Optional[float] = None


class TableHealth(BaseModel):
    """Health assessment for a table."""
    catalog: str
    namespace: str
    table_name: str
    status: HealthStatus
    health_score: int = Field(..., ge=0, le=100, description="0-100 health score")
    
    metrics: TableHealthMetrics
    recommendations: list[MaintenanceRecommendation]
    
    # Summary
    issues_count: int
    warnings_count: int
    last_checked: datetime = Field(default_factory=datetime.utcnow)


class MaintenanceFilter(BaseModel):
    """Filters for maintenance queries."""
    min_snapshots: Optional[int] = Field(None, ge=0)
    max_snapshots: Optional[int] = Field(None, ge=0)
    min_age_days: Optional[int] = Field(None, ge=0)
    min_small_files: Optional[int] = Field(None, ge=0)
    min_delete_files: Optional[int] = Field(None, ge=0)
    status: Optional[list[HealthStatus]] = None
    needs_maintenance: Optional[list[MaintenanceType]] = None


class TableHealthSummary(BaseModel):
    """Summary of table health across catalog."""
    total_tables: int
    healthy_tables: int
    warning_tables: int
    critical_tables: int
    tables_needing_snapshot_expiration: int
    tables_needing_compaction: int
    tables_needing_manifest_rewrite: int
    tables_with_delete_files: int
    total_wasted_storage_gb: float
    last_scan: datetime = Field(default_factory=datetime.utcnow)