"""Snapshot-related Pydantic models."""

from typing import Any, Optional

from pydantic import BaseModel, Field


class SnapshotInfo(BaseModel):
    """Information about a single snapshot."""

    snapshot_id: int = Field(..., description="Unique snapshot ID")
    parent_snapshot_id: Optional[int] = Field(None, description="Parent snapshot ID")
    timestamp_ms: int = Field(..., description="Snapshot timestamp in milliseconds")
    timestamp: str = Field(..., description="Human-readable timestamp (ISO format)")
    operation: str = Field(..., description="Operation type (append, overwrite, delete, replace)")
    summary: dict[str, Any] = Field(default_factory=dict, description="Snapshot summary metrics")
    manifest_list_path: str = Field(..., description="Path to manifest list file")
    schema_id: Optional[int] = Field(None, description="Schema ID used by this snapshot")
    sequence_number: Optional[int] = Field(None, description="Sequence number")


class SnapshotGraph(BaseModel):
    """Snapshot lineage graph for visualization."""

    nodes: list[SnapshotInfo] = Field(default_factory=list, description="List of snapshots")
    edges: list[tuple[int, int]] = Field(
        default_factory=list,
        description="List of (parent_id, child_id) edges",
    )
    current_snapshot_id: Optional[int] = Field(None, description="Current snapshot ID")


class SnapshotComparison(BaseModel):
    """Comparison between two snapshots."""

    snapshot1_id: int
    snapshot2_id: int
    snapshot1_summary: dict[str, Any]
    snapshot2_summary: dict[str, Any]
    files_added: int = Field(..., description="Number of files added from snap1 to snap2")
    files_removed: int = Field(..., description="Number of files removed from snap1 to snap2")
    files_unchanged: int = Field(..., description="Number of unchanged files")
    added_file_paths: list[str] = Field(default_factory=list, description="Paths of added files")
    removed_file_paths: list[str] = Field(default_factory=list, description="Paths of removed files")
    records_delta: Optional[int] = Field(None, description="Change in record count")
    size_delta_bytes: Optional[int] = Field(None, description="Change in total size")
