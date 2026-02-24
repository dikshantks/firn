"""Snapshot service for Iceberg snapshot operations."""

from datetime import datetime
from typing import Any, Optional

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from app.models import SnapshotInfo, SnapshotGraph, SnapshotComparison
from app.utils.iceberg_helpers import format_timestamp


class SnapshotService:
    """Service for Iceberg snapshot operations."""
    
    def __init__(self, catalog: Catalog) -> None:
        """
        Initialize the snapshot service.
        
        Args:
            catalog: pyiceberg Catalog instance
        """
        self.catalog = catalog
    
    def _load_table(self, namespace: str, table_name: str) -> Table:
        """Load a table from the catalog."""
        return self.catalog.load_table((namespace, table_name))
    
    @staticmethod
    def _summary_to_dict(summary) -> dict[str, str]:
        """Safely convert a pyiceberg Summary to a plain dict."""
        if not summary:
            return {}
        result: dict[str, str] = {"operation": str(summary.operation.value)}
        if hasattr(summary, "additional_properties"):
            result.update(summary.additional_properties)
        return result

    def _snapshot_to_info(self, snapshot) -> SnapshotInfo:
        """Convert pyiceberg snapshot to SnapshotInfo model."""
        summary = self._summary_to_dict(snapshot.summary)
        return SnapshotInfo(
            snapshot_id=snapshot.snapshot_id,
            parent_snapshot_id=snapshot.parent_snapshot_id,
            timestamp_ms=snapshot.timestamp_ms,
            timestamp=format_timestamp(snapshot.timestamp_ms),
            operation=summary.get("operation", "unknown"),
            summary=summary,
            manifest_list_path=snapshot.manifest_list,
            schema_id=snapshot.schema_id,
            sequence_number=snapshot.sequence_number,
        )
    
    def get_snapshot_graph(self, namespace: str, table_name: str) -> SnapshotGraph:
        """
        Get snapshot lineage graph for visualization.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            
        Returns:
            SnapshotGraph with nodes and edges
        """
        table = self._load_table(namespace, table_name)
        metadata = table.metadata
        
        nodes = []
        edges = []
        
        for snapshot in metadata.snapshots:
            nodes.append(self._snapshot_to_info(snapshot))
            
            if snapshot.parent_snapshot_id is not None:
                edges.append((snapshot.parent_snapshot_id, snapshot.snapshot_id))
        
        return SnapshotGraph(
            nodes=nodes,
            edges=edges,
            current_snapshot_id=metadata.current_snapshot_id,
        )
    
    def get_snapshot(
        self,
        namespace: str,
        table_name: str,
        snapshot_id: int,
    ) -> Optional[SnapshotInfo]:
        """
        Get details of a specific snapshot.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            snapshot_id: Snapshot ID
            
        Returns:
            SnapshotInfo or None if not found
        """
        table = self._load_table(namespace, table_name)
        snapshot = table.snapshot_by_id(snapshot_id)
        
        if not snapshot:
            return None
        
        return self._snapshot_to_info(snapshot)
    
    def compare_snapshots(
        self,
        namespace: str,
        table_name: str,
        snapshot1_id: int,
        snapshot2_id: int,
    ) -> SnapshotComparison:
        """
        Compare two snapshots.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            snapshot1_id: First snapshot ID
            snapshot2_id: Second snapshot ID
            
        Returns:
            SnapshotComparison with diff details
        """
        table = self._load_table(namespace, table_name)
        
        snap1 = table.snapshot_by_id(snapshot1_id)
        snap2 = table.snapshot_by_id(snapshot2_id)
        
        if not snap1:
            raise ValueError(f"Snapshot {snapshot1_id} not found")
        if not snap2:
            raise ValueError(f"Snapshot {snapshot2_id} not found")
        
        # Get files for each snapshot
        files1 = set()
        files2 = set()
        
        try:
            scan1 = table.scan(snapshot_id=snapshot1_id)
            for task in scan1.plan_files():
                files1.add(task.file.file_path)
        except Exception:
            pass
        
        try:
            scan2 = table.scan(snapshot_id=snapshot2_id)
            for task in scan2.plan_files():
                files2.add(task.file.file_path)
        except Exception:
            pass
        
        added = files2 - files1
        removed = files1 - files2
        unchanged = files1 & files2
        
        # Calculate deltas from summaries
        records_delta = None
        size_delta = None
        
        summary1 = self._summary_to_dict(snap1.summary)
        summary2 = self._summary_to_dict(snap2.summary)
        
        if "total-records" in summary1 and "total-records" in summary2:
            try:
                records_delta = int(summary2["total-records"]) - int(summary1["total-records"])
            except (ValueError, TypeError):
                pass
        
        if "total-files-size" in summary1 and "total-files-size" in summary2:
            try:
                size_delta = int(summary2["total-files-size"]) - int(summary1["total-files-size"])
            except (ValueError, TypeError):
                pass
        
        return SnapshotComparison(
            snapshot1_id=snapshot1_id,
            snapshot2_id=snapshot2_id,
            snapshot1_summary=summary1,
            snapshot2_summary=summary2,
            files_added=len(added),
            files_removed=len(removed),
            files_unchanged=len(unchanged),
            added_file_paths=list(added)[:100],  # Limit to 100 paths
            removed_file_paths=list(removed)[:100],
            records_delta=records_delta,
            size_delta_bytes=size_delta,
        )
    
    def get_operation_history(
        self,
        namespace: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """
        Get operation history across all snapshots.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            
        Returns:
            List of operation history entries
        """
        table = self._load_table(namespace, table_name)
        metadata = table.metadata
        
        history = []
        
        for snapshot in metadata.snapshots:
            summary = self._summary_to_dict(snapshot.summary)
            
            entry = {
                "snapshot_id": snapshot.snapshot_id,
                "parent_snapshot_id": snapshot.parent_snapshot_id,
                "timestamp": format_timestamp(snapshot.timestamp_ms),
                "timestamp_ms": snapshot.timestamp_ms,
                "operation": summary.get("operation", "unknown"),
                "added_data_files": int(summary.get("added-data-files", 0)),
                "deleted_data_files": int(summary.get("deleted-data-files", 0)),
                "added_records": int(summary.get("added-records", 0)),
                "deleted_records": int(summary.get("deleted-records", 0)),
                "total_records": int(summary.get("total-records", 0)) if "total-records" in summary else None,
                "total_data_files": int(summary.get("total-data-files", 0)) if "total-data-files" in summary else None,
            }
            
            history.append(entry)
        
        # Sort by timestamp (newest first)
        history.sort(key=lambda x: x["timestamp_ms"], reverse=True)
        
        return history
