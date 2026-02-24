"""Manifest service for reading Iceberg manifest files."""

import io
from typing import Any, Optional

import fastavro
from pyiceberg.catalog import Catalog
from pyiceberg.io import FileIO

from app.models import ManifestInfo, ManifestInfoWithEntries, ManifestEntry, ManifestListInfo, SnapshotDetails
from app.utils.iceberg_helpers import get_file_name_from_path


class ManifestService:
    """Service for reading Iceberg manifest lists and manifest files."""
    
    def __init__(self, catalog: Catalog) -> None:
        """
        Initialize the manifest service.
        
        Args:
            catalog: pyiceberg Catalog instance
        """
        self.catalog = catalog
    
    def _load_table(self, namespace: str, table_name: str):
        """Load a table from the catalog."""
        return self.catalog.load_table((namespace, table_name))
    
    def _read_avro_file(self, file_io: FileIO, path: str) -> list[dict[str, Any]]:
        """Read an Avro file and return records."""
        input_file = file_io.new_input(path)
        with input_file.open() as f:
            data = f.read()
        
        reader = fastavro.reader(io.BytesIO(data))
        return list(reader)
    
    def get_manifest_list(
        self,
        namespace: str,
        table_name: str,
        snapshot_id: int,
    ) -> ManifestListInfo:
        """
        Get manifest list for a snapshot.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            snapshot_id: Snapshot ID
            
        Returns:
            ManifestListInfo with manifest details
        """
        table = self._load_table(namespace, table_name)
        snapshot = table.snapshot_by_id(snapshot_id)
        
        if not snapshot:
            raise ValueError(f"Snapshot {snapshot_id} not found")
        
        manifest_list_path = snapshot.manifest_list
        file_io = table.io
        
        # Read manifest list file
        manifest_records = self._read_avro_file(file_io, manifest_list_path)
        
        manifests = []
        total_data_files = 0
        total_delete_files = 0
        total_records = 0
        total_size = 0
        
        for record in manifest_records:
            content = "data" if record.get("content", 0) == 0 else "deletes"
            
            added_files = record.get("added_files_count", 0) or 0
            existing_files = record.get("existing_files_count", 0) or 0
            deleted_files = record.get("deleted_files_count", 0) or 0
            added_rows = record.get("added_rows_count", 0) or 0
            existing_rows = record.get("existing_rows_count", 0) or 0
            deleted_rows = record.get("deleted_rows_count", 0) or 0
            
            raw_partitions = record.get("partitions", []) or []
            partitions = [p for p in raw_partitions if isinstance(p, dict)]

            manifest = ManifestInfo(
                manifest_path=record.get("manifest_path", ""),
                manifest_length=record.get("manifest_length", 0),
                partition_spec_id=record.get("partition_spec_id", 0),
                content=content,
                sequence_number=record.get("sequence_number", 0),
                min_sequence_number=record.get("min_sequence_number", 0),
                added_snapshot_id=record.get("added_snapshot_id", 0),
                added_files_count=added_files,
                existing_files_count=existing_files,
                deleted_files_count=deleted_files,
                added_rows_count=added_rows,
                existing_rows_count=existing_rows,
                deleted_rows_count=deleted_rows,
                partitions=partitions,
            )
            manifests.append(manifest)
            
            # Aggregate stats
            if content == "data":
                total_data_files += added_files + existing_files
            else:
                total_delete_files += added_files + existing_files
            
            total_records += added_rows + existing_rows
        
        return ManifestListInfo(
            snapshot_id=snapshot_id,
            manifest_list_path=manifest_list_path,
            manifests=manifests,
            total_data_files=total_data_files,
            total_delete_files=total_delete_files,
            total_records=total_records,
            total_size_bytes=total_size,
        )

    def get_snapshot_details(
        self,
        namespace: str,
        table_name: str,
        snapshot_id: int,
        entry_limit: int = 100,
    ) -> SnapshotDetails:
        """
        Get full details for a snapshot: manifest list, manifests, and file entries.
        """
        manifest_list = self.get_manifest_list(namespace, table_name, snapshot_id)
        manifests_with_entries: list[ManifestInfoWithEntries] = []

        for manifest in manifest_list.manifests:
            entries = self.get_manifest_entries(
                namespace, table_name, manifest.manifest_path, limit=entry_limit
            )
            manifests_with_entries.append(
                ManifestInfoWithEntries(
                    manifest_path=manifest.manifest_path,
                    manifest_length=manifest.manifest_length,
                    partition_spec_id=manifest.partition_spec_id,
                    content=manifest.content,
                    sequence_number=manifest.sequence_number,
                    min_sequence_number=manifest.min_sequence_number,
                    added_snapshot_id=manifest.added_snapshot_id,
                    added_files_count=manifest.added_files_count,
                    existing_files_count=manifest.existing_files_count,
                    deleted_files_count=manifest.deleted_files_count,
                    added_rows_count=manifest.added_rows_count,
                    existing_rows_count=manifest.existing_rows_count,
                    deleted_rows_count=manifest.deleted_rows_count,
                    partitions=manifest.partitions,
                    entries=entries,
                )
            )

        return SnapshotDetails(
            snapshot_id=manifest_list.snapshot_id,
            manifest_list_path=manifest_list.manifest_list_path,
            manifests=manifests_with_entries,
            total_data_files=manifest_list.total_data_files,
            total_delete_files=manifest_list.total_delete_files,
            total_records=manifest_list.total_records,
            total_size_bytes=manifest_list.total_size_bytes,
        )

    def get_all_snapshots_details(
        self,
        namespace: str,
        table_name: str,
        entry_limit: int = 100,
    ) -> list[SnapshotDetails]:
        """Get details for all snapshots in a table."""
        table = self._load_table(namespace, table_name)
        metadata = table.metadata
        details_list: list[SnapshotDetails] = []

        for snapshot in metadata.snapshots:
            details = self.get_snapshot_details(
                namespace, table_name, snapshot.snapshot_id, entry_limit=entry_limit
            )
            details_list.append(details)

        return details_list

    def get_manifest_entries(
        self,
        namespace: str,
        table_name: str,
        manifest_path: str,
        limit: int = 100,
    ) -> list[ManifestEntry]:
        """
        Get entries (data files) from a manifest file.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            manifest_path: Path to manifest file
            limit: Maximum entries to return
            
        Returns:
            List of ManifestEntry objects
        """
        table = self._load_table(namespace, table_name)
        file_io = table.io
        
        # Read manifest file
        records = self._read_avro_file(file_io, manifest_path)
        
        entries = []
        for record in records[:limit]:
            # Extract data file info from the record
            data_file = record.get("data_file", record)
            
            # Handle both nested and flat structures
            if isinstance(data_file, dict):
                file_path = data_file.get("file_path", "")
                file_format = data_file.get("file_format", "PARQUET")
                partition = data_file.get("partition", {}) or {}
                if not isinstance(partition, dict):
                    partition = {}
                record_count = data_file.get("record_count", 0)
                file_size = data_file.get("file_size_in_bytes", 0)
                column_sizes = data_file.get("column_sizes", None)
                value_counts = data_file.get("value_counts", None)
                null_value_counts = data_file.get("null_value_counts", None)
                nan_value_counts = data_file.get("nan_value_counts", None)
                lower_bounds = data_file.get("lower_bounds", None)
                upper_bounds = data_file.get("upper_bounds", None)
                split_offsets = data_file.get("split_offsets", None)
                sort_order_id = data_file.get("sort_order_id", None)
            else:
                # Fallback for unexpected structure
                file_path = str(data_file)
                file_format = "PARQUET"
                partition = {}
                record_count = 0
                file_size = 0
                column_sizes = None
                value_counts = None
                null_value_counts = None
                nan_value_counts = None
                lower_bounds = None
                upper_bounds = None
                split_offsets = None
                sort_order_id = None
            
            # Convert file format code to string
            if isinstance(file_format, int):
                format_map = {0: "AVRO", 1: "ORC", 2: "PARQUET"}
                file_format = format_map.get(file_format, "PARQUET")
            
            # Convert column stats dicts (keys might be integers)
            def convert_stats_dict(d):
                if d is None or not isinstance(d, dict):
                    return None
                return {str(k): v for k, v in d.items()}
            
            entry = ManifestEntry(
                status=record.get("status", 0),
                snapshot_id=record.get("snapshot_id"),
                sequence_number=record.get("sequence_number"),
                file_sequence_number=record.get("file_sequence_number"),
                file_path=file_path,
                file_format=file_format,
                partition=partition,
                record_count=record_count or 0,
                file_size_in_bytes=file_size or 0,
                column_sizes=convert_stats_dict(column_sizes),
                value_counts=convert_stats_dict(value_counts),
                null_value_counts=convert_stats_dict(null_value_counts),
                nan_value_counts=convert_stats_dict(nan_value_counts),
                lower_bounds=convert_stats_dict(lower_bounds),
                upper_bounds=convert_stats_dict(upper_bounds),
                split_offsets=split_offsets,
                sort_order_id=sort_order_id,
            )
            entries.append(entry)
        
        return entries
