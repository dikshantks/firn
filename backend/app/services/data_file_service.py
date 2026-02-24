"""Data file service for Iceberg data file operations."""

import io
from typing import Any, Optional

import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog

from app.models import (
    DataFileInfo,
    ColumnStats,
    DataFileInspection,
    DataFileSample,
    RowGroupInfo,
)
from app.utils.iceberg_helpers import get_file_name_from_path


class DataFileService:
    """Service for Iceberg data file operations."""
    
    def __init__(self, catalog: Catalog) -> None:
        """
        Initialize the data file service.
        
        Args:
            catalog: pyiceberg Catalog instance
        """
        self.catalog = catalog
    
    def _load_table(self, namespace: str, table_name: str):
        """Load a table from the catalog."""
        return self.catalog.load_table((namespace, table_name))

    @staticmethod
    def _partition_to_dict(partition, partition_spec) -> dict[str, str]:
        """Convert a pyiceberg partition Record to a plain dict using the spec field names."""
        if not partition:
            return {}
        result: dict[str, str] = {}
        try:
            values = list(partition)
            for i, field in enumerate(partition_spec.fields):
                if i < len(values):
                    result[field.name] = str(values[i])
        except Exception:
            pass
        return result
    
    def get_data_files(
        self,
        namespace: str,
        table_name: str,
        snapshot_id: int,
        limit: int = 100,
        min_size_bytes: Optional[int] = None,
        max_size_bytes: Optional[int] = None,
        file_format: Optional[str] = None,
    ) -> list[DataFileInfo]:
        """
        List data files for a snapshot with optional filtering.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            snapshot_id: Snapshot ID
            limit: Maximum files to return
            min_size_bytes: Minimum file size filter
            max_size_bytes: Maximum file size filter
            file_format: File format filter (PARQUET, ORC, AVRO)
            
        Returns:
            List of DataFileInfo objects
        """
        table = self._load_table(namespace, table_name)
        snapshot = table.snapshot_by_id(snapshot_id)
        
        if not snapshot:
            raise ValueError(f"Snapshot {snapshot_id} not found")
        
        # Get schema for column name mapping
        schema = table.schema()
        column_names = {field.field_id: field.name for field in schema.fields}
        column_types = {field.field_id: str(field.field_type) for field in schema.fields}
        
        # Scan files for this snapshot
        scan = table.scan(snapshot_id=snapshot_id)
        
        files = []
        count = 0
        
        for task in scan.plan_files():
            if count >= limit:
                break
            
            data_file = task.file
            
            # Apply filters
            if min_size_bytes and data_file.file_size_in_bytes < min_size_bytes:
                continue
            if max_size_bytes and data_file.file_size_in_bytes > max_size_bytes:
                continue
            if file_format and data_file.file_format.name.upper() != file_format.upper():
                continue
            
            # Build column stats
            column_stats = []
            
            # Get column sizes
            col_sizes = dict(data_file.column_sizes) if data_file.column_sizes else {}
            value_counts = dict(data_file.value_counts) if data_file.value_counts else {}
            null_counts = dict(data_file.null_value_counts) if data_file.null_value_counts else {}
            nan_counts = dict(data_file.nan_value_counts) if data_file.nan_value_counts else {}
            lower_bounds = dict(data_file.lower_bounds) if data_file.lower_bounds else {}
            upper_bounds = dict(data_file.upper_bounds) if data_file.upper_bounds else {}
            
            # Build stats for each column
            all_col_ids = set(col_sizes.keys()) | set(value_counts.keys()) | set(null_counts.keys())
            
            for col_id in all_col_ids:
                # Decode bounds if they are bytes
                lower = lower_bounds.get(col_id)
                upper = upper_bounds.get(col_id)
                
                if isinstance(lower, bytes):
                    try:
                        lower = lower.decode('utf-8')
                    except Exception:
                        lower = str(lower)
                
                if isinstance(upper, bytes):
                    try:
                        upper = upper.decode('utf-8')
                    except Exception:
                        upper = str(upper)
                
                column_stats.append(ColumnStats(
                    column_id=col_id,
                    column_name=column_names.get(col_id),
                    column_type=column_types.get(col_id),
                    size_bytes=col_sizes.get(col_id),
                    value_count=value_counts.get(col_id),
                    null_count=null_counts.get(col_id),
                    nan_count=nan_counts.get(col_id),
                    lower_bound=lower,
                    upper_bound=upper,
                ))
            
            # Get partition values
            partition_spec = table.metadata.spec()
            partition = self._partition_to_dict(data_file.partition, partition_spec)
            
            files.append(DataFileInfo(
                file_path=data_file.file_path,
                file_name=get_file_name_from_path(data_file.file_path),
                file_format=data_file.file_format.name,
                partition=partition,
                record_count=data_file.record_count,
                file_size_bytes=data_file.file_size_in_bytes,
                column_stats=column_stats,
                split_offsets=list(data_file.split_offsets) if data_file.split_offsets else None,
                sort_order_id=data_file.sort_order_id,
            ))
            count += 1
        
        return files
    
    def inspect_file(
        self,
        namespace: str,
        table_name: str,
        file_path: str,
    ) -> DataFileInspection:
        """
        Deep inspection of a data file (Parquet footer details).
        
        Args:
            namespace: Table namespace
            table_name: Table name
            file_path: Path to data file
            
        Returns:
            DataFileInspection with detailed metadata
        """
        table = self._load_table(namespace, table_name)
        file_io = table.io
        
        # Read the file
        input_file = file_io.new_input(file_path)
        with input_file.open() as f:
            data = f.read()
        
        # Parse Parquet metadata
        buffer = io.BytesIO(data)
        parquet_file = pq.ParquetFile(buffer)
        metadata = parquet_file.metadata
        schema = parquet_file.schema_arrow
        
        # Build row group info
        row_groups = []
        encodings_set = set()
        compression = None
        
        for i in range(metadata.num_row_groups):
            rg = metadata.row_group(i)
            columns = []
            
            for j in range(rg.num_columns):
                col = rg.column(j)
                
                # Collect encodings
                if col.encodings:
                    for enc in col.encodings:
                        encodings_set.add(str(enc))
                
                # Get compression
                if compression is None:
                    compression = str(col.compression)
                
                columns.append({
                    "path_in_schema": col.path_in_schema,
                    "physical_type": str(col.physical_type),
                    "num_values": col.num_values,
                    "total_compressed_size": col.total_compressed_size,
                    "total_uncompressed_size": col.total_uncompressed_size,
                    "compression": str(col.compression),
                    "encodings": [str(e) for e in (col.encodings or [])],
                    "is_stats_set": col.is_stats_set,
                })
            
            row_groups.append(RowGroupInfo(
                row_group_index=i,
                num_rows=rg.num_rows,
                total_byte_size=rg.total_byte_size,
                columns=columns,
            ))
        
        # Build schema fields
        schema_fields = [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable,
            }
            for field in schema
        ]
        
        return DataFileInspection(
            file_path=file_path,
            file_format="PARQUET",
            file_size_bytes=len(data),
            num_row_groups=metadata.num_row_groups,
            num_rows=metadata.num_rows,
            created_by=metadata.created_by,
            schema_fields=schema_fields,
            row_groups=row_groups,
            compression=compression,
            encodings=list(encodings_set),
        )
    
    def sample_file(
        self,
        namespace: str,
        table_name: str,
        file_path: str,
        num_rows: int = 10,
    ) -> DataFileSample:
        """
        Sample rows from a data file.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            file_path: Path to data file
            num_rows: Number of rows to sample
            
        Returns:
            DataFileSample with column names and sample rows
        """
        table = self._load_table(namespace, table_name)
        file_io = table.io
        
        # Read the file
        input_file = file_io.new_input(file_path)
        with input_file.open() as f:
            data = f.read()
        
        # Read Parquet data
        buffer = io.BytesIO(data)
        arrow_table = pq.read_table(buffer)
        
        # Get column names
        columns = arrow_table.column_names
        
        # Sample rows
        sample_table = arrow_table.slice(0, min(num_rows, arrow_table.num_rows))
        rows_dict = sample_table.to_pydict()
        
        # Convert to list of rows
        row_list = []
        for i in range(sample_table.num_rows):
            row = []
            for col in columns:
                value = rows_dict[col][i]
                # Convert to JSON-serializable format
                if hasattr(value, 'isoformat'):
                    value = value.isoformat()
                elif hasattr(value, 'as_py'):
                    value = value.as_py()
                row.append(value)
            row_list.append(row)
        
        return DataFileSample(
            file_path=file_path,
            columns=columns,
            rows=row_list,
            total_rows=arrow_table.num_rows,
            sampled_rows=len(row_list),
        )
    
    def get_storage_analytics(
        self,
        namespace: str,
        table_name: str,
    ) -> dict[str, Any]:
        """
        Get storage analytics for a table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            
        Returns:
            Dictionary with storage analytics
        """
        table = self._load_table(namespace, table_name)
        current_snapshot = table.current_snapshot()
        
        if not current_snapshot:
            return {
                "total_size_bytes": 0,
                "total_files": 0,
                "total_records": 0,
                "format_breakdown": {},
                "size_distribution": {},
                "partition_breakdown": {},
            }
        
        # Scan current snapshot
        scan = table.scan()
        
        total_size = 0
        total_files = 0
        total_records = 0
        format_counts = {}
        size_buckets = {
            "< 1MB": 0,
            "1-10MB": 0,
            "10-100MB": 0,
            "100MB-1GB": 0,
            "> 1GB": 0,
        }
        partition_sizes = {}
        
        for task in scan.plan_files():
            data_file = task.file
            
            total_size += data_file.file_size_in_bytes
            total_files += 1
            total_records += data_file.record_count
            
            # Format breakdown
            fmt = data_file.file_format.name
            format_counts[fmt] = format_counts.get(fmt, 0) + 1
            
            # Size distribution
            size_mb = data_file.file_size_in_bytes / (1024 * 1024)
            if size_mb < 1:
                size_buckets["< 1MB"] += 1
            elif size_mb < 10:
                size_buckets["1-10MB"] += 1
            elif size_mb < 100:
                size_buckets["10-100MB"] += 1
            elif size_mb < 1024:
                size_buckets["100MB-1GB"] += 1
            else:
                size_buckets["> 1GB"] += 1
            
            # Partition breakdown
            if data_file.partition:
                partition_spec = table.metadata.spec()
                partition_key = str(self._partition_to_dict(data_file.partition, partition_spec))
                if partition_key not in partition_sizes:
                    partition_sizes[partition_key] = {"files": 0, "size_bytes": 0, "records": 0}
                partition_sizes[partition_key]["files"] += 1
                partition_sizes[partition_key]["size_bytes"] += data_file.file_size_in_bytes
                partition_sizes[partition_key]["records"] += data_file.record_count
        
        return {
            "total_size_bytes": total_size,
            "total_size_human": self._format_bytes(total_size),
            "total_files": total_files,
            "total_records": total_records,
            "avg_file_size_bytes": total_size // total_files if total_files > 0 else 0,
            "avg_file_size_human": self._format_bytes(total_size // total_files) if total_files > 0 else "0 B",
            "format_breakdown": format_counts,
            "size_distribution": size_buckets,
            "partition_count": len(partition_sizes),
            "partition_breakdown": partition_sizes,
        }
    
    def _format_bytes(self, size: int) -> str:
        """Format bytes to human-readable string."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} PB"
