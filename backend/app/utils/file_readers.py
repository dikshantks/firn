"""File reading utilities for Avro and Parquet files."""

import io
from typing import Any, Optional

import fastavro
import pyarrow.parquet as pq


def read_avro_file(data: bytes) -> list[dict[str, Any]]:
    """
    Read an Avro file from bytes and return records.
    
    Args:
        data: Raw bytes of Avro file
        
    Returns:
        List of records as dictionaries
    """
    reader = fastavro.reader(io.BytesIO(data))
    return list(reader)


def read_avro_schema(data: bytes) -> dict[str, Any]:
    """
    Read the schema from an Avro file.
    
    Args:
        data: Raw bytes of Avro file
        
    Returns:
        Avro schema as dictionary
    """
    reader = fastavro.reader(io.BytesIO(data))
    return reader.writer_schema


def read_parquet_metadata(data: bytes) -> dict[str, Any]:
    """
    Read Parquet file metadata from bytes.
    
    Args:
        data: Raw bytes of Parquet file
        
    Returns:
        Dictionary with Parquet metadata
    """
    buffer = io.BytesIO(data)
    parquet_file = pq.ParquetFile(buffer)
    metadata = parquet_file.metadata
    schema = parquet_file.schema_arrow
    
    row_groups = []
    for i in range(metadata.num_row_groups):
        rg = metadata.row_group(i)
        columns = []
        for j in range(rg.num_columns):
            col = rg.column(j)
            columns.append({
                "path_in_schema": col.path_in_schema,
                "file_offset": col.file_offset,
                "file_path": col.file_path,
                "physical_type": str(col.physical_type),
                "num_values": col.num_values,
                "total_compressed_size": col.total_compressed_size,
                "total_uncompressed_size": col.total_uncompressed_size,
                "compression": str(col.compression),
                "encodings": [str(e) for e in (col.encodings or [])],
                "is_stats_set": col.is_stats_set,
            })
            
        row_groups.append({
            "row_group_index": i,
            "num_rows": rg.num_rows,
            "total_byte_size": rg.total_byte_size,
            "columns": columns,
        })
    
    return {
        "num_rows": metadata.num_rows,
        "num_row_groups": metadata.num_row_groups,
        "num_columns": metadata.num_columns,
        "created_by": metadata.created_by,
        "format_version": metadata.format_version,
        "serialized_size": metadata.serialized_size,
        "schema": [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable,
            }
            for field in schema
        ],
        "row_groups": row_groups,
    }


def sample_parquet_rows(data: bytes, num_rows: int = 10) -> dict[str, Any]:
    """
    Sample rows from a Parquet file.
    
    Args:
        data: Raw bytes of Parquet file
        num_rows: Number of rows to sample
        
    Returns:
        Dictionary with columns and sampled rows
    """
    buffer = io.BytesIO(data)
    table = pq.read_table(buffer)
    
    # Get column names
    columns = table.column_names
    
    # Sample rows
    sample_table = table.slice(0, min(num_rows, table.num_rows))
    rows = sample_table.to_pydict()
    
    # Convert to list of rows
    row_list = []
    for i in range(sample_table.num_rows):
        row = [rows[col][i] for col in columns]
        row_list.append(row)
    
    return {
        "columns": columns,
        "rows": row_list,
        "total_rows": table.num_rows,
        "sampled_rows": len(row_list),
    }
