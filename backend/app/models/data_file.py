"""Data file-related Pydantic models."""

from typing import Any, Optional

from pydantic import BaseModel, Field


class ColumnStats(BaseModel):
    """Statistics for a single column in a data file."""

    column_id: int = Field(..., description="Column ID")
    column_name: Optional[str] = Field(None, description="Column name")
    column_type: Optional[str] = Field(None, description="Column data type")
    size_bytes: Optional[int] = Field(None, description="Column size in bytes")
    value_count: Optional[int] = Field(None, description="Number of values")
    null_count: Optional[int] = Field(None, description="Number of null values")
    nan_count: Optional[int] = Field(None, description="Number of NaN values")
    lower_bound: Optional[Any] = Field(None, description="Lower bound value")
    upper_bound: Optional[Any] = Field(None, description="Upper bound value")


class DataFileInfo(BaseModel):
    """Information about a data file."""

    file_path: str = Field(..., description="Full path to data file")
    file_name: str = Field(..., description="File name only")
    file_format: str = Field(..., description="File format (PARQUET, ORC, AVRO)")
    partition: dict[str, Any] = Field(default_factory=dict, description="Partition values")
    record_count: int = Field(..., description="Number of records")
    file_size_bytes: int = Field(..., description="File size in bytes")
    column_stats: list[ColumnStats] = Field(default_factory=list, description="Per-column statistics")
    split_offsets: Optional[list[int]] = Field(None, description="Split offsets for parallel reads")
    sort_order_id: Optional[int] = Field(None, description="Sort order ID")


class RowGroupInfo(BaseModel):
    """Information about a Parquet row group."""

    row_group_index: int = Field(..., description="Row group index")
    num_rows: int = Field(..., description="Number of rows in row group")
    total_byte_size: int = Field(..., description="Total size in bytes")
    columns: list[dict[str, Any]] = Field(default_factory=list, description="Column chunk info")


class DataFileInspection(BaseModel):
    """Deep inspection of a data file (Parquet footer details)."""

    file_path: str
    file_format: str
    file_size_bytes: int
    
    # Parquet-specific
    num_row_groups: Optional[int] = Field(None, description="Number of row groups")
    num_rows: Optional[int] = Field(None, description="Total number of rows")
    created_by: Optional[str] = Field(None, description="Creator application")
    
    # Schema
    schema_fields: list[dict[str, Any]] = Field(default_factory=list, description="Schema fields")
    
    # Row groups
    row_groups: list[RowGroupInfo] = Field(default_factory=list, description="Row group details")
    
    # Compression and encoding
    compression: Optional[str] = Field(None, description="Compression codec")
    encodings: list[str] = Field(default_factory=list, description="Encodings used")


class DataFileSample(BaseModel):
    """Sample rows from a data file."""

    file_path: str
    columns: list[str] = Field(..., description="Column names")
    rows: list[list[Any]] = Field(..., description="Sample rows as list of values")
    total_rows: int = Field(..., description="Total rows in file")
    sampled_rows: int = Field(..., description="Number of rows sampled")
