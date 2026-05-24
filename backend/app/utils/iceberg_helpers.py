"""Helper functions for Iceberg operations."""

import base64
from datetime import datetime
from typing import Any, Optional
from urllib.parse import urlparse


def json_safe_value(value: Any) -> Any:
    """Recursively convert Avro/Iceberg values to JSON-serializable form."""
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return base64.b64encode(value).decode("ascii")
    if isinstance(value, dict):
        return {str(k): json_safe_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe_value(v) for v in value]
    if isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def format_timestamp(timestamp_ms: int) -> str:
    """Convert millisecond timestamp to ISO format string."""
    return datetime.fromtimestamp(timestamp_ms / 1000).isoformat()


def parse_iceberg_path(path: str) -> dict[str, Optional[str]]:
    """
    Parse an Iceberg file path into components.
    
    Handles both s3:// and file:// paths.
    
    Returns:
        dict with 'scheme', 'bucket', 'key', 'filename'
    """
    parsed = urlparse(path)
    
    if parsed.scheme in ("s3", "s3a", "s3n"):
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        filename = key.split("/")[-1] if key else None
        return {
            "scheme": parsed.scheme,
            "bucket": bucket,
            "key": key,
            "filename": filename,
        }
    elif parsed.scheme == "file" or not parsed.scheme:
        # Local file path
        path_str = parsed.path or path
        filename = path_str.split("/")[-1] if path_str else None
        return {
            "scheme": "file",
            "bucket": None,
            "key": path_str,
            "filename": filename,
        }
    else:
        # Other schemes (gs://, abfs://, etc.)
        return {
            "scheme": parsed.scheme,
            "bucket": parsed.netloc,
            "key": parsed.path.lstrip("/"),
            "filename": parsed.path.split("/")[-1] if parsed.path else None,
        }


def get_file_name_from_path(path: str) -> str:
    """Extract filename from a path."""
    return path.rstrip("/").split("/")[-1]


def iceberg_type_to_string(iceberg_type) -> str:
    """Convert pyiceberg type to string representation."""
    return str(iceberg_type)


def decode_partition_value(value, field_type: str) -> str:
    """Decode a partition value to string for display."""
    if value is None:
        return "null"
    return str(value)
