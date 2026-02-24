"""Storage service for reading files from S3/MinIO."""

from typing import Optional
from urllib.parse import urlparse

import boto3
from botocore.config import Config

from app.config import settings


class StorageService:
    """
    Service for reading files from S3-compatible object storage.
    
    Supports both AWS S3 and MinIO (via custom endpoint).
    """
    
    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = "us-east-1",
    ) -> None:
        """
        Initialize the storage service.
        
        Args:
            endpoint_url: S3 endpoint URL (for MinIO, e.g., http://localhost:9000)
            access_key: AWS access key or MinIO access key
            secret_key: AWS secret key or MinIO secret key
            region: AWS region (default: us-east-1)
        """
        self.endpoint_url = endpoint_url or settings.default_s3_endpoint
        self.access_key = access_key or settings.default_s3_access_key
        self.secret_key = secret_key or settings.default_s3_secret_key
        self.region = region or settings.default_s3_region
        
        # Create S3 client
        self._client = self._create_client()
    
    def _create_client(self):
        """Create boto3 S3 client with configured settings."""
        config = Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},  # Required for MinIO
        )
        
        client_kwargs = {
            "service_name": "s3",
            "config": config,
            "region_name": self.region,
        }
        
        if self.endpoint_url:
            client_kwargs["endpoint_url"] = self.endpoint_url
        
        if self.access_key and self.secret_key:
            client_kwargs["aws_access_key_id"] = self.access_key
            client_kwargs["aws_secret_access_key"] = self.secret_key
        
        return boto3.client(**client_kwargs)
    
    def _parse_s3_path(self, path: str) -> tuple[str, str]:
        """
        Parse an S3 path into bucket and key.
        
        Args:
            path: S3 path (s3://bucket/key or s3a://bucket/key)
            
        Returns:
            Tuple of (bucket, key)
        """
        parsed = urlparse(path)
        
        if parsed.scheme in ("s3", "s3a", "s3n"):
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            return bucket, key
        else:
            # Assume it's already bucket/key format
            parts = path.split("/", 1)
            if len(parts) == 2:
                return parts[0], parts[1]
            raise ValueError(f"Invalid S3 path: {path}")
    
    def read_file(self, path: str) -> bytes:
        """
        Read entire file from S3.
        
        Args:
            path: S3 path to file
            
        Returns:
            File contents as bytes
        """
        bucket, key = self._parse_s3_path(path)
        response = self._client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    
    def read_range(self, path: str, offset: int, length: int) -> bytes:
        """
        Read a byte range from a file in S3.
        
        Args:
            path: S3 path to file
            offset: Starting byte offset
            length: Number of bytes to read
            
        Returns:
            File contents as bytes
        """
        bucket, key = self._parse_s3_path(path)
        range_header = f"bytes={offset}-{offset + length - 1}"
        response = self._client.get_object(
            Bucket=bucket,
            Key=key,
            Range=range_header,
        )
        return response["Body"].read()
    
    def list_prefix(self, path: str, max_keys: int = 1000) -> list[str]:
        """
        List objects under a prefix.
        
        Args:
            path: S3 prefix path
            max_keys: Maximum number of keys to return
            
        Returns:
            List of full S3 paths
        """
        bucket, prefix = self._parse_s3_path(path)
        
        response = self._client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=max_keys,
        )
        
        paths = []
        for obj in response.get("Contents", []):
            paths.append(f"s3://{bucket}/{obj['Key']}")
        
        return paths
    
    def get_file_size(self, path: str) -> int:
        """
        Get the size of a file in S3.
        
        Args:
            path: S3 path to file
            
        Returns:
            File size in bytes
        """
        bucket, key = self._parse_s3_path(path)
        response = self._client.head_object(Bucket=bucket, Key=key)
        return response["ContentLength"]
    
    def file_exists(self, path: str) -> bool:
        """
        Check if a file exists in S3.
        
        Args:
            path: S3 path to file
            
        Returns:
            True if file exists
        """
        try:
            bucket, key = self._parse_s3_path(path)
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False


def create_storage_service_from_catalog_properties(
    properties: dict[str, str],
) -> StorageService:
    """
    Create a StorageService from catalog properties.
    
    Args:
        properties: Catalog properties dict
        
    Returns:
        Configured StorageService
    """
    return StorageService(
        endpoint_url=properties.get("s3.endpoint"),
        access_key=properties.get("s3.access-key-id"),
        secret_key=properties.get("s3.secret-access-key"),
        region=properties.get("s3.region", "us-east-1"),
    )
