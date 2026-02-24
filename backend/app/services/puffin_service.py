"""Puffin service for reading Iceberg statistics files."""

import io
import struct
from typing import Any, Optional

from pyiceberg.catalog import Catalog

from app.models.puffin import (
    PuffinFileInfo,
    BlobMetadata,
    ColumnStatistics,
    TableStatistics,
)


# Puffin file format constants
PUFFIN_MAGIC = b"PUF"
PUFFIN_MAGIC_SIZE = 4
PUFFIN_FOOTER_SIZE = 4 + 4 + 4  # flags + footer_payload_size + magic


class PuffinService:
    """Service for reading Iceberg Puffin statistics files."""
    
    def __init__(self, catalog: Catalog) -> None:
        """
        Initialize the puffin service.
        
        Args:
            catalog: pyiceberg Catalog instance
        """
        self.catalog = catalog
    
    def _load_table(self, namespace: str, table_name: str):
        """Load a table from the catalog."""
        return self.catalog.load_table((namespace, table_name))
    
    def list_statistics_files(
        self,
        namespace: str,
        table_name: str,
    ) -> list[PuffinFileInfo]:
        """
        List Puffin statistics files for a table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            
        Returns:
            List of PuffinFileInfo objects
        """
        table = self._load_table(namespace, table_name)
        metadata = table.metadata
        
        puffin_files = []
        
        # Check for statistics in metadata
        # Statistics are stored in metadata.statistics (list of StatisticsFile)
        if hasattr(metadata, 'statistics') and metadata.statistics:
            for stat_file in metadata.statistics:
                # Try to read the puffin file to get blob info
                try:
                    file_io = table.io
                    input_file = file_io.new_input(stat_file.statistics_path)
                    
                    with input_file.open() as f:
                        data = f.read()
                    
                    blobs = self._parse_puffin_file(data)
                    
                    puffin_files.append(PuffinFileInfo(
                        file_path=stat_file.statistics_path,
                        snapshot_id=stat_file.snapshot_id,
                        file_size_bytes=len(data),
                        blob_count=len(blobs),
                        blobs=blobs,
                    ))
                except Exception as e:
                    # If we can't read the file, still include basic info
                    puffin_files.append(PuffinFileInfo(
                        file_path=stat_file.statistics_path,
                        snapshot_id=stat_file.snapshot_id,
                        file_size_bytes=0,
                        blob_count=0,
                        blobs=[],
                    ))
        
        return puffin_files
    
    def _parse_puffin_file(self, data: bytes) -> list[BlobMetadata]:
        """
        Parse a Puffin file and extract blob metadata.
        
        Puffin file format:
        - Magic (4 bytes): "PUF" + version byte
        - Blobs (variable): blob data
        - Footer:
          - Blob metadata (JSON array)
          - Footer payload size (4 bytes)
          - Flags (4 bytes)
          - Magic (4 bytes)
        
        Args:
            data: Raw bytes of Puffin file
            
        Returns:
            List of BlobMetadata objects
        """
        if len(data) < PUFFIN_MAGIC_SIZE + PUFFIN_FOOTER_SIZE:
            return []
        
        # Check magic at start
        if data[:3] != PUFFIN_MAGIC:
            return []
        
        # Check magic at end
        if data[-4:-1] != PUFFIN_MAGIC:
            return []
        
        # Read footer
        footer_payload_size = struct.unpack('<I', data[-12:-8])[0]
        flags = struct.unpack('<I', data[-8:-4])[0]
        
        # Read footer payload (JSON blob metadata)
        footer_start = len(data) - 12 - footer_payload_size
        footer_payload = data[footer_start:footer_start + footer_payload_size]
        
        # Check if footer is compressed (flag bit 0)
        is_compressed = flags & 1
        
        if is_compressed:
            # Decompress using LZ4 or ZSTD based on flag
            try:
                import lz4.frame
                footer_payload = lz4.frame.decompress(footer_payload)
            except Exception:
                try:
                    import zstandard
                    dctx = zstandard.ZstdDecompressor()
                    footer_payload = dctx.decompress(footer_payload)
                except Exception:
                    return []
        
        # Parse JSON blob metadata
        try:
            import json
            blob_metadata_list = json.loads(footer_payload.decode('utf-8'))
        except Exception:
            return []
        
        blobs = []
        for blob_meta in blob_metadata_list:
            blobs.append(BlobMetadata(
                type=blob_meta.get('type', 'unknown'),
                snapshot_id=blob_meta.get('snapshot-id', 0),
                sequence_number=blob_meta.get('sequence-number', 0),
                fields=blob_meta.get('fields', []),
                offset=blob_meta.get('offset', 0),
                length=blob_meta.get('length', 0),
                compression_codec=blob_meta.get('compression-codec'),
                properties=blob_meta.get('properties', {}),
            ))
        
        return blobs
    
    def get_statistics(
        self,
        namespace: str,
        table_name: str,
        snapshot_id: int,
    ) -> Optional[TableStatistics]:
        """
        Get decoded statistics for a snapshot.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            snapshot_id: Snapshot ID
            
        Returns:
            TableStatistics or None if not found
        """
        table = self._load_table(namespace, table_name)
        metadata = table.metadata
        
        # Get schema for column name mapping
        schema = table.schema()
        column_names = {field.field_id: field.name for field in schema.fields}
        
        # Find statistics file for this snapshot
        if not hasattr(metadata, 'statistics') or not metadata.statistics:
            return None
        
        stat_file = None
        for sf in metadata.statistics:
            if sf.snapshot_id == snapshot_id:
                stat_file = sf
                break
        
        if not stat_file:
            return None
        
        # Read the puffin file
        try:
            file_io = table.io
            input_file = file_io.new_input(stat_file.statistics_path)
            
            with input_file.open() as f:
                data = f.read()
            
            blobs = self._parse_puffin_file(data)
            
            # Decode blob data
            column_statistics = []
            
            for blob in blobs:
                if blob.type == 'apache-datasketches-theta-v1':
                    # Decode Theta sketch for NDV
                    ndv = self._decode_theta_sketch(data, blob)
                    
                    for field_id in blob.fields:
                        column_statistics.append(ColumnStatistics(
                            column_id=field_id,
                            column_name=column_names.get(field_id),
                            ndv=ndv,
                            blob_type=blob.type,
                        ))
                
                elif blob.type == 'ndv':
                    # Simple NDV blob
                    ndv = self._decode_ndv_blob(data, blob)
                    
                    for field_id in blob.fields:
                        column_statistics.append(ColumnStatistics(
                            column_id=field_id,
                            column_name=column_names.get(field_id),
                            ndv=ndv,
                            blob_type=blob.type,
                        ))
            
            return TableStatistics(
                snapshot_id=snapshot_id,
                puffin_file_path=stat_file.statistics_path,
                column_statistics=column_statistics,
            )
            
        except Exception as e:
            print(f"Error reading statistics: {e}")
            return None
    
    def _decode_theta_sketch(self, data: bytes, blob: BlobMetadata) -> Optional[int]:
        """
        Decode a Theta sketch blob to get NDV estimate.
        
        Args:
            data: Full puffin file data
            blob: Blob metadata
            
        Returns:
            NDV estimate or None
        """
        try:
            blob_data = data[blob.offset:blob.offset + blob.length]
            
            # Decompress if needed
            if blob.compression_codec:
                if blob.compression_codec.lower() == 'lz4':
                    import lz4.frame
                    blob_data = lz4.frame.decompress(blob_data)
                elif blob.compression_codec.lower() in ('zstd', 'zstandard'):
                    import zstandard
                    dctx = zstandard.ZstdDecompressor()
                    blob_data = dctx.decompress(blob_data)
            
            # Try to decode using datasketches library
            try:
                from datasketches import theta_sketch
                sketch = theta_sketch.deserialize(blob_data)
                return int(sketch.get_estimate())
            except ImportError:
                # datasketches not installed, try basic parsing
                # Theta sketch format: first 8 bytes contain estimate info
                if len(blob_data) >= 8:
                    # This is a simplified estimate - actual decoding is more complex
                    return None
            
        except Exception:
            pass
        
        return None
    
    def _decode_ndv_blob(self, data: bytes, blob: BlobMetadata) -> Optional[int]:
        """
        Decode a simple NDV blob.
        
        Args:
            data: Full puffin file data
            blob: Blob metadata
            
        Returns:
            NDV value or None
        """
        try:
            blob_data = data[blob.offset:blob.offset + blob.length]
            
            # Decompress if needed
            if blob.compression_codec:
                if blob.compression_codec.lower() == 'lz4':
                    import lz4.frame
                    blob_data = lz4.frame.decompress(blob_data)
                elif blob.compression_codec.lower() in ('zstd', 'zstandard'):
                    import zstandard
                    dctx = zstandard.ZstdDecompressor()
                    blob_data = dctx.decompress(blob_data)
            
            # Simple NDV is typically stored as a long (8 bytes)
            if len(blob_data) >= 8:
                return struct.unpack('<Q', blob_data[:8])[0]
            
        except Exception:
            pass
        
        return None
