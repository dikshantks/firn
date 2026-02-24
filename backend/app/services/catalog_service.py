"""Catalog service for managing Iceberg catalog connections."""

from typing import Any, Optional

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError

from app.models import CatalogType, CatalogInfo, CatalogTestResult


class CatalogService:
    """
    Service for managing multiple Iceberg catalog connections.
    
    Supports Hive Metastore and AWS Glue catalogs.
    """
    
    def __init__(self) -> None:
        """Initialize the catalog service."""
        self._catalogs: dict[str, Catalog] = {}
        self._configs: dict[str, dict[str, Any]] = {}
    
    def register_catalog(
        self,
        name: str,
        catalog_type: CatalogType,
        properties: dict[str, Any],
    ) -> CatalogInfo:
        """
        Register a new catalog connection.
        
        Args:
            name: Unique name for this catalog
            catalog_type: Type of catalog (hive or glue)
            properties: Catalog-specific connection properties
            
        Returns:
            CatalogInfo with connection details
            
        Raises:
            ValueError: If catalog with name already exists or connection fails
        """
        if name in self._catalogs:
            raise ValueError(f"Catalog '{name}' already exists")
        
        # Build catalog properties based on type
        catalog_props = {"type": catalog_type.value, **properties}
        
        # Add S3 path-style access for MinIO compatibility
        if "s3.endpoint" in properties and "s3.path-style-access" not in properties:
            catalog_props["s3.path-style-access"] = "true"
        
        try:
            # Load the catalog using pyiceberg
            catalog = load_catalog(name, **catalog_props)
            
            # Store catalog and config
            self._catalogs[name] = catalog
            self._configs[name] = {
                "name": name,
                "type": catalog_type,
                "properties": properties,
            }
            
            # Get basic stats
            namespace_count = 0
            table_count = 0
            try:
                namespaces = list(catalog.list_namespaces())
                namespace_count = len(namespaces)
                for ns in namespaces:
                    tables = list(catalog.list_tables(ns))
                    table_count += len(tables)
            except Exception:
                pass  # Stats are optional
            
            return CatalogInfo(
                name=name,
                type=catalog_type,
                properties=properties,
                connected=True,
                namespace_count=namespace_count,
                table_count=table_count,
            )
            
        except Exception as e:
            raise ValueError(f"Failed to connect to catalog: {str(e)}")
    
    def get_catalog(self, name: str) -> Optional[Catalog]:
        """
        Get a pyiceberg Catalog instance by name.
        
        Args:
            name: Catalog name
            
        Returns:
            Catalog instance or None if not found
        """
        return self._catalogs.get(name)
    
    def get_catalog_info(self, name: str) -> Optional[CatalogInfo]:
        """
        Get info about a registered catalog.
        
        Args:
            name: Catalog name
            
        Returns:
            CatalogInfo or None if not found
        """
        if name not in self._configs:
            return None
        
        config = self._configs[name]
        catalog = self._catalogs.get(name)
        
        # Check if still connected
        connected = False
        namespace_count = None
        table_count = None
        
        if catalog:
            try:
                namespaces = list(catalog.list_namespaces())
                connected = True
                namespace_count = len(namespaces)
                table_count = sum(len(list(catalog.list_tables(ns))) for ns in namespaces)
            except Exception:
                connected = False
        
        return CatalogInfo(
            name=config["name"],
            type=config["type"],
            properties=config["properties"],
            connected=connected,
            namespace_count=namespace_count,
            table_count=table_count,
        )
    
    def list_catalogs(self) -> list[CatalogInfo]:
        """
        List all registered catalogs.
        
        Returns:
            List of CatalogInfo objects
        """
        return [
            self.get_catalog_info(name)
            for name in self._configs
            if self.get_catalog_info(name) is not None
        ]
    
    def test_catalog(self, name: str) -> Optional[CatalogTestResult]:
        """
        Test connectivity to a catalog.
        
        Args:
            name: Catalog name
            
        Returns:
            CatalogTestResult or None if catalog not found
        """
        if name not in self._catalogs:
            return None
        
        catalog = self._catalogs[name]
        
        try:
            # Try to list namespaces as a connectivity test
            namespaces = list(catalog.list_namespaces())
            namespace_count = len(namespaces)
            
            # Count tables
            table_count = 0
            for ns in namespaces:
                tables = list(catalog.list_tables(ns))
                table_count += len(tables)
            
            return CatalogTestResult(
                name=name,
                success=True,
                message=f"Successfully connected. Found {namespace_count} namespaces and {table_count} tables.",
                namespace_count=namespace_count,
                table_count=table_count,
            )
            
        except Exception as e:
            return CatalogTestResult(
                name=name,
                success=False,
                message="Connection failed",
                error=str(e),
            )
    
    def remove_catalog(self, name: str) -> bool:
        """
        Remove a catalog connection.
        
        Args:
            name: Catalog name
            
        Returns:
            True if removed, False if not found
        """
        if name not in self._catalogs:
            return False
        
        del self._catalogs[name]
        del self._configs[name]
        return True
    
    def reload_catalog(self, name: str) -> bool:
        """
        Reload a catalog connection (useful after config changes).
        
        Args:
            name: Catalog name
            
        Returns:
            True if reloaded successfully
        """
        if name not in self._configs:
            return False
        
        config = self._configs[name]
        
        # Remove old connection
        if name in self._catalogs:
            del self._catalogs[name]
        
        # Reconnect
        try:
            catalog_props = {"type": config["type"].value, **config["properties"]}
            if "s3.endpoint" in config["properties"]:
                catalog_props["s3.path-style-access"] = "true"
            
            catalog = load_catalog(name, **catalog_props)
            self._catalogs[name] = catalog
            return True
        except Exception:
            return False


# Global catalog service instance
catalog_service = CatalogService()
