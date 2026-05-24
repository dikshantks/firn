"""Catalog service for managing Iceberg catalog connections."""

from typing import Any, Optional

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.exceptions import NoSuchTableError
from sqlalchemy import delete, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from app.db import is_database_enabled, session_scope
from app.db.models import CatalogRegistry
from app.models import CatalogType, CatalogInfo, CatalogTestResult


SENSITIVE_PROPERTY_PARTS = (
    "access-key",
    "access_key",
    "secret",
    "password",
    "session-token",
    "session_token",
    "token",
)


class CatalogService:
    """
    Service for managing multiple Iceberg catalog connections.
    
    Supports Hive Metastore and AWS Glue catalogs.
    """
    
    def __init__(self) -> None:
        """Initialize the catalog service."""
        self._catalogs: dict[str, Catalog] = {}
        self._configs: dict[str, dict[str, Any]] = {}

    @property
    def use_database(self) -> bool:
        """Return whether catalog registry persistence is enabled."""
        return is_database_enabled()

    def _sanitize_properties(self, properties: dict[str, Any]) -> dict[str, Any]:
        """Redact credential-like catalog properties before returning API data."""
        redacted: dict[str, Any] = {}
        for key, value in properties.items():
            normalized = key.lower().replace(".", "-")
            if any(part in normalized for part in SENSITIVE_PROPERTY_PARTS):
                redacted[key] = "***REDACTED***"
            else:
                redacted[key] = value
        return redacted

    def _build_catalog_props(
        self,
        catalog_type: CatalogType,
        properties: dict[str, Any],
    ) -> dict[str, Any]:
        """Normalize user-facing catalog config into PyIceberg properties."""
        catalog_props = {"type": catalog_type.value, **properties}

        if catalog_type == CatalogType.GLUE:
            property_mapping = {
                "region_name": "client.region",
                "aws_access_key_id": "client.access-key-id",
                "aws_secret_access_key": "client.secret-access-key",
                "aws_session_token": "client.session-token",
                "profile_name": "client.profile-name",
            }

            for boto3_key, pyiceberg_key in property_mapping.items():
                if boto3_key in catalog_props and pyiceberg_key not in catalog_props:
                    catalog_props[pyiceberg_key] = catalog_props.pop(boto3_key)

            if "s3.region" not in catalog_props and "client.region" in catalog_props:
                catalog_props["s3.region"] = catalog_props["client.region"]

            if "client.access-key-id" in catalog_props and "s3.access-key-id" not in catalog_props:
                catalog_props["s3.access-key-id"] = catalog_props["client.access-key-id"]

            if "client.secret-access-key" in catalog_props and "s3.secret-access-key" not in catalog_props:
                catalog_props["s3.secret-access-key"] = catalog_props["client.secret-access-key"]

            if "client.session-token" in catalog_props and "s3.session-token" not in catalog_props:
                catalog_props["s3.session-token"] = catalog_props["client.session-token"]

            glue_to_s3_mapping = {
                "glue.region": "s3.region",
                "glue.access-key-id": "s3.access-key-id",
                "glue.secret-access-key": "s3.secret-access-key",
                "glue.session-token": "s3.session-token",
            }

            for glue_key, s3_key in glue_to_s3_mapping.items():
                if glue_key in catalog_props:
                    catalog_props[s3_key] = catalog_props[glue_key]

        if "s3.endpoint" in properties and "s3.path-style-access" not in properties:
            catalog_props["s3.path-style-access"] = "true"

        return catalog_props

    def _load_pyiceberg_catalog(
        self,
        name: str,
        catalog_type: CatalogType,
        properties: dict[str, Any],
    ) -> Catalog:
        catalog_props = self._build_catalog_props(catalog_type, properties)
        if catalog_type == CatalogType.GLUE:
            return GlueCatalog(name, **catalog_props)
        return load_catalog(name, **catalog_props)

    def _save_registry_row(
        self,
        name: str,
        catalog_type: CatalogType,
        properties: dict[str, Any],
    ) -> None:
        values = {
            "name": name,
            "type": catalog_type.value,
            "config_json": properties,
        }
        stmt = mysql_insert(CatalogRegistry).values(**values)
        stmt = stmt.on_duplicate_key_update(
            type=values["type"],
            config_json=values["config_json"],
        )
        with session_scope() as session:
            session.execute(stmt)

    def _get_registry_row(self, name: str) -> CatalogRegistry | None:
        with session_scope() as session:
            return session.execute(
                select(CatalogRegistry).where(CatalogRegistry.name == name)
            ).scalar_one_or_none()

    def _row_to_config(self, row: CatalogRegistry) -> dict[str, Any]:
        return {
            "name": row.name,
            "type": CatalogType(row.type),
            "properties": row.config_json,
        }
    
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
        if not self.use_database and name in self._catalogs:
            raise ValueError(f"Catalog '{name}' already exists")

        try:
            catalog = self._load_pyiceberg_catalog(name, catalog_type, properties)
            
            self._catalogs[name] = catalog
            self._configs[name] = {
                "name": name,
                "type": catalog_type,
                "properties": properties,
            }
            if self.use_database:
                self._save_registry_row(name, catalog_type, properties)
            
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
                properties=self._sanitize_properties(properties),
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
        catalog = self._catalogs.get(name)
        if catalog or not self.use_database:
            return catalog

        row = self._get_registry_row(name)
        if not row:
            return None

        config = self._row_to_config(row)
        try:
            catalog = self._load_pyiceberg_catalog(
                config["name"],
                config["type"],
                config["properties"],
            )
        except Exception:
            return None

        self._catalogs[name] = catalog
        self._configs[name] = config
        return catalog
    
    def get_catalog_info(self, name: str, include_stats: bool = False) -> Optional[CatalogInfo]:
        """
        Get info about a registered catalog.
        
        Args:
            name: Catalog name
            include_stats: If True, count namespaces and tables (slow for large catalogs)
            
        Returns:
            CatalogInfo or None if not found
        """
        if name not in self._configs:
            if not self.use_database:
                return None
            row = self._get_registry_row(name)
            if not row:
                return None
            self._configs[name] = self._row_to_config(row)

        config = self._configs[name]
        catalog = self.get_catalog(name)
        
        # Check if still connected (quick check - just verify catalog exists)
        connected = catalog is not None
        namespace_count = None
        table_count = None
        
        # Only fetch stats if explicitly requested (this is slow for large catalogs)
        if include_stats and catalog:
            try:
                namespaces = list(catalog.list_namespaces())
                connected = True
                namespace_count = len(namespaces)
                # Only count tables if there are few namespaces
                if namespace_count <= 50:
                    table_count = sum(len(list(catalog.list_tables(ns))) for ns in namespaces)
                else:
                    # For large catalogs, skip table count to avoid 6000+ API calls
                    table_count = None
            except Exception:
                connected = False
        
        return CatalogInfo(
            name=config["name"],
            type=config["type"],
            properties=self._sanitize_properties(config["properties"]),
            connected=connected,
            namespace_count=namespace_count,
            table_count=table_count,
        )
    
    def list_catalogs(self, include_stats: bool = False) -> list[CatalogInfo]:
        """
        List all registered catalogs.
        
        Args:
            include_stats: If True, include namespace/table counts (slow for large catalogs)
        
        Returns:
            List of CatalogInfo objects
        """
        if self.use_database:
            with session_scope() as session:
                rows = session.execute(select(CatalogRegistry)).scalars().all()
            self._configs.update({row.name: self._row_to_config(row) for row in rows})

        results = []
        for name in list(self._configs):
            info = self.get_catalog_info(name, include_stats=include_stats)
            if info is not None:
                results.append(info)
        return results
    
    def test_catalog(self, name: str) -> Optional[CatalogTestResult]:
        """
        Test connectivity to a catalog.
        
        Args:
            name: Catalog name
            
        Returns:
            CatalogTestResult or None if catalog not found
        """
        catalog = self.get_catalog(name)
        if not catalog:
            return None
        
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
        if self.use_database:
            with session_scope() as session:
                deleted = session.execute(
                    delete(CatalogRegistry).where(CatalogRegistry.name == name)
                ).rowcount or 0
            self._catalogs.pop(name, None)
            self._configs.pop(name, None)
            return deleted > 0

        if name not in self._catalogs:
            return False

        self._catalogs.pop(name, None)
        self._configs.pop(name, None)
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
            if not self.use_database:
                return False
            row = self._get_registry_row(name)
            if not row:
                return False
            self._configs[name] = self._row_to_config(row)
        
        config = self._configs[name]
        
        # Remove old connection
        if name in self._catalogs:
            del self._catalogs[name]
        
        # Reconnect
        try:
            catalog = self._load_pyiceberg_catalog(
                name,
                config["type"],
                config["properties"],
            )
            self._catalogs[name] = catalog
            return True
        except Exception:
            return False


# Global catalog service instance
catalog_service = CatalogService()
