"""Table API endpoints."""

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, status

from app.models import TableInfo, TableMetadata
from app.services import catalog_service, MetadataService

router = APIRouter()


@router.get("/namespaces", response_model=list[str])
async def list_namespaces(
    catalog: str = Query(..., description="Catalog name"),
) -> list[str]:
    """List all namespaces in a catalog (fast, no S3 calls)."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    namespaces = pyiceberg_catalog.list_namespaces()
    return [".".join(ns) for ns in namespaces]


@router.get("", response_model=list[TableInfo])
async def list_tables(
    catalog: str = Query(..., description="Catalog name"),
    namespace: Optional[str] = Query(None, description="Filter by namespace"),
    lazy: bool = Query(False, description="If true, skip metadata loading (fast mode)"),
    limit: Optional[int] = Query(None, description="Max tables to return"),
    offset: int = Query(0, description="Offset for pagination"),
) -> list[TableInfo]:
    """
    List tables in a catalog.
    
    Use lazy=true for fast listing without S3 calls (only table names).
    Use namespace parameter to filter by specific namespace.
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    metadata_service = MetadataService(pyiceberg_catalog, catalog)
    return metadata_service.list_tables(
        namespace=namespace,
        lazy=lazy,
        limit=limit,
        offset=offset,
    )


@router.get("/{namespace}/{table}", response_model=TableMetadata)
async def get_table(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
) -> TableMetadata:
    """Get detailed metadata for a table."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    metadata_service = MetadataService(pyiceberg_catalog, catalog)
    try:
        return metadata_service.get_table_metadata(namespace, table)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{namespace}.{table}' not found: {str(e)}",
        )


@router.get("/{namespace}/{table}/metadata", response_model=dict[str, Any])
async def get_raw_metadata(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
) -> dict[str, Any]:
    """Get raw metadata.json content for a table."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    metadata_service = MetadataService(pyiceberg_catalog, catalog)
    try:
        return metadata_service.get_raw_metadata(namespace, table)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{namespace}.{table}' not found: {str(e)}",
        )
