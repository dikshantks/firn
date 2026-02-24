"""Table API endpoints."""

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, status

from app.models import TableInfo, TableMetadata
from app.services import catalog_service, MetadataService

router = APIRouter()


@router.get("", response_model=list[TableInfo])
async def list_tables(
    catalog: str = Query(..., description="Catalog name"),
) -> list[TableInfo]:
    """List all tables in a catalog."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    metadata_service = MetadataService(pyiceberg_catalog, catalog)
    return metadata_service.list_tables()


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
