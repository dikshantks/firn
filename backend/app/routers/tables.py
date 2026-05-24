"""Table API endpoints."""

import asyncio
import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, status

from app.models import TableInfo, TableMetadata
from app.services import catalog_service, MetadataService

router = APIRouter()
logger = logging.getLogger(__name__)


def _catalog_unavailable_error(catalog: str, error: Exception) -> HTTPException:
    """Return a clean API error when an external catalog cannot be reached."""
    logger.warning("Catalog '%s' is unavailable: %s", catalog, error)
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=(
            f"Catalog '{catalog}' is unavailable. "
            f"Check the catalog URI, network/DNS, and metastore service. Error: {error}"
        ),
    )


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
    
    try:
        namespaces = await asyncio.to_thread(list, pyiceberg_catalog.list_namespaces())
    except Exception as e:
        raise _catalog_unavailable_error(catalog, e) from e
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
    try:
        return await asyncio.to_thread(
            lambda: metadata_service.list_tables(
                namespace=namespace,
                lazy=lazy,
                limit=limit,
                offset=offset,
            )
        )
    except Exception as e:
        raise _catalog_unavailable_error(catalog, e) from e


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
