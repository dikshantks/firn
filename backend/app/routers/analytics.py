"""Analytics API endpoints."""

from typing import Any

from fastapi import APIRouter, HTTPException, Query, status

from app.services import catalog_service, SnapshotService, DataFileService

router = APIRouter()


@router.get("/{namespace}/{table}/analytics/storage")
async def get_storage_analytics(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
) -> dict[str, Any]:
    """Get storage analytics for a table."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    data_file_service = DataFileService(pyiceberg_catalog)
    try:
        return data_file_service.get_storage_analytics(namespace, table)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting storage analytics: {str(e)}",
        )


@router.get("/{namespace}/{table}/analytics/history")
async def get_operation_history(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
) -> list[dict[str, Any]]:
    """Get operation history across snapshots."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    snapshot_service = SnapshotService(pyiceberg_catalog)
    try:
        return snapshot_service.get_operation_history(namespace, table)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting operation history: {str(e)}",
        )
