"""Puffin statistics API endpoints."""

from fastapi import APIRouter, HTTPException, Query, status

from app.models.puffin import PuffinFileInfo, TableStatistics
from app.services import catalog_service, PuffinService

router = APIRouter()


@router.get("/{namespace}/{table}/statistics", response_model=list[PuffinFileInfo])
async def list_statistics_files(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
) -> list[PuffinFileInfo]:
    """List Puffin statistics files for a table."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    puffin_service = PuffinService(pyiceberg_catalog)
    try:
        return puffin_service.list_statistics_files(namespace, table)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing statistics files: {str(e)}",
        )


@router.get("/{namespace}/{table}/statistics/{snapshot_id}", response_model=TableStatistics)
async def get_statistics(
    namespace: str,
    table: str,
    snapshot_id: int,
    catalog: str = Query(..., description="Catalog name"),
) -> TableStatistics:
    """Get decoded statistics for a snapshot."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    puffin_service = PuffinService(pyiceberg_catalog)
    try:
        stats = puffin_service.get_statistics(namespace, table, snapshot_id)
        if not stats:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No statistics found for snapshot {snapshot_id}",
            )
        return stats
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting statistics: {str(e)}",
        )
