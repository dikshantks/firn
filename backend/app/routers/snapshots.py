"""Snapshot API endpoints."""

from fastapi import APIRouter, HTTPException, Query, status

from app.models import SnapshotInfo, SnapshotGraph, SnapshotComparison, SnapshotDetails
from app.services import catalog_service, SnapshotService, ManifestService

router = APIRouter()


@router.get("/{namespace}/{table}/snapshots/details", response_model=list[SnapshotDetails])
async def get_all_snapshots_details(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
    entry_limit: int = Query(100, ge=1, le=500, description="Max entries per manifest"),
) -> list[SnapshotDetails]:
    """Get full details for all snapshots: manifest list, manifests, and data/delete files."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )

    manifest_service = ManifestService(pyiceberg_catalog)
    try:
        return manifest_service.get_all_snapshots_details(
            namespace, table, entry_limit=entry_limit
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting snapshot details: {str(e)}",
        )


@router.get("/{namespace}/{table}/snapshots", response_model=SnapshotGraph)
async def get_snapshot_graph(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
) -> SnapshotGraph:
    """Get snapshot lineage graph for visualization."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    snapshot_service = SnapshotService(pyiceberg_catalog)
    try:
        return snapshot_service.get_snapshot_graph(namespace, table)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting snapshots: {str(e)}",
        )


@router.get("/{namespace}/{table}/snapshots/{snapshot_id}", response_model=SnapshotInfo)
async def get_snapshot(
    namespace: str,
    table: str,
    snapshot_id: int,
    catalog: str = Query(..., description="Catalog name"),
) -> SnapshotInfo:
    """Get details of a specific snapshot."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    snapshot_service = SnapshotService(pyiceberg_catalog)
    try:
        snapshot = snapshot_service.get_snapshot(namespace, table, snapshot_id)
        if not snapshot:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Snapshot {snapshot_id} not found",
            )
        return snapshot
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting snapshot: {str(e)}",
        )


@router.post("/{namespace}/{table}/snapshots/compare", response_model=SnapshotComparison)
async def compare_snapshots(
    namespace: str,
    table: str,
    snapshot1: int = Query(..., description="First snapshot ID"),
    snapshot2: int = Query(..., description="Second snapshot ID"),
    catalog: str = Query(..., description="Catalog name"),
) -> SnapshotComparison:
    """Compare two snapshots."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    snapshot_service = SnapshotService(pyiceberg_catalog)
    try:
        return snapshot_service.compare_snapshots(namespace, table, snapshot1, snapshot2)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error comparing snapshots: {str(e)}",
        )
