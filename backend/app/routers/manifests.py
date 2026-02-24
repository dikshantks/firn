"""Manifest API endpoints."""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status

from app.models import ManifestInfo, ManifestEntry, ManifestListInfo
from app.services import catalog_service, ManifestService

router = APIRouter()


@router.get("/{namespace}/{table}/snapshots/{snapshot_id}/manifests", response_model=ManifestListInfo)
async def get_manifest_list(
    namespace: str,
    table: str,
    snapshot_id: int,
    catalog: str = Query(..., description="Catalog name"),
) -> ManifestListInfo:
    """Get manifest list for a snapshot."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    manifest_service = ManifestService(pyiceberg_catalog)
    try:
        return manifest_service.get_manifest_list(namespace, table, snapshot_id)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting manifest list: {str(e)}",
        )


@router.get("/{namespace}/{table}/manifests", response_model=list[ManifestEntry])
async def get_manifest_entries(
    namespace: str,
    table: str,
    path: str = Query(..., description="Manifest file path"),
    catalog: str = Query(..., description="Catalog name"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum entries to return"),
) -> list[ManifestEntry]:
    """Get entries (data files) from a manifest file."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    manifest_service = ManifestService(pyiceberg_catalog)
    try:
        return manifest_service.get_manifest_entries(namespace, table, path, limit)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error reading manifest: {str(e)}",
        )
