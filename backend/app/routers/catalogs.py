"""Catalog management API endpoints."""

from fastapi import APIRouter, HTTPException, status

from app.models import CatalogCreate, CatalogInfo, CatalogTestResult
from app.services import catalog_service

router = APIRouter()


@router.post("", response_model=CatalogInfo, status_code=status.HTTP_201_CREATED)
async def create_catalog(catalog: CatalogCreate) -> CatalogInfo:
    """Register a new catalog connection."""
    try:
        return catalog_service.register_catalog(
            name=catalog.name,
            catalog_type=catalog.type,
            properties=catalog.properties,
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.get("", response_model=list[CatalogInfo])
async def list_catalogs() -> list[CatalogInfo]:
    """List all registered catalogs."""
    return catalog_service.list_catalogs()


@router.get("/{name}", response_model=CatalogInfo)
async def get_catalog(name: str) -> CatalogInfo:
    """Get details of a specific catalog."""
    catalog = catalog_service.get_catalog_info(name)
    if not catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{name}' not found",
        )
    return catalog


@router.get("/{name}/test", response_model=CatalogTestResult)
async def test_catalog(name: str) -> CatalogTestResult:
    """Test connectivity to a catalog."""
    result = catalog_service.test_catalog(name)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{name}' not found",
        )
    return result


@router.delete("/{name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_catalog(name: str) -> None:
    """Remove a catalog connection."""
    success = catalog_service.remove_catalog(name)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{name}' not found",
        )
