"""Catalog management API endpoints."""

from fastapi import APIRouter, HTTPException, Query, status

from app.models import CatalogCreate, CatalogInfo, CatalogTestResult
from app.services import catalog_service
from app.services.job_service import job_service, JobStatus

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
async def list_catalogs(
    include_stats: bool = Query(
        False,
        description="Include namespace/table counts (slow for large catalogs)"
    ),
) -> list[CatalogInfo]:
    """
    List all registered catalogs.
    
    By default, returns catalog info without namespace/table counts for fast response.
    Set include_stats=true to fetch counts (may be slow for large catalogs).
    """
    return catalog_service.list_catalogs(include_stats=include_stats)


@router.get("/{name}", response_model=CatalogInfo)
async def get_catalog(
    name: str,
    include_stats: bool = Query(
        False,
        description="Include namespace/table counts (slow for large catalogs)"
    ),
) -> CatalogInfo:
    """
    Get details of a specific catalog.
    
    By default, returns catalog info without namespace/table counts for fast response.
    Set include_stats=true to fetch counts (may be slow for large catalogs).
    """
    catalog = catalog_service.get_catalog_info(name, include_stats=include_stats)
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


@router.post("/async", status_code=status.HTTP_202_ACCEPTED)
async def create_catalog_async(catalog: CatalogCreate) -> dict:
    """
    Register a new catalog connection in the background.
    
    Returns a job ID that can be used to track progress via:
    - GET /api/jobs/{job_id} - Get current status
    - GET /api/jobs/{job_id}/stream - SSE stream for real-time updates
    
    This is useful for large catalogs where registration may take
    a long time due to namespace/table enumeration.
    """
    job = job_service.create_job(initial_message="Starting catalog registration...")
    
    def register_with_progress(job_id: str):
        job_service.update_job(
            job_id,
            progress=10,
            message=f"Connecting to {catalog.type.value} catalog '{catalog.name}'..."
        )
        
        try:
            result = catalog_service.register_catalog(
                name=catalog.name,
                catalog_type=catalog.type,
                properties=catalog.properties,
            )
            
            job_service.update_job(
                job_id,
                progress=90,
                message="Catalog registered, gathering statistics..."
            )
            
            return {
                "name": result.name,
                "type": result.type.value,
                "connected": result.connected,
                "namespace_count": result.namespace_count,
                "table_count": result.table_count,
            }
        except ValueError as e:
            raise Exception(str(e))
    
    job_service.run_in_background(job.id, register_with_progress)
    
    return {
        "job_id": job.id,
        "message": "Catalog registration started. Use the job_id to track progress.",
    }
