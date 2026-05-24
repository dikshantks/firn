"""Job management API endpoints with SSE streaming."""

import asyncio
import json
from typing import Any

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict

from app.services.job_service import job_service, JobStatus


router = APIRouter()


class JobResponse(BaseModel):
    """Response model for job status."""
    id: str
    status: str
    progress: int
    message: str
    result: Any = None
    error: str | None = None

    model_config = ConfigDict(from_attributes=True)


class JobCreatedResponse(BaseModel):
    """Response when a job is created."""
    job_id: str
    message: str = "Job created successfully"


@router.get("", response_model=list[JobResponse])
async def list_jobs(limit: int = 100) -> list[JobResponse]:
    """List all recent jobs."""
    jobs = job_service.list_jobs(limit=limit)
    return [
        JobResponse(
            id=job.id,
            status=job.status.value,
            progress=job.progress,
            message=job.message,
            result=job.result,
            error=job.error,
        )
        for job in jobs
    ]


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str) -> JobResponse:
    """Get status of a specific job."""
    job = job_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found"
        )
    
    return JobResponse(
        id=job.id,
        status=job.status.value,
        progress=job.progress,
        message=job.message,
        result=job.result,
        error=job.error,
    )


@router.get("/{job_id}/stream")
async def stream_job(job_id: str):
    """
    SSE endpoint for real-time job progress streaming.
    
    Clients can connect to this endpoint to receive real-time
    updates about job progress. The stream will close when the
    job completes or fails.
    
    Example usage in JavaScript:
    ```
    const eventSource = new EventSource('/api/jobs/{job_id}/stream');
    eventSource.onmessage = (event) => {
        const data = JSON.parse(event.data);
        console.log(data.progress, data.message);
    };
    ```
    """
    job = job_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found"
        )
    
    async def event_generator():
        """Generate SSE events for job progress."""
        last_progress = -1
        last_message = ""
        
        while True:
            job = job_service.get_job(job_id)
            
            if not job:
                yield f"data: {json.dumps({'error': 'Job not found'})}\n\n"
                break
            
            current_data = {
                "id": job.id,
                "status": job.status.value,
                "progress": job.progress,
                "message": job.message,
            }
            if job.result is not None:
                current_data["result"] = job.result
            if job.error:
                current_data["error"] = job.error
            
            if job.progress != last_progress or job.message != last_message:
                yield f"data: {json.dumps(current_data)}\n\n"
                last_progress = job.progress
                last_message = job.message
            
            if job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
                if job.progress == last_progress and job.message == last_message:
                    yield f"data: {json.dumps(current_data)}\n\n"
                break
            
            await asyncio.sleep(0.3)
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_job(job_id: str) -> None:
    """Delete a completed or failed job."""
    job = job_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found"
        )
    
    if job.status not in [JobStatus.COMPLETED, JobStatus.FAILED]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete a running job"
        )
    
    job_service.delete_job(job_id)


@router.post("/cleanup", response_model=dict)
async def cleanup_jobs(max_age_seconds: int = 3600) -> dict:
    """Remove old completed/failed jobs."""
    removed = job_service.cleanup_old_jobs(max_age_seconds)
    return {"removed": removed, "message": f"Removed {removed} old jobs"}
