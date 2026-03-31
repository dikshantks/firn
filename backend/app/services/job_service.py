"""Job service for background task management with progress tracking."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


class JobStatus(str, Enum):
    """Status of a background job."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Job:
    """Represents a background job with progress tracking."""
    id: str
    status: JobStatus
    progress: int = 0
    message: str = ""
    result: Any = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


class JobService:
    """
    Service for managing background jobs with progress tracking.
    
    Uses ThreadPoolExecutor to run tasks in background threads,
    allowing FastAPI to handle other requests while long-running
    operations complete.
    """
    
    def __init__(self, max_workers: int = 4):
        """
        Initialize the job service.
        
        Args:
            max_workers: Maximum number of concurrent background tasks
        """
        self._jobs: dict[str, Job] = {}
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
    
    def create_job(self, initial_message: str = "") -> Job:
        """
        Create a new job and return it.
        
        Args:
            initial_message: Optional initial status message
            
        Returns:
            The created Job object
        """
        job_id = str(uuid.uuid4())
        job = Job(
            id=job_id,
            status=JobStatus.PENDING,
            message=initial_message or "Job created, waiting to start..."
        )
        self._jobs[job_id] = job
        return job
    
    def update_job(
        self,
        job_id: str,
        status: Optional[JobStatus] = None,
        progress: Optional[int] = None,
        message: Optional[str] = None,
        result: Any = None,
        error: Optional[str] = None,
    ) -> Optional[Job]:
        """
        Update a job's status and progress.
        
        Args:
            job_id: The job ID to update
            status: New status (optional)
            progress: Progress percentage 0-100 (optional)
            message: Status message (optional)
            result: Result data when completed (optional)
            error: Error message if failed (optional)
            
        Returns:
            The updated Job or None if not found
        """
        if job_id not in self._jobs:
            return None
        
        job = self._jobs[job_id]
        
        if status is not None:
            job.status = status
        if progress is not None:
            job.progress = min(100, max(0, progress))
        if message is not None:
            job.message = message
        if result is not None:
            job.result = result
        if error is not None:
            job.error = error
        
        job.updated_at = datetime.utcnow()
        return job
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """
        Get a job by ID.
        
        Args:
            job_id: The job ID
            
        Returns:
            The Job or None if not found
        """
        return self._jobs.get(job_id)
    
    def list_jobs(self, limit: int = 100) -> list[Job]:
        """
        List recent jobs.
        
        Args:
            limit: Maximum number of jobs to return
            
        Returns:
            List of jobs, most recent first
        """
        jobs = list(self._jobs.values())
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return jobs[:limit]
    
    def run_in_background(
        self,
        job_id: str,
        func: Callable[..., Any],
        *args,
        **kwargs
    ) -> None:
        """
        Run a function in the background thread pool.
        
        The function will receive job_id as a keyword argument
        so it can update progress via job_service.update_job().
        
        Args:
            job_id: The job ID to associate with this task
            func: The function to run
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
        """
        self._executor.submit(self._run_task, job_id, func, args, kwargs)
    
    def _run_task(
        self,
        job_id: str,
        func: Callable[..., Any],
        args: tuple,
        kwargs: dict
    ) -> None:
        """
        Internal method to run a task and handle status updates.
        """
        self.update_job(job_id, status=JobStatus.RUNNING, message="Task started...")
        
        try:
            result = func(*args, job_id=job_id, **kwargs)
            self.update_job(
                job_id,
                status=JobStatus.COMPLETED,
                progress=100,
                message="Completed successfully",
                result=result
            )
        except Exception as e:
            self.update_job(
                job_id,
                status=JobStatus.FAILED,
                message="Task failed",
                error=str(e)
            )
    
    def cleanup_old_jobs(self, max_age_seconds: int = 3600) -> int:
        """
        Remove jobs older than max_age_seconds.
        
        Args:
            max_age_seconds: Maximum age in seconds (default 1 hour)
            
        Returns:
            Number of jobs removed
        """
        now = datetime.utcnow()
        to_remove = []
        
        for job_id, job in self._jobs.items():
            age = (now - job.created_at).total_seconds()
            if age > max_age_seconds and job.status in [JobStatus.COMPLETED, JobStatus.FAILED]:
                to_remove.append(job_id)
        
        for job_id in to_remove:
            del self._jobs[job_id]
        
        return len(to_remove)


job_service = JobService()
