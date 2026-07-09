"""Job service for background task management with progress tracking."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from sqlalchemy import delete, select

from app.config import settings
from app.db import is_database_enabled, session_scope
from app.db.models import JobRecord


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
    type: str = "generic"
    progress: int = 0
    message: str = ""
    result: Any = None
    error: Optional[str] = None
    payload: Optional[dict[str, Any]] = None
    catalog: Optional[str] = None
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

    @property
    def use_database(self) -> bool:
        """Return whether DB-backed jobs are enabled."""
        return is_database_enabled()

    def _record_to_job(self, record: JobRecord) -> Job:
        return Job(
            id=record.id,
            status=JobStatus(record.status),
            type=record.type,
            progress=record.progress,
            message=record.message or "",
            result=record.result_json,
            error=record.error,
            payload=record.payload_json,
            catalog=record.catalog,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )
    
    def create_job(
        self,
        initial_message: str = "",
        *,
        job_type: str = "generic",
        payload: Optional[dict[str, Any]] = None,
        catalog: Optional[str] = None,
    ) -> Job:
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
            type=job_type,
            message=initial_message or "Job created, waiting to start...",
            payload=payload,
            catalog=catalog,
        )
        if self.use_database:
            with session_scope() as session:
                session.add(
                    JobRecord(
                        id=job.id,
                        type=job.type,
                        status=job.status.value,
                        progress=job.progress,
                        message=job.message,
                        payload_json=job.payload,
                        catalog=job.catalog,
                        created_at=job.created_at,
                        updated_at=job.updated_at,
                    )
                )
            return job

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
        if self.use_database:
            with session_scope() as session:
                record = session.get(JobRecord, job_id)
                if not record:
                    return None
                if status is not None:
                    record.status = status.value
                if progress is not None:
                    record.progress = min(100, max(0, progress))
                if message is not None:
                    record.message = message
                if result is not None:
                    record.result_json = result
                if error is not None:
                    record.error = error
                record.heartbeat_at = datetime.utcnow()
                record.updated_at = datetime.utcnow()
                session.flush()
                return self._record_to_job(record)

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
        if self.use_database:
            with session_scope() as session:
                record = session.get(JobRecord, job_id)
                if not record:
                    return None
                return self._record_to_job(record)

        return self._jobs.get(job_id)
    
    def list_jobs(self, limit: int = 100) -> list[Job]:
        """
        List recent jobs.
        
        Args:
            limit: Maximum number of jobs to return
            
        Returns:
            List of jobs, most recent first
        """
        if self.use_database:
            with session_scope() as session:
                records = session.execute(
                    select(JobRecord)
                    .order_by(JobRecord.created_at.desc())
                    .limit(limit)
                ).scalars().all()
            return [self._record_to_job(record) for record in records]

        jobs = list(self._jobs.values())
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return jobs[:limit]

    def find_latest_job(
        self,
        *,
        job_type: Optional[str] = None,
        catalog: Optional[str] = None,
        statuses: Optional[list[JobStatus]] = None,
    ) -> Optional[Job]:
        """Return the most recently created job matching filters."""
        allowed_statuses = {status.value for status in statuses} if statuses else None

        if self.use_database:
            with session_scope() as session:
                query = select(JobRecord)
                if job_type:
                    query = query.where(JobRecord.type == job_type)
                if catalog:
                    query = query.where(JobRecord.catalog == catalog)
                if allowed_statuses:
                    query = query.where(JobRecord.status.in_(allowed_statuses))
                query = query.order_by(JobRecord.created_at.desc()).limit(1)
                record = session.execute(query).scalar_one_or_none()
                if not record:
                    return None
                return self._record_to_job(record)

        jobs = list(self._jobs.values())
        if job_type:
            jobs = [job for job in jobs if job.type == job_type]
        if catalog:
            jobs = [job for job in jobs if job.catalog == catalog]
        if allowed_statuses:
            jobs = [job for job in jobs if job.status.value in allowed_statuses]
        if not jobs:
            return None
        jobs.sort(key=lambda job: job.created_at, reverse=True)
        return jobs[0]
    
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

    def _claim_job(self, job_id: str) -> bool:
        if not self.use_database:
            return True

        with session_scope() as session:
            record = session.execute(
                select(JobRecord)
                .where(
                    JobRecord.id == job_id,
                    JobRecord.status == JobStatus.PENDING.value,
                )
                .with_for_update(skip_locked=True)
            ).scalar_one_or_none()
            if not record:
                return False
            record.status = JobStatus.RUNNING.value
            record.owner_replica = settings.replica_id
            record.heartbeat_at = datetime.utcnow()
            record.updated_at = datetime.utcnow()
            return True
    
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
        if not self._claim_job(job_id):
            return
        if not self.use_database:
            self.update_job(job_id, status=JobStatus.RUNNING, message="Task started...")
        else:
            self.update_job(job_id, message="Task started...")
        
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
        if self.use_database:
            cutoff = datetime.utcnow() - timedelta(seconds=max_age_seconds)
            with session_scope() as session:
                return session.execute(
                    delete(JobRecord).where(
                        JobRecord.created_at < cutoff,
                        JobRecord.status.in_(
                            [JobStatus.COMPLETED.value, JobStatus.FAILED.value]
                        ),
                    )
                ).rowcount or 0

        now = datetime.utcnow()
        to_remove = []
        
        for job_id, job in self._jobs.items():
            age = (now - job.created_at).total_seconds()
            if age > max_age_seconds and job.status in [JobStatus.COMPLETED, JobStatus.FAILED]:
                to_remove.append(job_id)
        
        for job_id in to_remove:
            del self._jobs[job_id]
        
        return len(to_remove)

    def delete_job(self, job_id: str) -> bool:
        """Delete a terminal job."""
        if self.use_database:
            with session_scope() as session:
                deleted = session.execute(
                    delete(JobRecord).where(JobRecord.id == job_id)
                ).rowcount or 0
                return deleted > 0

        return self._jobs.pop(job_id, None) is not None

    def sweep_stale_running_jobs(self, max_heartbeat_age_seconds: int = 60) -> int:
        """Mark jobs with stale heartbeats as failed."""
        if not self.use_database:
            return 0

        cutoff = datetime.utcnow() - timedelta(seconds=max_heartbeat_age_seconds)
        updated = 0
        with session_scope() as session:
            records = session.execute(
                select(JobRecord).where(
                    JobRecord.status == JobStatus.RUNNING.value,
                    JobRecord.heartbeat_at < cutoff,
                )
            ).scalars().all()
            for record in records:
                record.status = JobStatus.FAILED.value
                record.error = "Job heartbeat expired"
                record.updated_at = datetime.utcnow()
                updated += 1
        return updated


job_service = JobService()
