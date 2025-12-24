# housekeeper/core.py
"""
Housekeeper - HPC Job Management Library

Main interface for job submission, tracking, and management.
"""

import os
import uuid
import time
from datetime import datetime
from typing import Optional, List, Dict, Any, Union, Tuple
from pathlib import Path

from .config import SchedulerConfig, load_config, parse_config
from .job import Job, JobState
from .database import JobDatabase
from .scheduler import PBSScheduler, SLURMScheduler, BaseScheduler
from .log_checker import check_log, check_job_logs, LogCheckResult


class Housekeeper:
    """
    Main housekeeper interface for HPC job management.
    
    Usage:
        hk = Housekeeper()
        hk.set_config('scheduler_config.yaml')
        
        job = hk.submit(
            command="python train.py",
            name="train_model",
            nodes=1, ppn=8,
            walltime="04:00:00"
        )
        
        hk.wait(job.job_id)
    """
    
    def __init__(self, config: Union[str, Dict, SchedulerConfig, None] = None,
                 jobs_dir: str = "./jobs", scheduler: Optional[str] = None):
        """
        Initialize Housekeeper.
        
        Args:
            config: Config file path, dict, or SchedulerConfig object
            jobs_dir: Directory for job scripts and database
            scheduler: Override scheduler type ('pbs' or 'slurm')
        """
        self.jobs_dir = Path(jobs_dir)
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        
        # Database
        self.db = JobDatabase(str(self.jobs_dir / "housekeeper.db"))
        
        # Config
        self.config: Optional[SchedulerConfig] = None
        self._scheduler: Optional[BaseScheduler] = None
        
        if config:
            self.set_config(config)
        elif scheduler:
            self._init_scheduler(scheduler)
        else:
            # Try to auto-detect
            self._auto_detect_scheduler()
    
    def set_config(self, config: Union[str, Dict, SchedulerConfig]):
        """
        Set scheduler configuration.
        
        Args:
            config: Config file path, dict, or SchedulerConfig object
        """
        if isinstance(config, str):
            self.config = load_config(config)
        elif isinstance(config, dict):
            self.config = parse_config(config)
        elif isinstance(config, SchedulerConfig):
            self.config = config
        else:
            raise ValueError(f"Invalid config type: {type(config)}")
        
        self._init_scheduler(self.config.scheduler)
    
    def _init_scheduler(self, scheduler_type: str):
        """Initialize the scheduler backend"""
        scheduler_type = scheduler_type.lower()
        
        if scheduler_type == 'pbs':
            self._scheduler = PBSScheduler(self.config)
        elif scheduler_type == 'slurm':
            self._scheduler = SLURMScheduler(self.config)
        else:
            raise ValueError(f"Unknown scheduler: {scheduler_type}")
    
    def _auto_detect_scheduler(self):
        """Auto-detect available scheduler"""
        import shutil
        
        if shutil.which('qsub'):
            self._init_scheduler('pbs')
        elif shutil.which('sbatch'):
            self._init_scheduler('slurm')
        else:
            # Default to PBS for script generation
            self._init_scheduler('pbs')
    
    @property
    def scheduler(self) -> BaseScheduler:
        """Get the scheduler backend"""
        if self._scheduler is None:
            raise RuntimeError("Scheduler not initialized. Call set_config() first.")
        return self._scheduler
    
    # =========================================================================
    # Job Submission
    # =========================================================================
    
    def submit(self, command: str, name: str,
               nodes: int = 1, ppn: int = 1,
               walltime: str = "04:00:00",
               mem_gb: Optional[int] = None,
               gpu: bool = False,
               job_subdir: Optional[str] = None,
               working_dir: Optional[str] = None,
               after_ok: Optional[List[str]] = None,
               after_any: Optional[List[str]] = None,
               extra_directives: Optional[List[str]] = None,
               extra_modules: Optional[List[str]] = None,
               max_retries: int = 0) -> Job:
        """
        Submit a job to the scheduler.
        
        Args:
            command: Command to execute
            name: Job name
            nodes: Number of nodes
            ppn: Processors per node
            walltime: Wall time (HH:MM:SS)
            mem_gb: Memory in GB
            gpu: Whether to request GPU
            job_subdir: Subdirectory for job files
            working_dir: Working directory for job
            after_ok: Job IDs that must complete successfully
            after_any: Job IDs that must complete (success or fail)
            extra_directives: Additional scheduler directives
            extra_modules: Additional modules to load
            max_retries: Maximum retry attempts
        
        Returns:
            Job object with job_id set
        """
        # Generate internal ID
        internal_id = str(uuid.uuid4())[:8]
        
        # Determine job directory
        if job_subdir:
            job_dir = self.jobs_dir / job_subdir
        else:
            job_dir = self.jobs_dir / name
        job_dir.mkdir(parents=True, exist_ok=True)
        
        # File paths
        script_path = str(job_dir / f"{name}{self.scheduler.script_extension}")
        output_file = str(job_dir / f"{name}.out")
        error_file = str(job_dir / f"{name}.err")
        log_file = str(job_dir / f"{name}.log")
        
        # Build script
        script_content = self.scheduler.build_script(
            job_name=name,
            command=command,
            nodes=nodes,
            ppn=ppn,
            walltime=walltime,
            mem_gb=mem_gb,
            gpu=gpu,
            output_file=output_file,
            error_file=error_file,
            working_dir=working_dir or str(Path.cwd()),
            after_ok=after_ok,
            after_any=after_any,
            extra_directives=extra_directives,
            extra_modules=extra_modules
        )
        
        # Write script
        with open(script_path, 'w') as f:
            f.write(script_content)
        os.chmod(script_path, 0o755)
        
        # Create job object
        job = Job(
            name=name,
            internal_id=internal_id,
            command=command,
            script_path=script_path,
            nodes=nodes,
            ppn=ppn,
            walltime=walltime,
            mem_gb=mem_gb,
            gpu=gpu,
            output_file=output_file,
            error_file=error_file,
            log_file=log_file,
            working_dir=working_dir or str(Path.cwd()),
            job_subdir=job_subdir,
            after_ok=after_ok or [],
            after_any=after_any or [],
            max_retries=max_retries,
            state=JobState.PENDING
        )
        
        # Submit to scheduler
        job_id = self.scheduler.submit(script_path)
        
        if job_id:
            job.job_id = job_id
            job.state = JobState.SUBMITTED
            job.submit_time = datetime.now()
        else:
            job.state = JobState.FAILED
        
        # Save to database
        self.db.save_job(job)
        
        return job
    
    def submit_script(self, script_path: str, name: Optional[str] = None) -> Job:
        """
        Submit an existing script file.
        
        Args:
            script_path: Path to script file
            name: Job name (defaults to script filename)
        
        Returns:
            Job object
        """
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script not found: {script_path}")
        
        name = name or Path(script_path).stem
        internal_id = str(uuid.uuid4())[:8]
        
        job = Job(
            name=name,
            internal_id=internal_id,
            script_path=script_path,
            state=JobState.PENDING
        )
        
        job_id = self.scheduler.submit(script_path)
        
        if job_id:
            job.job_id = job_id
            job.state = JobState.SUBMITTED
            job.submit_time = datetime.now()
        else:
            job.state = JobState.FAILED
        
        self.db.save_job(job)
        return job
    
    # =========================================================================
    # Job Status and Monitoring
    # =========================================================================
    
    def status(self, job_id: str) -> Job:
        """
        Get current status of a job.
        
        Args:
            job_id: Scheduler job ID
        
        Returns:
            Updated Job object
        """
        # Try to find in database first
        job = self.db.get_job_by_scheduler_id(job_id)
        
        if job is None:
            # Create minimal job object
            job = Job(name="unknown", job_id=job_id)
        
        # Update status from scheduler
        status_str = self.scheduler.get_status(job_id)
        job.state = JobState(status_str) if status_str in [s.value for s in JobState] else JobState.UNKNOWN
        
        # Update timing
        if job.state == JobState.RUNNING and job.start_time is None:
            job.start_time = datetime.now()
        elif job.state in (JobState.COMPLETED, JobState.FAILED) and job.end_time is None:
            job.end_time = datetime.now()
        
        # Save updated status
        if job.internal_id:
            self.db.save_job(job)
        
        return job
    
    def refresh(self, job: Job) -> Job:
        """Refresh job status"""
        if job.job_id:
            return self.status(job.job_id)
        return job
    
    def wait(self, job_ids: Union[str, List[str]], poll_interval: int = 10,
             timeout: Optional[int] = None) -> Dict[str, Job]:
        """
        Wait for jobs to complete.
        
        Args:
            job_ids: Single job ID or list of job IDs
            poll_interval: Seconds between status checks
            timeout: Maximum wait time in seconds
        
        Returns:
            Dict mapping job_id to final Job object
        """
        if isinstance(job_ids, str):
            job_ids = [job_ids]
        
        pending = set(job_ids)
        results = {}
        start_time = time.time()
        
        while pending:
            if timeout and (time.time() - start_time) > timeout:
                break
            
            for job_id in list(pending):
                job = self.status(job_id)
                
                if job.is_done:
                    pending.remove(job_id)
                    results[job_id] = job
            
            if pending:
                time.sleep(poll_interval)
        
        # Add remaining pending jobs
        for job_id in pending:
            results[job_id] = self.status(job_id)
        
        return results
    
    def wait_all(self, poll_interval: int = 10) -> Dict[str, Job]:
        """Wait for all tracked jobs to complete"""
        active_jobs = self.db.get_active_jobs()
        job_ids = [j.job_id for j in active_jobs if j.job_id]
        return self.wait(job_ids, poll_interval)
    
    # =========================================================================
    # Job Control
    # =========================================================================
    
    def cancel(self, job_id: str) -> bool:
        """Cancel a job"""
        success = self.scheduler.cancel(job_id)
        
        if success:
            job = self.db.get_job_by_scheduler_id(job_id)
            if job:
                job.state = JobState.CANCELLED
                job.end_time = datetime.now()
                self.db.save_job(job)
        
        return success
    
    def cancel_all(self) -> int:
        """Cancel all active jobs"""
        active_jobs = self.db.get_active_jobs()
        cancelled = 0
        
        for job in active_jobs:
            if job.job_id and self.cancel(job.job_id):
                cancelled += 1
        
        return cancelled
    
    def retry(self, job_id: str) -> Optional[Job]:
        """
        Retry a failed job.
        
        Args:
            job_id: Scheduler job ID of failed job
        
        Returns:
            New Job object or None if retry not possible
        """
        old_job = self.db.get_job_by_scheduler_id(job_id)
        
        if old_job is None:
            return None
        
        if old_job.attempt >= old_job.max_retries + 1:
            return None
        
        # Submit new job
        new_job = self.submit(
            command=old_job.command,
            name=f"{old_job.name}_retry{old_job.attempt}",
            nodes=old_job.nodes,
            ppn=old_job.ppn,
            walltime=old_job.walltime,
            mem_gb=old_job.mem_gb,
            gpu=old_job.gpu,
            working_dir=old_job.working_dir,
            max_retries=old_job.max_retries
        )
        
        new_job.attempt = old_job.attempt + 1
        self.db.save_job(new_job)
        
        return new_job
    
    # =========================================================================
    # Information and Statistics
    # =========================================================================
    
    def stats(self) -> Dict[str, int]:
        """Get job statistics"""
        return self.db.get_stats()
    
    def list_jobs(self, state: Optional[JobState] = None) -> List[Job]:
        """List jobs, optionally filtered by state"""
        if state:
            return self.db.get_jobs_by_state(state)
        return self.db.get_all_jobs()
    
    def list_active(self) -> List[Job]:
        """List active (non-terminal) jobs"""
        return self.db.get_active_jobs()
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by scheduler job ID"""
        return self.db.get_job_by_scheduler_id(job_id)
    
    def clear_completed(self):
        """Remove completed jobs from database"""
        jobs = self.db.get_jobs_by_state(JobState.COMPLETED)
        for job in jobs:
            self.db.delete_job(job.internal_id)
    
    def clear_all(self):
        """Clear all jobs from database"""
        self.db.clear_all()
    
    # =========================================================================
    # Log Checking
    # =========================================================================
    
    def check_log(self, job_id: str, 
                  whitelist: Optional[List[str]] = None) -> LogCheckResult:
        """
        Check job log files for errors.
        
        Args:
            job_id: Scheduler job ID
            whitelist: List of error patterns to ignore (caller provides)
        
        Returns:
            LogCheckResult with success status and error lines
        """
        job = self.db.get_job_by_scheduler_id(job_id)
        
        if job is None:
            return LogCheckResult(
                success=False,
                log_path="",
                error_lines=[f"Job {job_id} not found in database"]
            )
        
        # Determine job directory
        if job.job_subdir:
            job_dir = str(self.jobs_dir / job.job_subdir)
        else:
            job_dir = str(self.jobs_dir / job.name)
        
        return check_job_logs(job_dir, job.name, whitelist)
    
    def check_log_file(self, log_path: str,
                       whitelist: Optional[List[str]] = None) -> LogCheckResult:
        """
        Check a specific log file for errors.
        
        Args:
            log_path: Path to log file
            whitelist: List of error patterns to ignore
        
        Returns:
            LogCheckResult with success status and error lines
        """
        return check_log(log_path, whitelist)
    
    def wait_and_check(self, job_ids: Union[str, List[str]], 
                       poll_interval: int = 10,
                       timeout: Optional[int] = None,
                       whitelist: Optional[List[str]] = None) -> Dict[str, Tuple[Job, LogCheckResult]]:
        """
        Wait for jobs to complete and check their logs.
        
        Args:
            job_ids: Single job ID or list of job IDs
            poll_interval: Seconds between status checks
            timeout: Maximum wait time in seconds
            whitelist: List of error patterns to ignore
        
        Returns:
            Dict mapping job_id to (Job, LogCheckResult) tuples
        """
        # Wait for jobs
        results = self.wait(job_ids, poll_interval, timeout)
        
        # Check logs for each completed job
        checked_results = {}
        for job_id, job in results.items():
            log_result = self.check_log(job_id, whitelist)
            
            # Update job state if log check found errors
            if not log_result.success and job.state == JobState.COMPLETED:
                job.state = JobState.FAILED
                self.db.save_job(job)
            
            checked_results[job_id] = (job, log_result)
        
        return checked_results

    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def generate_script(self, command: str, name: str,
                        nodes: int = 1, ppn: int = 1,
                        walltime: str = "04:00:00",
                        mem_gb: Optional[int] = None,
                        gpu: bool = False,
                        **kwargs) -> str:
        """
        Generate batch script content without submitting.
        
        Useful for debugging or manual submission.
        """
        return self.scheduler.build_script(
            job_name=name,
            command=command,
            nodes=nodes,
            ppn=ppn,
            walltime=walltime,
            mem_gb=mem_gb,
            gpu=gpu,
            **kwargs
        )
    
    def print_script(self, command: str, name: str, **kwargs):
        """Print generated script to stdout"""
        print(self.generate_script(command, name, **kwargs))


# Convenience function
def housekeeper(config: Union[str, Dict, None] = None,
                jobs_dir: str = "./jobs",
                scheduler: Optional[str] = None) -> Housekeeper:
    """
    Create a Housekeeper instance.
    
    Args:
        config: Config file path or dict
        jobs_dir: Directory for job files
        scheduler: Scheduler type ('pbs' or 'slurm')
    
    Returns:
        Housekeeper instance
    """
    return Housekeeper(config=config, jobs_dir=jobs_dir, scheduler=scheduler)
