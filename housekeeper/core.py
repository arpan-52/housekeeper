"""
Main housekeeper class - user-facing API
"""

import time
import json
import shutil
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime

from .job import job, job_status, job_resources, failure_type
from .database import database
from .scheduler import slurm_scheduler, pbs_scheduler
from .tracking import log_parser, failure_detector
from .utils import generate_job_id, detect_scheduler


class housekeeper:
    """
    Main housekeeper class for HPC job management
    
    Example:
        >>> hk = housekeeper(workdir="./pipeline")
        >>> job_id = hk.submit("python train.py", name="training", gpus=2)
        >>> hk.monitor([job_id])
    """
    
    def __init__(self,
                 workdir: str = "./housekeeper_jobs",
                 scheduler: Optional[str] = None,
                 db_path: Optional[str] = None,
                 error_whitelist: Optional[List[str]] = None,
                 whitelist_threshold: int = 3):
        """
        Initialize housekeeper
        
        Args:
            workdir: Working directory for job files
            scheduler: "slurm", "pbs", or None for auto-detect
            db_path: Path to SQLite database (default: workdir/housekeeper.db)
            error_whitelist: List of error patterns to ignore in logs
            whitelist_threshold: Word match threshold for whitelist
        """
        # Setup directories
        self.workdir = Path(workdir).absolute()
        self.workdir.mkdir(parents=True, exist_ok=True)
        
        # Initialize database
        if db_path is None:
            db_path = self.workdir / "housekeeper.db"
        self.db = database(str(db_path))
        
        # Initialize scheduler backend
        if scheduler:
            if scheduler.lower() == "slurm":
                self.backend = slurm_scheduler()
            elif scheduler.lower() == "pbs":
                self.backend = pbs_scheduler()
            else:
                raise ValueError(f"unknown scheduler: {scheduler}")
        else:
            # Auto-detect
            detected = detect_scheduler()
            if detected == "slurm":
                self.backend = slurm_scheduler()
            elif detected == "pbs":
                self.backend = pbs_scheduler()
            else:
                raise RuntimeError("no scheduler found (need sbatch or qsub)")
        
        if not self.backend.check_available():
            raise RuntimeError(f"scheduler not available")
        
        # Initialize tracking
        self.log_parser = log_parser(
            error_whitelist=error_whitelist,
            whitelist_threshold=whitelist_threshold
        )
        self.failure_detector = failure_detector(self.log_parser)
    
    def submit(self,
               command: str,
               name: Optional[str] = None,
               nodes: int = 1,
               cpus: int = 1,
               gpus: int = 0,
               memory: str = "4GB",
               walltime: str = "01:00:00",
               queue: Optional[str] = None,
               account: Optional[str] = None,
               expected_files: Optional[List[str]] = None,
               after_ok: Optional[List[str]] = None,
               after_fail: Optional[List[str]] = None,
               after_any: Optional[List[str]] = None,
               max_retries: int = 0,
               env: Optional[Dict[str, str]] = None) -> str:
        """
        Submit a job
        
        Args:
            command: Command to execute
            name: Job name (auto-generated if not provided)
            nodes: Number of nodes
            cpus: CPUs per node
            gpus: GPUs per node
            memory: Memory (e.g., "4GB", "32GB")
            walltime: Wall time (HH:MM:SS)
            queue: Queue/partition name
            account: Account/project name
            expected_files: List of files that should exist after completion
            after_ok: Run after these jobs complete successfully
            after_fail: Run after these jobs fail
            after_any: Run after these jobs finish (any status)
            max_retries: Number of times to retry on failure
            env: Environment variables dict
            
        Returns:
            Job ID
        """
        # Generate job ID
        job_id = generate_job_id()
        
        # Set defaults
        if name is None:
            name = f"job_{job_id}"
        if expected_files is None:
            expected_files = []
        if after_ok is None:
            after_ok = []
        if after_fail is None:
            after_fail = []
        if after_any is None:
            after_any = []
        if env is None:
            env = {}
        
        # Create job directory
        job_dir = self.workdir / job_id
        job_dir.mkdir(exist_ok=True)
        
        # Create resources
        resources = job_resources(
            nodes=nodes,
            cpus=cpus,
            gpus=gpus,
            memory=memory,
            walltime=walltime,
            queue=queue,
            account=account
        )
        
        # Create job object
        j = job(
            id=job_id,
            name=name,
            command=command,
            workdir=str(job_dir),
            resources=resources,
            expected_files=expected_files,
            after_ok=after_ok,
            after_fail=after_fail,
            after_any=after_any,
            max_retries=max_retries,
            env=env
        )
        
        # Store in database
        self.db.add_job(j)
        
        # Check if we can submit immediately or need to wait for dependencies
        if self._dependencies_ready(j):
            self._submit_job(j)
        else:
            print(f"job {job_id} waiting for dependencies")
        
        return job_id
    
    def _dependencies_ready(self, j: job) -> bool:
        """Check if job dependencies are satisfied"""
        # Check after_ok dependencies
        for dep_id in j.after_ok:
            dep_job = self.db.get_job(dep_id)
            if not dep_job or dep_job.status != job_status.completed:
                return False
        
        # Check after_fail dependencies
        for dep_id in j.after_fail:
            dep_job = self.db.get_job(dep_id)
            if not dep_job or dep_job.status != job_status.failed:
                return False
        
        # Check after_any dependencies
        for dep_id in j.after_any:
            dep_job = self.db.get_job(dep_id)
            if not dep_job or dep_job.status not in [job_status.completed, job_status.failed, 
                                                       job_status.cancelled, job_status.timeout]:
                return False
        
        return True
    
    def _submit_job(self, j: job):
        """Submit job to scheduler"""
        # Setup paths
        script_path = Path(j.workdir) / f"{j.name}.sh"
        stdout_path = Path(j.workdir) / "stdout.log"
        stderr_path = Path(j.workdir) / "stderr.log"
        
        # Build dependency dict for scheduler
        dependencies = {}
        if j.after_ok:
            # Get scheduler IDs for dependencies
            scheduler_ids = []
            for dep_id in j.after_ok:
                dep_job = self.db.get_job(dep_id)
                if dep_job and dep_job.scheduler_id:
                    scheduler_ids.append(dep_job.scheduler_id)
            if scheduler_ids:
                dependencies['after_ok'] = scheduler_ids
        
        if j.after_fail:
            scheduler_ids = []
            for dep_id in j.after_fail:
                dep_job = self.db.get_job(dep_id)
                if dep_job and dep_job.scheduler_id:
                    scheduler_ids.append(dep_job.scheduler_id)
            if scheduler_ids:
                dependencies['after_fail'] = scheduler_ids
        
        if j.after_any:
            scheduler_ids = []
            for dep_id in j.after_any:
                dep_job = self.db.get_job(dep_id)
                if dep_job and dep_job.scheduler_id:
                    scheduler_ids.append(dep_job.scheduler_id)
            if scheduler_ids:
                dependencies['after_any'] = scheduler_ids
        
        # Generate script
        script_content = self.backend.generate_script(
            command=j.command,
            job_name=j.name,
            resources=j.resources.to_dict(),
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            dependencies=dependencies if dependencies else None,
            env=j.env if j.env else None
        )
        
        # Write script
        script_path.write_text(script_content)
        script_path.chmod(0o755)
        
        # Submit to scheduler
        try:
            scheduler_id = self.backend.submit(str(script_path))
            
            # Update job
            self.db.update_job(
                j.id,
                scheduler_id=scheduler_id,
                script_path=str(script_path),
                stdout_path=str(stdout_path),
                stderr_path=str(stderr_path),
                status=job_status.queued,
                submitted_at=datetime.now()
            )
            
            print(f"submitted job {j.id} (scheduler id: {scheduler_id})")
            
        except Exception as e:
            print(f"failed to submit job {j.id}: {e}")
            self.db.update_job(j.id, status=job_status.failed, 
                             failure_reason=str(e), failure_type=failure_type.scheduler)
    
    def track(self, job_id: str) -> job_status:
        """
        Check status of a single job
        
        Args:
            job_id: Job ID
            
        Returns:
            Current job status
        """
        j = self.db.get_job(job_id)
        if not j:
            return job_status.unknown
        
        # If not yet submitted, check if dependencies are ready
        if j.status == job_status.pending:
            if self._dependencies_ready(j):
                self._submit_job(j)
                j = self.db.get_job(job_id)  # Reload
            return j.status
        
        # If already terminal state, return it
        if j.status in [job_status.completed, job_status.failed, 
                       job_status.cancelled, job_status.timeout]:
            return j.status
        
        # Get status from scheduler
        if j.scheduler_id:
            sched_status = self.backend.status(j.scheduler_id)
            
            # Update database
            self.db.update_job(job_id, status=sched_status)
            
            # If job finished, check for failures
            if sched_status in [job_status.completed, job_status.failed, 
                               job_status.cancelled, job_status.timeout]:
                self._check_job_completion(job_id)
                
                # Check if dependents can now run
                self._check_dependents(job_id)
            
            return sched_status
        
        return j.status
    
    def _check_job_completion(self, job_id: str):
        """Check completed job for failures and handle retries"""
        j = self.db.get_job(job_id)
        if not j:
            return
        
        # Try to extract exit code from logs if not set
        if j.exit_code is None and j.stderr_path:
            exit_code = self.failure_detector.extract_exit_code(j.stderr_path)
            if exit_code is not None:
                self.db.update_job(job_id, exit_code=exit_code)
                j.exit_code = exit_code
        
        # Run failure detection
        failed, ftype, reason, error_lines = self.failure_detector.detect(j)
        
        if failed:
            # Update failure info
            self.db.update_job(
                job_id,
                status=job_status.failed,
                failure_type=ftype,
                failure_reason=reason,
                error_lines=error_lines,
                completed_at=datetime.now()
            )
            
            # Check if should retry
            if j.retry_count < j.max_retries:
                self._retry_job(job_id)
        else:
            # Mark as completed
            self.db.update_job(job_id, completed_at=datetime.now())
    
    def _retry_job(self, job_id: str):
        """Retry a failed job"""
        j = self.db.get_job(job_id)
        if not j:
            return
        
        print(f"retrying job {job_id} (attempt {j.retry_count + 1}/{j.max_retries})")
        
        # Increment retry count
        self.db.update_job(job_id, retry_count=j.retry_count + 1)
        
        # Resubmit
        self._submit_job(j)
    
    def _check_dependents(self, job_id: str):
        """Check if any dependent jobs can now run"""
        dependents = self.db.get_dependents(job_id)
        
        for dep_id in dependents:
            dep_job = self.db.get_job(dep_id)
            if dep_job and dep_job.status == job_status.pending:
                if self._dependencies_ready(dep_job):
                    self._submit_job(dep_job)
    
    def monitor(self, job_ids: List[str], poll_interval: int = 30) -> List[Dict]:
        """
        Monitor jobs until all complete
        
        Args:
            job_ids: List of job IDs to monitor
            poll_interval: Seconds between status checks
            
        Returns:
            List of job result dicts
        """
        print(f"monitoring {len(job_ids)} jobs...")
        
        active = set(job_ids)
        results = []
        
        while active:
            time.sleep(poll_interval)
            
            for job_id in list(active):
                status = self.track(job_id)
                
                if status in [job_status.completed, job_status.failed,
                             job_status.cancelled, job_status.timeout]:
                    active.remove(job_id)
                    
                    j = self.db.get_job(job_id)
                    if j:
                        results.append(j.to_dict())
                        
                        if status == job_status.failed:
                            print(f"job {job_id} failed: {j.failure_reason}")
                        else:
                            print(f"job {job_id} {status.value}")
            
            if active:
                print(f"  {len(active)} jobs still active...")
        
        print("all jobs completed")
        return results
    
    def cancel(self, job_id: str):
        """Cancel a job"""
        j = self.db.get_job(job_id)
        if j and j.scheduler_id:
            self.backend.cancel(j.scheduler_id)
            self.db.update_job(job_id, status=job_status.cancelled)
            print(f"cancelled job {job_id}")
    
    def retry(self, job_id: str) -> str:
        """
        Manually retry a failed job
        
        Args:
            job_id: Job ID to retry
            
        Returns:
            New job ID for the retry
        """
        j = self.db.get_job(job_id)
        if not j:
            raise ValueError(f"job {job_id} not found")
        
        # Create new job with same parameters
        new_job_id = self.submit(
            command=j.command,
            name=f"{j.name}_retry",
            nodes=j.resources.nodes,
            cpus=j.resources.cpus,
            gpus=j.resources.gpus,
            memory=j.resources.memory,
            walltime=j.resources.walltime,
            queue=j.resources.queue,
            account=j.resources.account,
            expected_files=j.expected_files,
            env=j.env
        )
        
        # Link to parent
        self.db.update_job(new_job_id, parent_job_id=job_id)
        
        return new_job_id
    
    def list_jobs(self, status: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """
        List jobs
        
        Args:
            status: Filter by status (e.g., "failed", "completed")
            limit: Maximum number to return
            
        Returns:
            List of job dicts
        """
        jobs = self.db.list_jobs(status=status, limit=limit)
        return [j.to_dict() for j in jobs]
    
    def job_info(self, job_id: str) -> Optional[Dict]:
        """Get detailed job information"""
        j = self.db.get_job(job_id)
        return j.to_dict() if j else None
    
    def failure_info(self, job_id: str) -> Optional[Dict]:
        """Get detailed failure information for a job"""
        j = self.db.get_job(job_id)
        if not j:
            return None
        
        return {
            "job_id": j.id,
            "name": j.name,
            "status": j.status.value,
            "exit_code": j.exit_code,
            "failure_type": j.failure_type.value if j.failure_type else None,
            "failure_reason": j.failure_reason,
            "error_lines": j.error_lines,
            "stderr_path": j.stderr_path,
            "stdout_path": j.stdout_path,
            "retry_count": j.retry_count,
            "max_retries": j.max_retries,
        }
    
    def cleanup(self, job_id: str):
        """Remove job files and database entry"""
        j = self.db.get_job(job_id)
        if j:
            # Remove job directory
            job_dir = Path(j.workdir)
            if job_dir.exists():
                shutil.rmtree(job_dir)
            
            # Remove from database
            self.db.delete_job(job_id)
            print(f"cleaned up job {job_id}")
    
    def export_state(self, output_path: str):
        """Export all job state to JSON file"""
        jobs = self.db.list_jobs()
        data = {
            "exported_at": datetime.now().isoformat(),
            "total_jobs": len(jobs),
            "jobs": [j.to_dict() for j in jobs]
        }
        
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"exported state to {output_path}")
