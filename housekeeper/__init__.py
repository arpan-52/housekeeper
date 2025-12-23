# housekeeper/__init__.py
"""
Housekeeper - HPC Job Management Library

A simple, powerful library for managing HPC jobs across PBS and SLURM clusters.

Usage:
    from housekeeper import Housekeeper
    
    hk = Housekeeper()
    hk.set_config('scheduler_config.yaml')
    
    job = hk.submit(
        command="python train.py",
        name="train_model",
        nodes=1, ppn=8,
        walltime="04:00:00",
        gpu=True
    )
    
    hk.wait(job.job_id)
    
    # Check logs for errors (pass your own whitelist)
    result = hk.check_log(job.job_id, whitelist=["pattern to ignore"])
    if not result.success:
        print(f"Errors found: {result.error_lines}")
"""

from .core import Housekeeper, housekeeper
from .job import Job, JobState
from .config import SchedulerConfig, load_config, save_default_config
from .database import JobDatabase
from .log_checker import check_log, check_job_logs, LogCheckResult, is_whitelisted

__version__ = "2.0.0"
__all__ = [
    'Housekeeper',
    'housekeeper', 
    'Job',
    'JobState',
    'SchedulerConfig',
    'load_config',
    'save_default_config',
    'JobDatabase',
    # Log checking
    'check_log',
    'check_job_logs',
    'LogCheckResult',
    'is_whitelisted'
]
