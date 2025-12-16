"""
Job model and related enums
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime


class job_status(str, Enum):
    """Job status states"""
    pending = "pending"         # Created, not yet submitted
    queued = "queued"           # Submitted to scheduler, waiting
    running = "running"         # Currently executing
    completed = "completed"     # Finished successfully
    failed = "failed"           # Failed (scheduler or code error)
    cancelled = "cancelled"     # Cancelled by user
    timeout = "timeout"         # Exceeded walltime
    unknown = "unknown"         # Cannot determine status


class failure_type(str, Enum):
    """Types of job failures"""
    scheduler = "scheduler"     # Scheduler reported failure
    exit_code = "exit_code"     # Non-zero exit code
    log_error = "log_error"     # Errors found in log
    missing_file = "missing_file"  # Expected output file missing
    timeout = "timeout"         # Job exceeded walltime
    memory = "memory"           # Out of memory
    dependency = "dependency"   # Dependency failed
    unknown = "unknown"


@dataclass
class job_resources:
    """Resource requirements for a job"""
    nodes: int = 1
    cpus: int = 1
    gpus: int = 0
    memory: str = "4GB"
    walltime: str = "01:00:00"
    queue: Optional[str] = None
    account: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "job_resources":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class job:
    """Represents a single HPC job"""
    id: str
    name: str
    command: str
    workdir: str
    resources: job_resources = field(default_factory=job_resources)
    
    # Scheduler info
    scheduler_id: Optional[str] = None
    script_path: Optional[str] = None
    
    # Status
    status: job_status = job_status.pending
    exit_code: Optional[int] = None
    
    # Output paths
    stdout_path: Optional[str] = None
    stderr_path: Optional[str] = None
    
    # File tracking
    expected_files: List[str] = field(default_factory=list)
    
    # Dependencies
    after_ok: List[str] = field(default_factory=list)
    after_fail: List[str] = field(default_factory=list)
    after_any: List[str] = field(default_factory=list)
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)
    submitted_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Failure info
    failure_type: Optional[failure_type] = None
    failure_reason: Optional[str] = None
    error_lines: List[str] = field(default_factory=list)
    
    # Retry
    retry_count: int = 0
    max_retries: int = 0
    parent_job_id: Optional[str] = None  # If this is a retry
    
    # Environment
    env: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage/display"""
        return {
            "id": self.id,
            "name": self.name,
            "command": self.command,
            "workdir": self.workdir,
            "resources": self.resources.to_dict(),
            "scheduler_id": self.scheduler_id,
            "script_path": self.script_path,
            "status": self.status.value,
            "exit_code": self.exit_code,
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
            "expected_files": self.expected_files,
            "after_ok": self.after_ok,
            "after_fail": self.after_fail,
            "after_any": self.after_any,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "failure_type": self.failure_type.value if self.failure_type else None,
            "failure_reason": self.failure_reason,
            "error_lines": self.error_lines,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "parent_job_id": self.parent_job_id,
            "env": self.env,
        }
