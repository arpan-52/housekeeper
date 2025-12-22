# housekeeper/job.py
"""
Job representation
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class JobState(Enum):
    """Job states"""
    PENDING = "pending"
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


@dataclass
class Job:
    """Represents a submitted job"""
    
    # Identifiers
    name: str
    job_id: Optional[str] = None
    internal_id: Optional[str] = None  # housekeeper's internal tracking ID
    
    # Command
    command: str = ""
    script_path: Optional[str] = None
    
    # Resources
    nodes: int = 1
    ppn: int = 1
    walltime: str = "04:00:00"
    mem_gb: Optional[int] = None
    gpu: bool = False
    
    # State
    state: JobState = JobState.PENDING
    exit_code: Optional[int] = None
    
    # Files
    output_file: Optional[str] = None
    error_file: Optional[str] = None
    log_file: Optional[str] = None
    working_dir: Optional[str] = None
    job_subdir: Optional[str] = None
    
    # Dependencies
    after_ok: List[str] = field(default_factory=list)
    after_any: List[str] = field(default_factory=list)
    
    # Timing
    submit_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    # Retry tracking
    attempt: int = 1
    max_retries: int = 0
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage"""
        return {
            'name': self.name,
            'job_id': self.job_id,
            'internal_id': self.internal_id,
            'command': self.command,
            'script_path': self.script_path,
            'nodes': self.nodes,
            'ppn': self.ppn,
            'walltime': self.walltime,
            'mem_gb': self.mem_gb,
            'gpu': self.gpu,
            'state': self.state.value,
            'exit_code': self.exit_code,
            'output_file': self.output_file,
            'error_file': self.error_file,
            'log_file': self.log_file,
            'working_dir': self.working_dir,
            'job_subdir': self.job_subdir,
            'after_ok': ','.join(self.after_ok) if self.after_ok else None,
            'after_any': ','.join(self.after_any) if self.after_any else None,
            'submit_time': self.submit_time.isoformat() if self.submit_time else None,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'attempt': self.attempt,
            'max_retries': self.max_retries
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Job':
        """Create Job from dictionary"""
        job = cls(
            name=data.get('name', ''),
            job_id=data.get('job_id'),
            internal_id=data.get('internal_id'),
            command=data.get('command', ''),
            script_path=data.get('script_path'),
            nodes=data.get('nodes', 1),
            ppn=data.get('ppn', 1),
            walltime=data.get('walltime', '04:00:00'),
            mem_gb=data.get('mem_gb'),
            gpu=data.get('gpu', False),
            exit_code=data.get('exit_code'),
            output_file=data.get('output_file'),
            error_file=data.get('error_file'),
            log_file=data.get('log_file'),
            working_dir=data.get('working_dir'),
            job_subdir=data.get('job_subdir'),
            attempt=data.get('attempt', 1),
            max_retries=data.get('max_retries', 0)
        )
        
        # State
        state_str = data.get('state', 'pending')
        job.state = JobState(state_str) if isinstance(state_str, str) else state_str
        
        # Dependencies
        if data.get('after_ok'):
            job.after_ok = data['after_ok'].split(',') if isinstance(data['after_ok'], str) else data['after_ok']
        if data.get('after_any'):
            job.after_any = data['after_any'].split(',') if isinstance(data['after_any'], str) else data['after_any']
        
        # Times
        if data.get('submit_time'):
            job.submit_time = datetime.fromisoformat(data['submit_time']) if isinstance(data['submit_time'], str) else data['submit_time']
        if data.get('start_time'):
            job.start_time = datetime.fromisoformat(data['start_time']) if isinstance(data['start_time'], str) else data['start_time']
        if data.get('end_time'):
            job.end_time = datetime.fromisoformat(data['end_time']) if isinstance(data['end_time'], str) else data['end_time']
        
        return job
    
    @property
    def is_done(self) -> bool:
        """Check if job is in a terminal state"""
        return self.state in (JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED)
    
    @property
    def is_running(self) -> bool:
        """Check if job is running"""
        return self.state == JobState.RUNNING
    
    @property
    def is_pending(self) -> bool:
        """Check if job is pending"""
        return self.state in (JobState.PENDING, JobState.SUBMITTED)
    
    @property
    def duration(self) -> Optional[float]:
        """Get job duration in seconds"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
