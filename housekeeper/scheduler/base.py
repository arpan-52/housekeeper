"""
Abstract base class for scheduler backends
"""

from abc import ABC, abstractmethod
from typing import Dict, List
from ..job import job_status


class base_scheduler(ABC):
    """Abstract base for scheduler implementations"""
    
    @abstractmethod
    def generate_script(self, command: str, job_name: str, resources: Dict,
                       stdout_path: str, stderr_path: str,
                       dependencies: Dict[str, List[str]] = None,
                       env: Dict[str, str] = None) -> str:
        """Generate batch script for submission"""
        pass
    
    @abstractmethod
    def submit(self, script_path: str) -> str:
        """Submit script and return scheduler job ID"""
        pass
    
    @abstractmethod
    def status(self, scheduler_id: str) -> job_status:
        """Get job status from scheduler"""
        pass
    
    @abstractmethod
    def cancel(self, scheduler_id: str):
        """Cancel a job"""
        pass
    
    @abstractmethod
    def check_available(self) -> bool:
        """Check if this scheduler is available"""
        pass
