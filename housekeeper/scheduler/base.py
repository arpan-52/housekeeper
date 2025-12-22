# housekeeper/scheduler/base.py
"""
Base scheduler interface
"""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from ..config import SchedulerConfig


class BaseScheduler(ABC):
    """Abstract base class for job schedulers"""
    
    def __init__(self, config: Optional[SchedulerConfig] = None):
        self.config = config
    
    @abstractmethod
    def build_script(self, job_name: str, command: str, 
                     nodes: int = 1, ppn: int = 1,
                     walltime: str = "04:00:00",
                     mem_gb: Optional[int] = None,
                     gpu: bool = False,
                     output_file: Optional[str] = None,
                     error_file: Optional[str] = None,
                     working_dir: Optional[str] = None,
                     after_ok: Optional[List[str]] = None,
                     after_any: Optional[List[str]] = None,
                     extra_directives: Optional[List[str]] = None,
                     extra_modules: Optional[List[str]] = None) -> str:
        """Build batch script content"""
        pass
    
    @abstractmethod
    def submit(self, script_path: str) -> Optional[str]:
        """Submit job and return job ID"""
        pass
    
    @abstractmethod
    def cancel(self, job_id: str) -> bool:
        """Cancel a job"""
        pass
    
    @abstractmethod
    def get_status(self, job_id: str) -> str:
        """Get job status: 'pending', 'running', 'completed', 'failed', 'unknown'"""
        pass
    
    @abstractmethod
    def get_job_info(self, job_id: str) -> Dict[str, Any]:
        """Get detailed job information"""
        pass
    
    @property
    @abstractmethod
    def script_extension(self) -> str:
        """File extension for batch scripts"""
        pass
    
    def get_queue(self, gpu: bool = False) -> Optional[str]:
        """Get appropriate queue based on job type"""
        if self.config is None:
            return None
        if gpu and self.config.queues.gpu:
            return self.config.queues.gpu
        return self.config.queues.default
    
    def get_modules(self, gpu: bool = False) -> List[str]:
        """Get modules to load"""
        if self.config is None:
            return []
        modules = list(self.config.modules)
        if gpu and self.config.gpu.modules:
            modules.extend(self.config.gpu.modules)
        return modules
    
    def get_directives(self) -> List[str]:
        """Get custom directives"""
        if self.config is None:
            return []
        return list(self.config.directives)
