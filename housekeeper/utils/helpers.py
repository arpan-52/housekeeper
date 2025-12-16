"""
Helper utility functions
"""

import uuid
import shutil
from typing import Optional


def generate_job_id() -> str:
    """Generate a unique job ID"""
    return str(uuid.uuid4())[:8]


def detect_scheduler() -> Optional[str]:
    """
    Auto-detect available scheduler
    
    Returns:
        "slurm", "pbs", or None
    """
    if shutil.which("sbatch"):
        return "slurm"
    elif shutil.which("qsub"):
        return "pbs"
    return None


def format_duration(seconds: float) -> str:
    """
    Format duration in human-readable form
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string (e.g., "1.5h", "45.2m", "30.0s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    elif seconds < 86400:
        return f"{seconds/3600:.1f}h"
    else:
        return f"{seconds/86400:.1f}d"
