"""
SLURM scheduler backend
"""

import subprocess
import shutil
import re
from typing import Dict, List

from .base import base_scheduler
from ..job import job_status


class slurm_scheduler(base_scheduler):
    """SLURM scheduler implementation"""
    
    def check_available(self) -> bool:
        return shutil.which("sbatch") is not None
    
    def generate_script(self, command: str, job_name: str, resources: Dict,
                       stdout_path: str, stderr_path: str,
                       dependencies: Dict[str, List[str]] = None,
                       env: Dict[str, str] = None) -> str:
        """Generate SLURM batch script"""
        lines = [
            "#!/bin/bash",
            f"#SBATCH --job-name={job_name}",
            f"#SBATCH --output={stdout_path}",
            f"#SBATCH --error={stderr_path}",
            f"#SBATCH --nodes={resources.get('nodes', 1)}",
            f"#SBATCH --ntasks-per-node=1",
            f"#SBATCH --cpus-per-task={resources.get('cpus', 1)}",
            f"#SBATCH --mem={resources.get('memory', '4GB')}",
            f"#SBATCH --time={resources.get('walltime', '01:00:00')}",
        ]
        
        if resources.get('gpus', 0) > 0:
            lines.append(f"#SBATCH --gres=gpu:{resources['gpus']}")
        
        if resources.get('queue'):
            lines.append(f"#SBATCH --partition={resources['queue']}")
        
        if resources.get('account'):
            lines.append(f"#SBATCH --account={resources['account']}")
        
        # Handle dependencies
        if dependencies:
            dep_strings = []
            if dependencies.get('after_ok'):
                dep_strings.append("afterok:" + ":".join(dependencies['after_ok']))
            if dependencies.get('after_fail'):
                dep_strings.append("afternotok:" + ":".join(dependencies['after_fail']))
            if dependencies.get('after_any'):
                dep_strings.append("afterany:" + ":".join(dependencies['after_any']))
            
            if dep_strings:
                lines.append(f"#SBATCH --dependency={','.join(dep_strings)}")
        
        lines.append("")
        
        # Add environment variables
        if env:
            for key, value in env.items():
                lines.append(f"export {key}={value}")
            lines.append("")
        
        # Change to submit directory
        lines.append("cd $SLURM_SUBMIT_DIR")
        lines.append("")
        
        # User command
        lines.append(command)
        
        return "\n".join(lines)
    
    def submit(self, script_path: str) -> str:
        """Submit job to SLURM"""
        try:
            result = subprocess.run(
                ["sbatch", script_path],
                capture_output=True,
                text=True,
                check=True
            )
            # Parse "Submitted batch job 12345"
            match = re.search(r'(\d+)$', result.stdout.strip())
            if match:
                return match.group(1)
            raise RuntimeError(f"could not parse job id from: {result.stdout}")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"sbatch failed: {e.stderr}")
    
    def status(self, scheduler_id: str) -> job_status:
        """Get job status from SLURM"""
        try:
            # Try squeue first (running/queued jobs)
            result = subprocess.run(
                ["squeue", "-j", scheduler_id, "--noheader", "--format=%T"],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0 and result.stdout.strip():
                state = result.stdout.strip().upper()
                if state == "PENDING":
                    return job_status.queued
                elif state == "RUNNING":
                    return job_status.running
                elif state == "COMPLETING":
                    return job_status.running
                else:
                    return job_status.unknown
            
            # Job not in queue, check history with sacct
            return self._check_history(scheduler_id)
            
        except Exception:
            return job_status.unknown
    
    def _check_history(self, scheduler_id: str) -> job_status:
        """Check completed job history"""
        try:
            result = subprocess.run(
                ["sacct", "-j", scheduler_id, "-n", "-X", "-o", "State"],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0 and result.stdout.strip():
                state = result.stdout.strip().upper()
                if "COMPLETED" in state:
                    return job_status.completed
                elif "TIMEOUT" in state:
                    return job_status.timeout
                elif "CANCELLED" in state:
                    return job_status.cancelled
                elif "FAILED" in state or "NODE_FAIL" in state:
                    return job_status.failed
                else:
                    return job_status.failed
            
            return job_status.unknown
            
        except Exception:
            return job_status.unknown
    
    def cancel(self, scheduler_id: str):
        """Cancel a SLURM job"""
        try:
            subprocess.run(["scancel", scheduler_id], check=False, timeout=5)
        except Exception:
            pass
