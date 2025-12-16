"""
PBS scheduler backend
"""

import subprocess
import shutil
import re
from typing import Dict, List

from .base import base_scheduler
from ..job import job_status


class pbs_scheduler(base_scheduler):
    """PBS/Torque scheduler implementation"""
    
    def check_available(self) -> bool:
        return shutil.which("qsub") is not None
    
    def generate_script(self, command: str, job_name: str, resources: Dict,
                       stdout_path: str, stderr_path: str,
                       dependencies: Dict[str, List[str]] = None,
                       env: Dict[str, str] = None) -> str:
        """Generate PBS batch script"""
        lines = [
            "#!/bin/bash",
            f"#PBS -N {job_name}",
            f"#PBS -o {stdout_path}",
            f"#PBS -e {stderr_path}",
            f"#PBS -l select={resources.get('nodes', 1)}:ncpus={resources.get('cpus', 1)}:mem={resources.get('memory', '4gb')}",
            f"#PBS -l walltime={resources.get('walltime', '01:00:00')}",
        ]
        
        if resources.get('gpus', 0) > 0:
            # Modify the select line to include GPUs
            lines[4] = f"#PBS -l select={resources.get('nodes', 1)}:ncpus={resources.get('cpus', 1)}:mem={resources.get('memory', '4gb')}:ngpus={resources['gpus']}"
        
        if resources.get('queue'):
            lines.append(f"#PBS -q {resources['queue']}")
        
        if resources.get('account'):
            lines.append(f"#PBS -A {resources['account']}")
        
        # Handle dependencies
        if dependencies:
            dep_strings = []
            if dependencies.get('after_ok'):
                for dep_id in dependencies['after_ok']:
                    dep_strings.append(f"afterok:{dep_id}")
            if dependencies.get('after_fail'):
                for dep_id in dependencies['after_fail']:
                    dep_strings.append(f"afternotok:{dep_id}")
            if dependencies.get('after_any'):
                for dep_id in dependencies['after_any']:
                    dep_strings.append(f"afterany:{dep_id}")
            
            if dep_strings:
                lines.append(f"#PBS -W depend={':'.join(dep_strings)}")
        
        lines.append("")
        
        # Add environment variables
        if env:
            for key, value in env.items():
                lines.append(f"export {key}={value}")
            lines.append("")
        
        # Change to work directory
        lines.append("cd $PBS_O_WORKDIR")
        lines.append("")
        
        # User command
        lines.append(command)
        
        return "\n".join(lines)
    
    def submit(self, script_path: str) -> str:
        """Submit job to PBS"""
        try:
            result = subprocess.run(
                ["qsub", script_path],
                capture_output=True,
                text=True,
                check=True
            )
            # Parse job ID (format: "12345.hostname")
            match = re.match(r'(\d+)', result.stdout.strip())
            if match:
                return match.group(1)
            raise RuntimeError(f"could not parse job id from: {result.stdout}")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"qsub failed: {e.stderr}")
    
    def status(self, scheduler_id: str) -> job_status:
        """Get job status from PBS"""
        try:
            result = subprocess.run(
                ["qstat", scheduler_id],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode != 0:
                # Job not in queue, assume completed
                return job_status.completed
            
            output = result.stdout
            
            # Parse status from qstat output
            # Typical format has status in job_state column
            if " R " in output:  # Running
                return job_status.running
            elif " Q " in output:  # Queued
                return job_status.queued
            elif " H " in output:  # Held
                return job_status.queued
            elif " E " in output:  # Exiting
                return job_status.running
            elif " C " in output:  # Completed
                return job_status.completed
            
            return job_status.unknown
            
        except Exception:
            return job_status.unknown
    
    def cancel(self, scheduler_id: str):
        """Cancel a PBS job"""
        try:
            subprocess.run(["qdel", scheduler_id], check=False, timeout=5)
        except Exception:
            pass
