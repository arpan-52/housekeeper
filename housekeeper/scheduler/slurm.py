# housekeeper/scheduler/slurm.py
"""
SLURM scheduler implementation
"""

import subprocess
import re
from typing import Optional, List, Dict, Any
from .base import BaseScheduler
from ..config import SchedulerConfig


class SLURMScheduler(BaseScheduler):
    """SLURM scheduler"""
    
    def __init__(self, config: Optional[SchedulerConfig] = None):
        super().__init__(config)
    
    @property
    def script_extension(self) -> str:
        return '.sbatch'
    
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
        """Build SLURM batch script"""
        lines = ["#!/bin/bash"]
        
        # Job name
        lines.append(f"#SBATCH --job-name={job_name}")
        
        # Partition (queue)
        partition = self._get_partition(gpu)
        if partition:
            lines.append(f"#SBATCH --partition={partition}")
        
        # Account
        if self.config and self.config.slurm_account:
            lines.append(f"#SBATCH --account={self.config.slurm_account}")
        
        # Resources
        lines.append(f"#SBATCH --nodes={nodes}")
        lines.append(f"#SBATCH --ntasks-per-node={ppn}")
        
        if mem_gb:
            lines.append(f"#SBATCH --mem={mem_gb}G")
        
        # Walltime
        lines.append(f"#SBATCH --time={walltime}")
        
        # GPU
        if gpu and self.config and self.config.gpu.enabled:
            gres = self.config.gpu.gres or "gpu:1"
            lines.append(f"#SBATCH --gres={gres}")
        
        # Output files
        if output_file:
            lines.append(f"#SBATCH --output={output_file}")
        if error_file:
            lines.append(f"#SBATCH --error={error_file}")
        
        # Dependencies
        if after_ok:
            dep_str = ":".join(after_ok)
            lines.append(f"#SBATCH --dependency=afterok:{dep_str}")
        if after_any:
            dep_str = ":".join(after_any)
            lines.append(f"#SBATCH --dependency=afterany:{dep_str}")
        
        # Custom directives from config
        for directive in self.get_directives():
            if not directive.startswith('#SBATCH'):
                directive = f"#SBATCH {directive}"
            lines.append(directive)
        
        # Extra directives
        if extra_directives:
            for directive in extra_directives:
                if not directive.startswith('#SBATCH'):
                    directive = f"#SBATCH {directive}"
                lines.append(directive)
        
        lines.append("")
        
        # Working directory
        if working_dir:
            lines.append(f"cd {working_dir}")
        lines.append("")
        
        # Load modules
        modules = self.get_modules(gpu)
        if extra_modules:
            modules.extend(extra_modules)
        if modules:
            for module in modules:
                lines.append(f"module load {module}")
            lines.append("")
        
        # Environment variables from config
        if self.config and self.config.env_vars:
            for key, value in self.config.env_vars.items():
                lines.append(f"export {key}={value}")
            lines.append("")
        
        # Command
        lines.append(command)
        lines.append("")
        
        return "\n".join(lines)
    
    def _get_partition(self, gpu: bool) -> Optional[str]:
        """Get appropriate partition"""
        if gpu and self.config and self.config.gpu.partition:
            return self.config.gpu.partition
        return self.get_queue(gpu)
    
    def submit(self, script_path: str) -> Optional[str]:
        """Submit SLURM job and return job ID"""
        try:
            result = subprocess.run(
                ['sbatch', script_path],
                capture_output=True, text=True, check=True
            )
            # SLURM returns "Submitted batch job 12345"
            output = result.stdout.strip()
            match = re.search(r'(\d+)', output)
            if match:
                return match.group(1)
            return None
        except subprocess.CalledProcessError as e:
            print(f"SLURM submit failed: {e.stderr}")
            return None
        except FileNotFoundError:
            print("sbatch command not found - SLURM not available")
            return None
    
    def cancel(self, job_id: str) -> bool:
        """Cancel SLURM job"""
        try:
            subprocess.run(['scancel', job_id], check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError:
            return False
        except FileNotFoundError:
            return False
    
    def get_status(self, job_id: str) -> str:
        """Get SLURM job status"""
        try:
            result = subprocess.run(
                ['squeue', '-j', job_id, '-h', '-o', '%T'],
                capture_output=True, text=True
            )
            
            if result.returncode != 0 or not result.stdout.strip():
                # Job not in queue - check sacct
                return self._check_completed_job(job_id)
            
            state = result.stdout.strip().upper()
            state_map = {
                'PENDING': 'pending',
                'RUNNING': 'running',
                'COMPLETING': 'running',
                'COMPLETED': 'completed',
                'FAILED': 'failed',
                'CANCELLED': 'failed',
                'TIMEOUT': 'failed',
                'NODE_FAIL': 'failed',
                'PREEMPTED': 'failed'
            }
            return state_map.get(state, 'unknown')
            
        except FileNotFoundError:
            return 'unknown'
    
    def _check_completed_job(self, job_id: str) -> str:
        """Check sacct for completed job status"""
        try:
            result = subprocess.run(
                ['sacct', '-j', job_id, '-n', '-o', 'State', '-P'],
                capture_output=True, text=True
            )
            
            if result.returncode == 0 and result.stdout.strip():
                states = result.stdout.strip().split('\n')
                # Get first non-empty state
                for state in states:
                    state = state.strip().upper()
                    if state:
                        if 'COMPLETED' in state:
                            return 'completed'
                        elif 'FAILED' in state or 'CANCELLED' in state:
                            return 'failed'
            
            return 'completed'  # Assume completed if not found
            
        except FileNotFoundError:
            return 'completed'
    
    def get_job_info(self, job_id: str) -> Dict[str, Any]:
        """Get detailed SLURM job information"""
        info = {'job_id': job_id, 'status': self.get_status(job_id)}
        
        try:
            # Try squeue first for running jobs
            result = subprocess.run(
                ['squeue', '-j', job_id, '-h', '-o', '%j|%P|%N|%M|%L'],
                capture_output=True, text=True
            )
            
            if result.returncode == 0 and result.stdout.strip():
                parts = result.stdout.strip().split('|')
                if len(parts) >= 5:
                    info['job_name'] = parts[0]
                    info['partition'] = parts[1]
                    info['nodes'] = parts[2]
                    info['time_used'] = parts[3]
                    info['time_left'] = parts[4]
            else:
                # Try sacct for completed jobs
                result = subprocess.run(
                    ['sacct', '-j', job_id, '-n', '-o', 
                     'JobName,Partition,ExitCode,Elapsed', '-P'],
                    capture_output=True, text=True
                )
                
                if result.returncode == 0 and result.stdout.strip():
                    lines = result.stdout.strip().split('\n')
                    if lines:
                        parts = lines[0].split('|')
                        if len(parts) >= 4:
                            info['job_name'] = parts[0]
                            info['partition'] = parts[1]
                            info['exit_code'] = parts[2]
                            info['elapsed'] = parts[3]
                            
        except FileNotFoundError:
            pass
        
        return info
