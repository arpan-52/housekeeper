# housekeeper/scheduler/pbs.py
"""
PBS/OpenPBS/Torque scheduler implementation
"""

import subprocess
import re
from typing import Optional, List, Dict, Any
from .base import BaseScheduler
from ..config import SchedulerConfig


class PBSScheduler(BaseScheduler):
    """PBS scheduler (OpenPBS and Torque)"""
    
    def __init__(self, config: Optional[SchedulerConfig] = None):
        super().__init__(config)
        # Detect resource style from config or default to 'select' (OpenPBS)
        if config:
            self.resource_style = config.pbs_resource_style
        else:
            self.resource_style = 'select'
    
    @property
    def script_extension(self) -> str:
        return '.pbs'
    
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
        """
        Build PBS batch script.
        
        Supports both OpenPBS (select syntax) and Torque (nodes syntax).
        """
        lines = ["#!/bin/bash"]
        
        # Job name
        lines.append(f"#PBS -N {job_name}")
        
        # Queue
        queue = self.get_queue(gpu)
        if queue:
            lines.append(f"#PBS -q {queue}")
        
        # Resource specification
        resource_line = self._build_resource_line(nodes, ppn, mem_gb, gpu)
        lines.append(f"#PBS -l {resource_line}")
        
        # Walltime
        lines.append(f"#PBS -l walltime={walltime}")
        
        # Output files
        if output_file:
            lines.append(f"#PBS -o {output_file}")
        if error_file:
            lines.append(f"#PBS -e {error_file}")
        
        # Dependencies
        if after_ok:
            dep_str = ":".join(after_ok)
            lines.append(f"#PBS -W depend=afterok:{dep_str}")
        if after_any:
            dep_str = ":".join(after_any)
            lines.append(f"#PBS -W depend=afterany:{dep_str}")
        
        # Custom directives from config
        for directive in self.get_directives():
            if not directive.startswith('#PBS'):
                directive = f"#PBS {directive}"
            lines.append(directive)
        
        # Extra directives passed directly
        if extra_directives:
            for directive in extra_directives:
                if not directive.startswith('#PBS'):
                    directive = f"#PBS {directive}"
                lines.append(directive)
        
        lines.append("")
        
        # Working directory
        if working_dir:
            lines.append(f"cd {working_dir}")
        else:
            lines.append("cd $PBS_O_WORKDIR")
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
    
    def _build_resource_line(self, nodes: int, ppn: int, 
                              mem_gb: Optional[int], gpu: bool) -> str:
        """Build resource specification line based on style"""
        
        if self.resource_style == 'select':
            # OpenPBS style: select=1:ncpus=4:mem=32gb
            parts = [f"select={nodes}:ncpus={ppn}"]
            
            if mem_gb:
                parts[0] += f":mem={mem_gb}gb"
            
            # GPU configuration
            if gpu and self.config and self.config.gpu.enabled:
                parts[0] += f":ngpus={self.config.gpu.ngpus}"
                if self.config.gpu.host:
                    parts[0] += f":host={self.config.gpu.host}"
            
            return parts[0]
        
        else:
            # Torque style: nodes=1:ppn=4
            parts = [f"nodes={nodes}:ppn={ppn}"]
            
            if mem_gb:
                parts.append(f"mem={mem_gb}gb")
            
            # GPU for Torque (varies by site)
            if gpu and self.config and self.config.gpu.enabled:
                parts.append(f"gpus={self.config.gpu.ngpus}")
            
            return ",".join(parts)
    
    def submit(self, script_path: str) -> Optional[str]:
        """Submit PBS job and return job ID"""
        try:
            result = subprocess.run(
                ['qsub', script_path],
                capture_output=True, text=True, check=True
            )
            job_id = result.stdout.strip()
            # PBS returns full job ID like "12345.server", extract just the number
            if '.' in job_id:
                job_id = job_id.split('.')[0]
            return job_id
        except subprocess.CalledProcessError as e:
            print(f"PBS submit failed: {e.stderr}")
            return None
        except FileNotFoundError:
            print("qsub command not found - PBS not available")
            return None
    
    def cancel(self, job_id: str) -> bool:
        """Cancel PBS job"""
        try:
            subprocess.run(['qdel', job_id], check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError:
            return False
        except FileNotFoundError:
            return False
    
    def get_status(self, job_id: str) -> str:
        """Get PBS job status"""
        try:
            result = subprocess.run(
                ['qstat', '-f', job_id],
                capture_output=True, text=True
            )
            
            if result.returncode != 0:
                # Job not in queue - check if completed
                return 'completed'
            
            output = result.stdout
            
            # Parse job_state
            match = re.search(r'job_state\s*=\s*(\w+)', output)
            if match:
                state = match.group(1).upper()
                state_map = {
                    'Q': 'pending',
                    'H': 'pending',
                    'W': 'pending',
                    'R': 'running',
                    'E': 'running',
                    'C': 'completed',
                    'F': 'failed'
                }
                return state_map.get(state, 'unknown')
            
            return 'unknown'
            
        except FileNotFoundError:
            return 'unknown'
    
    def get_job_info(self, job_id: str) -> Dict[str, Any]:
        """Get detailed PBS job information"""
        info = {'job_id': job_id, 'status': self.get_status(job_id)}
        
        try:
            result = subprocess.run(
                ['qstat', '-f', job_id],
                capture_output=True, text=True
            )
            
            if result.returncode == 0:
                output = result.stdout
                
                # Parse various fields
                patterns = {
                    'job_name': r'Job_Name\s*=\s*(.+)',
                    'queue': r'queue\s*=\s*(\w+)',
                    'exit_status': r'Exit_status\s*=\s*(\d+)',
                    'output_path': r'Output_Path\s*=\s*(.+)',
                    'error_path': r'Error_Path\s*=\s*(.+)',
                }
                
                for key, pattern in patterns.items():
                    match = re.search(pattern, output)
                    if match:
                        info[key] = match.group(1).strip()
                
        except FileNotFoundError:
            pass
        
        return info
