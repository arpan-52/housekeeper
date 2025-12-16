"""
Failure detector - checks multiple failure modes
"""

import glob
from pathlib import Path
from typing import List, Tuple, Optional

from ..job import job, job_status, failure_type
from .log_parser import log_parser


class failure_detector:
    """Detect job failures from multiple sources"""
    
    def __init__(self, log_parser_instance: log_parser):
        """
        Initialize failure detector
        
        Args:
            log_parser_instance: Configured log parser
        """
        self.log_parser = log_parser_instance
    
    def detect(self, j: job) -> Tuple[bool, Optional[failure_type], Optional[str], List[str]]:
        """
        Detect if job failed and determine failure type
        
        Args:
            j: Job to check
            
        Returns:
            Tuple of (failed, failure_type, reason, error_lines)
        """
        # 1. Check scheduler status
        if j.status == job_status.failed:
            return True, failure_type.scheduler, "scheduler reported failure", []
        
        if j.status == job_status.cancelled:
            return True, failure_type.scheduler, "job was cancelled", []
        
        if j.status == job_status.timeout:
            return True, failure_type.timeout, "job exceeded walltime", []
        
        # 2. Check exit code
        if j.exit_code is not None and j.exit_code != 0:
            return True, failure_type.exit_code, f"exit code {j.exit_code}", []
        
        # 3. Check logs for errors
        error_lines = []
        log_files = []
        
        if j.stderr_path and Path(j.stderr_path).exists():
            log_files.append(j.stderr_path)
        
        if j.stdout_path and Path(j.stdout_path).exists():
            log_files.append(j.stdout_path)
        
        if log_files:
            result = self.log_parser.parse_multiple(log_files)
            if result.has_errors:
                error_lines = result.error_lines[:20]  # Keep first 20 errors
                reason = f"found {len(result.error_lines)} error(s) in logs"
                return True, failure_type.log_error, reason, error_lines
        
        # 4. Check expected output files
        missing = self.check_expected_files(j.expected_files)
        if missing:
            reason = f"missing output files: {', '.join(missing[:5])}"
            if len(missing) > 5:
                reason += f" and {len(missing)-5} more"
            return True, failure_type.missing_file, reason, []
        
        # No failure detected
        return False, None, None, []
    
    def check_expected_files(self, expected_files: List[str]) -> List[str]:
        """
        Check if expected output files exist
        
        Args:
            expected_files: List of file patterns to check
            
        Returns:
            List of missing files/patterns
        """
        missing = []
        
        for file_pattern in expected_files:
            # Handle glob patterns
            if '*' in file_pattern or '?' in file_pattern:
                matches = glob.glob(file_pattern)
                if not matches:
                    missing.append(file_pattern)
            else:
                # Regular file
                if not Path(file_pattern).exists():
                    missing.append(file_pattern)
        
        return missing
    
    def check_oom_killed(self, log_path: str) -> bool:
        """
        Check if job was killed due to out of memory
        
        Args:
            log_path: Path to log file
            
        Returns:
            True if OOM detected
        """
        if not Path(log_path).exists():
            return False
        
        oom_patterns = [
            r'out of memory',
            r'oom-kill',
            r'memory limit',
            r'exceeded memory limit',
            r'malloc.*failed',
            r'cannot allocate memory',
        ]
        
        try:
            with open(log_path, 'r', errors='ignore') as f:
                content = f.read().lower()
                for pattern in oom_patterns:
                    import re
                    if re.search(pattern, content, re.IGNORECASE):
                        return True
        except Exception:
            pass
        
        return False
    
    def extract_exit_code(self, log_path: str) -> Optional[int]:
        """
        Try to extract exit code from log file
        
        Some schedulers write exit code to log files
        
        Args:
            log_path: Path to log file
            
        Returns:
            Exit code if found
        """
        if not Path(log_path).exists():
            return None
        
        try:
            with open(log_path, 'r', errors='ignore') as f:
                # Read last 100 lines
                lines = f.readlines()[-100:]
                
                for line in lines:
                    # Look for common exit code patterns
                    import re
                    
                    # SLURM: "DUE TO TIME LIMIT ***"
                    # PBS: "exit code 1"
                    # Generic: "exited with code 1"
                    match = re.search(r'exit(?:ed)?\s+(?:with\s+)?code\s+(\d+)', line, re.IGNORECASE)
                    if match:
                        return int(match.group(1))
                    
                    # Another pattern: "Command exit status: 1"
                    match = re.search(r'exit\s+status:\s+(\d+)', line, re.IGNORECASE)
                    if match:
                        return int(match.group(1))
        
        except Exception:
            pass
        
        return None
