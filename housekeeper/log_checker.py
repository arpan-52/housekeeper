# housekeeper/log_checker.py
"""
Log file parsing and error detection with whitelist support.
"""

import os
import re
from typing import List, Tuple, Optional, Set
from dataclasses import dataclass


@dataclass
class LogCheckResult:
    """Result of log file check"""
    success: bool
    log_path: str
    error_lines: List[str]
    has_severe: bool = False
    
    def __bool__(self):
        return self.success


def is_whitelisted(line: str, whitelist: List[str], match_threshold: int = 3) -> bool:
    """
    Check if a line matches any whitelist pattern.
    
    Uses fuzzy matching - if 3+ words from whitelist entry appear in line,
    it's considered whitelisted.
    
    Args:
        line: The log line to check
        whitelist: List of whitelisted error patterns
        match_threshold: Minimum word matches to consider whitelisted
    
    Returns:
        True if line is whitelisted (safe to ignore)
    """
    if not whitelist:
        return False
    
    line_lower = line.lower()
    line_words = set(line_lower.split())
    
    for pattern in whitelist:
        pattern_words = set(pattern.lower().split())
        matches = len(pattern_words.intersection(line_words))
        if matches >= match_threshold:
            return True
    
    return False


def check_log(log_path: str, 
              whitelist: Optional[List[str]] = None,
              error_patterns: Optional[List[str]] = None,
              check_severe: bool = True) -> LogCheckResult:
    """
    Check a log file for errors, filtering out whitelisted patterns.
    
    Args:
        log_path: Path to log file (.out, .err, .log, etc.)
        whitelist: List of patterns to ignore (empty by default)
        error_patterns: Patterns to search for (default: ['error', 'Error', 'ERROR'])
        check_severe: Also check for 'SEVERE' (CASA specific)
    
    Returns:
        LogCheckResult with success status and any error lines found
    """
    # Whitelist is empty by default - caller provides it
    active_whitelist = whitelist or []
    
    # Default error patterns
    if error_patterns is None:
        error_patterns = ['error', 'Error', 'ERROR']
    
    if check_severe:
        error_patterns.append('SEVERE')
    
    # Check file exists
    if not os.path.exists(log_path):
        return LogCheckResult(
            success=False,
            log_path=log_path,
            error_lines=[f"Log file not found: {log_path}"]
        )
    
    # Read and scan
    error_lines = []
    has_severe = False
    
    try:
        with open(log_path, 'r', errors='ignore') as f:
            for line in f:
                # Check if line contains any error pattern
                if any(pattern in line for pattern in error_patterns):
                    # Check if whitelisted
                    if not is_whitelisted(line, active_whitelist):
                        error_lines.append(line.strip())
                        if 'SEVERE' in line:
                            has_severe = True
    except Exception as e:
        return LogCheckResult(
            success=False,
            log_path=log_path,
            error_lines=[f"Error reading log file: {e}"]
        )
    
    return LogCheckResult(
        success=len(error_lines) == 0,
        log_path=log_path,
        error_lines=error_lines,
        has_severe=has_severe
    )


def check_job_logs(job_dir: str, job_name: str,
                   whitelist: Optional[List[str]] = None) -> LogCheckResult:
    """
    Check all log files for a job (.out, .err, .log).
    
    Args:
        job_dir: Directory containing job log files
        job_name: Base name of job (files are job_name.out, job_name.err, etc.)
        whitelist: List of patterns to ignore
    
    Returns:
        Combined LogCheckResult
    """
    extensions = ['.out', '.err', '.log']
    all_errors = []
    has_severe = False
    checked_any = False
    
    for ext in extensions:
        log_path = os.path.join(job_dir, f"{job_name}{ext}")
        if os.path.exists(log_path):
            checked_any = True
            result = check_log(log_path, whitelist)
            all_errors.extend(result.error_lines)
            if result.has_severe:
                has_severe = True
    
    if not checked_any:
        return LogCheckResult(
            success=False,
            log_path=job_dir,
            error_lines=[f"No log files found for job {job_name} in {job_dir}"]
        )
    
    return LogCheckResult(
        success=len(all_errors) == 0,
        log_path=job_dir,
        error_lines=all_errors,
        has_severe=has_severe
    )
