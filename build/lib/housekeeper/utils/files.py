"""
File-related utility functions
"""

import time
import glob as glob_module
from pathlib import Path
from typing import List


def wait_for_files(file_patterns: List[str], 
                   timeout: int = 300, 
                   check_interval: int = 5) -> bool:
    """
    Wait for files to appear on filesystem
    
    Args:
        file_patterns: List of file paths or glob patterns
        timeout: Maximum time to wait in seconds
        check_interval: Seconds between checks
        
    Returns:
        True if all files found, False if timeout
    """
    end_time = time.time() + timeout
    
    while time.time() < end_time:
        if check_files_exist(file_patterns):
            return True
        time.sleep(check_interval)
    
    return False


def check_files_exist(file_patterns: List[str]) -> bool:
    """
    Check if all files/patterns exist
    
    Args:
        file_patterns: List of file paths or glob patterns
        
    Returns:
        True if all files found
    """
    for pattern in file_patterns:
        # Handle glob patterns
        if '*' in pattern or '?' in pattern:
            matches = glob_module.glob(pattern)
            if not matches:
                return False
        else:
            # Regular file
            if not Path(pattern).exists():
                return False
    
    return True


def get_matching_files(file_patterns: List[str]) -> List[str]:
    """
    Get all files matching the patterns
    
    Args:
        file_patterns: List of file paths or glob patterns
        
    Returns:
        List of matching file paths
    """
    files = []
    
    for pattern in file_patterns:
        if '*' in pattern or '?' in pattern:
            files.extend(glob_module.glob(pattern))
        else:
            if Path(pattern).exists():
                files.append(pattern)
    
    return files
