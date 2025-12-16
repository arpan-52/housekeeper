"""
Log parser for detecting errors in job output
Based on the check_error_whitelist logic from utils
"""

import re
from pathlib import Path
from typing import List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class log_result:
    """Result of log parsing"""
    has_errors: bool
    error_lines: List[str]
    total_errors: int
    whitelisted_errors: int


class log_parser:
    """Parse log files for errors with configurable whitelist"""
    
    # Default error patterns to search for
    default_error_patterns = [
        r'\berror\b',
        r'\bexception\b',
        r'\bfailed\b',
        r'\bfailure\b',
        r'segmentation fault',
        r'core dumped',
        r'\bkilled\b',
        r'traceback',
        r'\bsevere\b',
    ]
    
    def __init__(self, 
                 error_whitelist: List[str] = None,
                 whitelist_threshold: int = 3,
                 custom_patterns: List[str] = None,
                 case_sensitive: bool = False):
        """
        Initialize log parser
        
        Args:
            error_whitelist: List of error patterns to ignore
            whitelist_threshold: Minimum word matches to consider whitelisted
            custom_patterns: Additional regex patterns to consider as errors
            case_sensitive: Whether pattern matching is case sensitive
        """
        self.error_whitelist = error_whitelist or []
        self.whitelist_threshold = whitelist_threshold
        self.case_sensitive = case_sensitive
        
        # Build error patterns
        self.error_patterns = self.default_error_patterns.copy()
        if custom_patterns:
            self.error_patterns.extend(custom_patterns)
    
    def parse(self, log_path: str, max_lines: int = 10000) -> log_result:
        """
        Parse a log file for errors
        
        Args:
            log_path: Path to log file
            max_lines: Maximum lines to read from end of file
            
        Returns:
            log_result with error information
        """
        if not Path(log_path).exists():
            return log_result(
                has_errors=False,
                error_lines=[],
                total_errors=0,
                whitelisted_errors=0
            )
        
        try:
            # Read log file (last N lines for efficiency)
            with open(log_path, 'r', errors='ignore') as f:
                # Read all lines and take last max_lines
                lines = f.readlines()
                if len(lines) > max_lines:
                    lines = lines[-max_lines:]
            
            error_lines = []
            total_errors = 0
            whitelisted_errors = 0
            
            # Search for error patterns
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Check if line matches any error pattern
                is_error = False
                for pattern in self.error_patterns:
                    flags = 0 if self.case_sensitive else re.IGNORECASE
                    if re.search(pattern, line, flags):
                        is_error = True
                        break
                
                if is_error:
                    total_errors += 1
                    
                    # Check if this error is whitelisted
                    if self.is_whitelisted(line):
                        whitelisted_errors += 1
                    else:
                        # Keep line, truncate if too long
                        if len(line) > 500:
                            line = line[:500] + "..."
                        error_lines.append(line)
                
                # Limit number of error lines kept
                if len(error_lines) >= 50:
                    break
            
            return log_result(
                has_errors=len(error_lines) > 0,
                error_lines=error_lines,
                total_errors=total_errors,
                whitelisted_errors=whitelisted_errors
            )
            
        except Exception as e:
            # If we can't parse the log, return no errors found
            return log_result(
                has_errors=False,
                error_lines=[f"Failed to parse log: {str(e)}"],
                total_errors=0,
                whitelisted_errors=0
            )
    
    def is_whitelisted(self, line: str) -> bool:
        """
        Check if an error line matches the whitelist
        
        This uses word-based matching: counts how many words from the
        whitelist pattern appear in the error line. If the count exceeds
        the threshold, the error is considered whitelisted.
        
        Args:
            line: Error line to check
            
        Returns:
            True if line matches whitelist
        """
        if not self.error_whitelist:
            return False
        
        line_lower = line.lower() if not self.case_sensitive else line
        
        for white_error in self.error_whitelist:
            white_lower = white_error.lower() if not self.case_sensitive else white_error
            
            # Split into words and compare
            white_words = set(white_lower.split())
            error_words = set(line_lower.split())
            
            # Count matching words
            matches = len(white_words.intersection(error_words))
            
            if matches >= self.whitelist_threshold:
                return True
        
        return False
    
    def parse_multiple(self, log_paths: List[str]) -> log_result:
        """
        Parse multiple log files and aggregate results
        
        Args:
            log_paths: List of log file paths
            
        Returns:
            Combined log_result
        """
        all_error_lines = []
        total_errors = 0
        whitelisted_errors = 0
        
        for log_path in log_paths:
            result = self.parse(log_path)
            all_error_lines.extend(result.error_lines)
            total_errors += result.total_errors
            whitelisted_errors += result.whitelisted_errors
        
        return log_result(
            has_errors=len(all_error_lines) > 0,
            error_lines=all_error_lines,
            total_errors=total_errors,
            whitelisted_errors=whitelisted_errors
        )
