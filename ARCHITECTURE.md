# housekeeper Architecture

Internal architecture and design decisions.

## Overview

housekeeper is a Python library for managing HPC jobs with intelligent failure tracking. It provides a simple API that hides the complexity of scheduler backends (SLURM/PBS) and log parsing.

## Design Philosophy

1. **Simple, lowercase API**: No unnecessary capitals (`housekeeper`, not `Housekeeper`)
2. **Agnostic**: No domain-specific code, users provide their own whitelists
3. **Two-level tracking**: Scheduler status + log parsing
4. **Persistent state**: SQLite for reliability
5. **No clutter**: Track via scheduler's stdout/stderr, no extra logging

## Directory Structure

```
housekeeper/
├── __init__.py              # Public API exports
├── core.py                  # Main housekeeper class (user-facing)
├── job.py                   # Data models (job, job_status, etc.)
├── database.py              # SQLite persistence
│
├── scheduler/               # Scheduler backends
│   ├── __init__.py
│   ├── base.py              # Abstract base class
│   ├── slurm.py             # SLURM implementation
│   └── pbs.py               # PBS implementation
│
├── tracking/                # Failure detection
│   ├── __init__.py
│   ├── log_parser.py        # Generic log parsing with whitelist
│   └── failure_detector.py  # Multi-mode failure detection
│
└── utils/                   # Utilities
    ├── __init__.py
    ├── helpers.py           # Job ID, scheduler detection, etc.
    └── files.py             # File existence checking
```

## Core Components

### 1. housekeeper (core.py)

The main user-facing class. Responsibilities:

- Job submission workflow
- Status tracking and polling
- Dependency management
- Retry logic
- Database interaction

**Key methods:**
- `submit()`: Create and submit job
- `track()`: Check single job status
- `monitor()`: Poll until jobs complete
- `_submit_job()`: Internal submission to scheduler
- `_check_job_completion()`: Run failure detection
- `_check_dependents()`: Trigger dependent jobs

### 2. Job Models (job.py)

Data classes for type safety:

- `job`: Represents a single HPC job
- `job_status`: Enum of statuses (pending, queued, running, etc.)
- `job_resources`: Resource requirements
- `failure_type`: Types of failures (scheduler, exit_code, log_error, etc.)

All lowercase for consistency.

### 3. Database (database.py)

SQLite storage with thread safety.

**Tables:**
- `jobs`: All job metadata
- `dependencies`: Job dependency graph

**Key features:**
- Thread-safe with locks
- JSON serialization for complex types
- Efficient querying with indexes

### 4. Scheduler Backends

Abstract base class with implementations for SLURM and PBS.

**Interface:**
- `generate_script()`: Create batch script
- `submit()`: Submit to scheduler, return ID
- `status()`: Query job status
- `cancel()`: Cancel job
- `check_available()`: Is this scheduler present?

**SLURM (slurm.py):**
- Uses `sbatch`, `squeue`, `sacct`, `scancel`
- Parses "Submitted batch job 12345"
- Checks both queue and history

**PBS (pbs.py):**
- Uses `qsub`, `qstat`, `qdel`
- Parses job ID from qsub output
- Status codes: R (running), Q (queued), C (completed)

### 5. Log Parser (tracking/log_parser.py)

Generic error detection with whitelist support.

**Algorithm:**
1. Read log file (last N lines for efficiency)
2. Search for error patterns (regex)
3. For each error, check if whitelisted
4. Return non-whitelisted errors

**Whitelist matching:**
- Word-based (not exact match)
- Configurable threshold (default: 3 matching words)
- Case-insensitive option

**Example:**
```
Error line: "FutureWarning: something deprecated"
Whitelist:  "FutureWarning"
Words:      {"futurewarning"} ∩ {"futurewarning", "something", "deprecated"}
Matches:    1 word
Threshold:  3
Result:     NOT whitelisted (1 < 3)

Whitelist:  "FutureWarning something deprecated"
Matches:    3 words
Result:     WHITELISTED (3 >= 3)
```

### 6. Failure Detector (tracking/failure_detector.py)

Multi-mode failure detection:

1. **Scheduler status**: Check if scheduler marked as failed
2. **Exit code**: Non-zero exit code
3. **Log errors**: Parse stdout/stderr for errors
4. **Missing files**: Check expected output files
5. **OOM detection**: Specific patterns for memory issues

Returns tuple: `(failed, failure_type, reason, error_lines)`

### 7. Utilities (utils/)

Helper functions:

- `generate_job_id()`: 8-char UUID
- `detect_scheduler()`: Check for sbatch/qsub
- `format_duration()`: Human-readable time
- `wait_for_files()`: Poll for file existence
- `check_files_exist()`: One-shot file check

## Data Flow

### Job Submission

```
user calls submit()
    ↓
create job object
    ↓
save to database
    ↓
check dependencies
    ↓
if ready: _submit_job()
    ↓
generate script
    ↓
write to disk
    ↓
call backend.submit()
    ↓
update with scheduler_id
```

### Status Tracking

```
user calls track(job_id)
    ↓
load job from database
    ↓
if pending: check dependencies
    ↓
if dependencies ready: submit
    ↓
if queued/running: query scheduler
    ↓
update database
    ↓
if finished: _check_job_completion()
    ↓
run failure_detector
    ↓
if failed & retries left: _retry_job()
    ↓
check dependents
    ↓
submit ready dependents
```

### Failure Detection

```
_check_job_completion()
    ↓
extract exit code from logs
    ↓
call failure_detector.detect()
    ↓
check scheduler status
    ↓
check exit code
    ↓
parse logs with log_parser
    ↓
check expected files
    ↓
return (failed, type, reason, errors)
    ↓
if failed: update database
    ↓
if retry_count < max: _retry_job()
```

## Dependency Management

Dependencies form a DAG (Directed Acyclic Graph):

```python
# Example DAG:
#       A
#      / \
#     B   C
#      \ /
#       D

job_a = submit(...)
job_b = submit(..., after_ok=[job_a])
job_c = submit(..., after_ok=[job_a])
job_d = submit(..., after_ok=[job_b, job_c])
```

**Three dependency types:**
- `after_ok`: Run after successful completion
- `after_fail`: Run after failure (e.g., cleanup)
- `after_any`: Run after any terminal state

**Resolution:**
1. Job A completes
2. housekeeper calls `_check_dependents(job_a)`
3. Finds jobs B and C depend on A
4. Checks if B and C are ready (all deps satisfied)
5. Submits B and C
6. Eventually D becomes ready and gets submitted

## Generated Scripts

Clean and simple, no clutter:

```bash
#!/bin/bash
#SBATCH --job-name=myjob
#SBATCH --output=/path/to/stdout.log
#SBATCH --error=/path/to/stderr.log
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16GB
#SBATCH --time=02:00:00

export MY_VAR=value  # If env provided

cd $SLURM_SUBMIT_DIR

python script.py  # User's command
```

**No extra logging**: We track via the scheduler's stdout/stderr files.

## Database Schema

### jobs table

Stores all job metadata:

```sql
CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    command TEXT NOT NULL,
    workdir TEXT NOT NULL,
    script_path TEXT,
    scheduler_id TEXT,
    
    resources TEXT NOT NULL,      -- JSON
    
    status TEXT DEFAULT 'pending',
    exit_code INTEGER,
    
    stdout_path TEXT,
    stderr_path TEXT,
    expected_files TEXT,          -- JSON array
    
    created_at TIMESTAMP NOT NULL,
    submitted_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    failure_type TEXT,
    failure_reason TEXT,
    error_lines TEXT,             -- JSON array
    
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 0,
    parent_job_id TEXT,
    
    env TEXT DEFAULT '{}'         -- JSON object
)
```

### dependencies table

Stores job dependency graph:

```sql
CREATE TABLE dependencies (
    job_id TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    dep_type TEXT DEFAULT 'after_ok',
    PRIMARY KEY (job_id, depends_on, dep_type)
)
```

## Thread Safety

- Database operations use threading.Lock()
- Each database connection is opened/closed per operation
- Safe for concurrent access from multiple processes

## Error Handling

Exceptions are caught at boundaries:

- Scheduler submission failures → update job status to failed
- Log parsing failures → return empty result (don't fail tracking)
- Database errors → propagate up (these are critical)

## Performance Considerations

1. **Polling interval**: Default 30s, user configurable
2. **Log parsing**: Only last 10K lines to avoid memory issues
3. **Database queries**: Indexed on status, scheduler_id, created_at
4. **Batch operations**: Fetch all dependencies in single query

## Future Extensions

Possible additions without breaking API:

1. **Job arrays**: Submit array[1-100] jobs
2. **Resource prediction**: Estimate needed resources
3. **Dashboard**: Web UI for monitoring
4. **Notifications**: Email/Slack on job events
5. **Cost tracking**: Track compute hours used
6. **Checkpoint/restart**: Save intermediate state

## Testing

Minimal test structure (to be expanded):

```
tests/
├── test_job.py              # Job model tests
├── test_database.py         # Database operations
├── test_log_parser.py       # Log parsing logic
├── test_failure_detector.py # Failure detection
├── test_scheduler.py        # Scheduler backends
└── test_integration.py      # End-to-end tests
```

## Why These Choices?

**SQLite vs JSON files:**
- SQLite handles concurrent access
- Efficient querying (status filters, etc.)
- ACID transactions
- Still portable (single file)

**Lowercase naming:**
- Less typing
- Modern Python style
- Friendly, approachable

**No extra logging:**
- Scheduler already provides stdout/stderr
- Less clutter in filesystem
- Parse what's already there

**Word-based whitelist:**
- More flexible than exact match
- Handles variations in error messages
- Configurable threshold for precision

**Separate tracking module:**
- Allows custom log parsers
- Domain-agnostic design
- Easy to extend
