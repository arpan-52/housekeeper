# housekeeper Usage Guide

Complete guide for using housekeeper to manage HPC jobs.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [API Reference](#api-reference)
- [Error Whitelisting](#error-whitelisting)
- [Dependencies](#dependencies)
- [Retries](#retries)
- [Advanced Usage](#advanced-usage)

## Installation

```bash
# From source
git clone https://github.com/yourusername/housekeeper.git
cd housekeeper
pip install -e .

# Or with pip (when published)
pip install housekeeper
```

## Quick Start

```python
from housekeeper import housekeeper

# Initialize (auto-detects SLURM or PBS)
hk = housekeeper(workdir="./my_jobs")

# Submit a job
job_id = hk.submit(
    command="python train.py --epochs 100",
    name="training",
    cpus=8,
    memory="32GB",
    walltime="04:00:00"
)

# Check status
status = hk.track(job_id)
print(f"Job status: {status}")

# Monitor until complete
hk.monitor([job_id])
```

## Core Concepts

### Jobs

A job represents a single computation submitted to the HPC scheduler. Each job has:

- **ID**: Unique identifier (8-character UUID)
- **Name**: Human-readable name
- **Command**: The actual command to run
- **Resources**: CPUs, GPUs, memory, walltime
- **Status**: pending, queued, running, completed, failed, etc.
- **Dependencies**: Other jobs this job depends on

### Status Flow

```
pending → queued → running → completed
                          ↘ failed → (retry)
```

### Working Directory

Each job gets its own directory:

```
workdir/
├── job_abc123/
│   ├── training.sh      # Generated batch script
│   ├── stdout.log       # Standard output
│   └── stderr.log       # Standard error
├── job_def456/
│   └── ...
└── housekeeper.db       # SQLite database
```

## API Reference

### housekeeper()

Initialize the job manager.

```python
hk = housekeeper(
    workdir="./jobs",              # Working directory
    scheduler=None,                # "slurm", "pbs", or None (auto)
    db_path=None,                  # Custom database path
    error_whitelist=None,          # List of errors to ignore
    whitelist_threshold=3          # Word match threshold
)
```

### submit()

Submit a new job.

```python
job_id = hk.submit(
    # Required
    command="python script.py",    # Command to run
    
    # Resources
    name="myjob",                  # Job name
    nodes=1,                       # Number of nodes
    cpus=4,                        # CPUs per node
    gpus=0,                        # GPUs per node
    memory="16GB",                 # Memory
    walltime="02:00:00",           # HH:MM:SS
    queue=None,                    # Queue/partition
    account=None,                  # Account/project
    
    # Validation
    expected_files=[],             # Files to check after completion
    
    # Dependencies
    after_ok=[],                   # Run after these succeed
    after_fail=[],                 # Run after these fail
    after_any=[],                  # Run after these finish
    
    # Retry
    max_retries=0,                 # Auto-retry count
    
    # Environment
    env={}                         # Environment variables
)
```

### track()

Check status of a single job (non-blocking).

```python
status = hk.track(job_id)
# Returns: job_status.pending, .queued, .running, .completed, .failed, etc.
```

### monitor()

Monitor jobs until all complete (blocking).

```python
results = hk.monitor(
    [job1, job2, job3],            # List of job IDs
    poll_interval=30               # Seconds between checks
)
# Returns list of job dictionaries
```

### Other Methods

```python
# Cancel a job
hk.cancel(job_id)

# Retry a failed job (creates new job)
new_job_id = hk.retry(job_id)

# List all jobs
jobs = hk.list_jobs(status="failed", limit=100)

# Get job details
info = hk.job_info(job_id)

# Get failure details
failure = hk.failure_info(job_id)

# Clean up job files
hk.cleanup(job_id)

# Export state to JSON
hk.export_state("state.json")
```

## Error Whitelisting

Many HPC applications emit warnings that look like errors but aren't. Use whitelisting to ignore them.

### Example: Radio Astronomy

```python
radio_whitelist = [
    "WARN",
    "FutureWarning", 
    "DeprecationWarning",
    "Leap second table TAI_UTC seems out-of-date",
    "NumbaPendingDeprecationWarning",
    "CuPy may not function correctly",
]

hk = housekeeper(
    workdir="./pipeline",
    error_whitelist=radio_whitelist,
    whitelist_threshold=3  # Minimum word matches
)
```

### How It Works

The whitelist uses word-based matching:

1. Error line: `"UserWarning: Leap second table TAI_UTC seems out-of-date"`
2. Whitelist entry: `"Leap second table TAI_UTC seems out-of-date"`
3. Matching words: `["leap", "second", "table", "tai_utc", "seems", "out-of-date"]`
4. Matches: 6 words → Exceeds threshold (3) → **Whitelisted**

### Custom Error Patterns

```python
from housekeeper.tracking import log_parser

parser = log_parser(
    error_whitelist=my_whitelist,
    custom_patterns=[
        r'FATAL:',
        r'CRITICAL:',
        r'my_app.*crashed',
    ]
)

hk = housekeeper(workdir="./jobs")
hk.log_parser = parser  # Replace default parser
```

## Dependencies

Chain jobs together with dependencies.

### after_ok: Run after success

```python
# Job 2 runs only if Job 1 succeeds
job1 = hk.submit(command="preprocess.py", name="preprocess")
job2 = hk.submit(
    command="train.py",
    name="train",
    after_ok=[job1]
)
```

### after_fail: Run after failure

```python
# Cleanup job runs only if main job fails
main = hk.submit(command="risky_job.py", name="main")
cleanup = hk.submit(
    command="cleanup.py",
    name="cleanup",
    after_fail=[main]
)
```

### after_any: Run after completion (any status)

```python
# Notification runs regardless of success/failure
process = hk.submit(command="process.py", name="process")
notify = hk.submit(
    command="send_email.py",
    name="notify",
    after_any=[process]
)
```

### Complex DAG

```python
# Diamond dependency pattern
data = hk.submit(command="download.py", name="data")

model_a = hk.submit(command="train_a.py", after_ok=[data])
model_b = hk.submit(command="train_b.py", after_ok=[data])

ensemble = hk.submit(
    command="ensemble.py",
    after_ok=[model_a, model_b]
)
```

## Retries

Automatically retry failed jobs.

### Basic Retry

```python
job = hk.submit(
    command="flaky_script.py",
    name="flaky",
    max_retries=3  # Retry up to 3 times
)
```

### Manual Retry

```python
# Check if failed
if hk.track(job_id) == "failed":
    # Create new retry job
    new_job = hk.retry(job_id)
```

### Retry Logic

1. Job fails
2. housekeeper checks `retry_count < max_retries`
3. If true, resubmits with incremented counter
4. Original job stays in database with `failed` status
5. New submission gets new scheduler ID

## Advanced Usage

### File Validation

Ensure output files exist after job completes:

```python
job = hk.submit(
    command="simulation.py",
    expected_files=[
        "output.h5",
        "results/*.txt",  # Glob patterns supported
        "data/final_*.fits"
    ]
)
```

### Environment Variables

Pass environment to job:

```python
job = hk.submit(
    command="python script.py",
    env={
        "OMP_NUM_THREADS": "16",
        "CUDA_VISIBLE_DEVICES": "0,1",
        "MY_VAR": "value"
    }
)
```

### Custom Scheduler Settings

```python
# Force specific scheduler
hk = housekeeper(
    workdir="./jobs",
    scheduler="slurm"  # Don't auto-detect
)

# Use specific queue/partition
job = hk.submit(
    command="...",
    queue="gpu",       # SLURM: partition, PBS: queue
    account="proj123"  # Billing account
)
```

### Failure Analysis

```python
# Get detailed failure info
info = hk.failure_info(job_id)

print(f"Exit code: {info['exit_code']}")
print(f"Failure type: {info['failure_type']}")
print(f"Reason: {info['failure_reason']}")
print(f"Error lines:")
for line in info['error_lines']:
    print(f"  {line}")

# Read full logs
with open(info['stderr_path']) as f:
    print(f.read())
```

### Batch Submission

```python
job_ids = []

for i in range(100):
    job = hk.submit(
        command=f"python process.py --index {i}",
        name=f"job_{i}",
        cpus=2
    )
    job_ids.append(job)

# Monitor all
hk.monitor(job_ids, poll_interval=60)
```

### State Export

```python
# Export all job info to JSON
hk.export_state("pipeline_state.json")

# Results in:
# {
#   "exported_at": "2024-12-16T10:30:00",
#   "total_jobs": 10,
#   "jobs": [
#     {"id": "abc123", "name": "job1", "status": "completed", ...},
#     ...
#   ]
# }
```

## Best Practices

1. **Use descriptive names**: `calibration_spw0` not `job_123`
2. **Set realistic walltimes**: Don't overestimate (wastes queue time)
3. **Validate outputs**: Use `expected_files` to catch failures
4. **Whitelist warnings**: Reduce false positive failures
5. **Use dependencies**: Build clear job DAGs
6. **Monitor in batches**: Don't poll every second
7. **Clean up**: Remove old job directories when done

## Troubleshooting

### Jobs stuck in pending

Check dependencies:
```python
job = hk.job_info(job_id)
print(job['after_ok'])  # Are these completed?
```

### False positive failures

Add to whitelist:
```python
# Check what errors were detected
info = hk.failure_info(job_id)
print(info['error_lines'])

# Add to whitelist
hk = housekeeper(
    error_whitelist=[
        "error pattern to ignore",
        ...
    ]
)
```

### Scheduler not detected

Force scheduler:
```python
hk = housekeeper(scheduler="slurm")  # or "pbs"
```

### Database locked errors

Use separate work directories for concurrent pipelines:
```python
hk1 = housekeeper(workdir="./pipeline1")
hk2 = housekeeper(workdir="./pipeline2")
```
