# housekeeper

HPC job management with intelligent failure tracking for SLURM and PBS clusters.

## Features

- **Simple API**: Submit jobs with `submit()`, track with `track()`, monitor with `monitor()`
- **Smart failure detection**: Parses scheduler logs for errors with configurable whitelist
- **Dependency management**: Chain jobs with `after_ok`, `after_fail`, `after_any`
- **File validation**: Verify expected output files exist after job completion
- **Auto-retry**: Automatically retry failed jobs with configurable limits
- **Persistent state**: SQLite database tracks all job history

## Installation

```bash
pip install housekeeper
```

Or from source:

```bash
git clone https://github.com/yourusername/housekeeper.git
cd housekeeper
pip install -e .
```

## Quick Start

```python
from housekeeper import housekeeper

# Single directory for all jobs in this code run
hk = housekeeper(jobs_dir="./my_pipeline_run")

# Submit a job (optional: name the subdirectory)
job1 = hk.submit(
    command="python process.py --input data.fits",
    name="process",
    job_subdir="preprocessing",  # Custom subdir name
    cpus=8,
    memory="32GB",
    walltime="02:00:00",
    expected_files=["output.fits"],
)

# Submit dependent job
job2 = hk.submit(
    command="python analyze.py --input output.fits",
    name="analyze",
    job_subdir="analysis",
    after_ok=[job1],
)

# Monitor until completion
hk.monitor([job1, job2])

# Check results
for job in hk.list_jobs():
    print(f"{job['name']}: {job['status']}")
```

**Directory structure:**
```
my_pipeline_run/
├── housekeeper.db          # Job database
├── preprocessing/          # job1 (you named it)
│   ├── process.sh
│   ├── stdout.log
│   └── stderr.log
└── analysis/               # job2 (you named it)
    ├── analyze.sh
    ├── stdout.log
    └── stderr.log
```

**For large pipelines (millions of jobs):**

```python
# Single directory for entire run
hk = housekeeper(jobs_dir=f"./calibration_run_{obs_id}")

# Submit many jobs with custom subdirs
for spw in range(1000):
    job = hk.submit(
        command=f"calibrate.py --spw {spw}",
        name=f"cal_spw{spw}",
        job_subdir=f"spw_{spw:03d}",  # spw_000, spw_001, ...
        cpus=4
    )
```

Result:
```
calibration_run_12345/
├── housekeeper.db
├── spw_000/
├── spw_001/
├── spw_002/
└── ... (1000 total)
```

## Error Whitelisting

Many HPC applications emit warnings that look like errors. Provide a whitelist:

```python
hk = housekeeper(
    jobs_dir="./pipeline",
    error_whitelist=[
        "WARN",
        "FutureWarning",
        "DeprecationWarning",
        "Leap second table TAI_UTC seems out-of-date",
    ],
    whitelist_threshold=3,  # Word match threshold
)
```

## API Reference

### housekeeper

```python
hk = housekeeper(
    jobs_dir="./my_jobs",       # Directory for all jobs in this run
    scheduler=None,             # "slurm", "pbs", or None for auto-detect
    db_path=None,               # SQLite path (default: jobs_dir/housekeeper.db)
    error_whitelist=None,       # List of error patterns to ignore
    whitelist_threshold=3,      # Word match threshold for whitelist
)
```

### submit()

```python
job_id = hk.submit(
    command="...",              # Command to run
    name="job_name",            # Job name
    job_subdir=None,            # Custom subdirectory name (default: job_id)
    nodes=1,                    # Number of nodes
    cpus=1,                     # CPUs per node
    gpus=0,                     # GPUs per node
    memory="4GB",               # Memory
    walltime="01:00:00",        # Wall time limit
    queue=None,                 # Queue/partition
    account=None,               # Account/project
    expected_files=[],          # Files that should exist after completion
    after_ok=[],                # Run after these jobs complete successfully
    after_fail=[],              # Run after these jobs fail
    after_any=[],               # Run after these jobs finish (any status)
    max_retries=0,              # Auto-retry count on failure
    env=None,                   # Environment variables dict
)
```

### track()

```python
status = hk.track(job_id)       # Returns JobStatus enum
```

### monitor()

```python
results = hk.monitor(
    job_ids,                    # List of job IDs
    poll_interval=30,           # Seconds between checks
)
```

### Other methods

```python
hk.cancel(job_id)               # Cancel a job
hk.retry(job_id)                # Retry a failed job
hk.list_jobs(status=None)       # List jobs, optionally filter by status
hk.job_info(job_id)             # Get detailed job info
hk.failure_info(job_id)         # Get failure details
hk.cleanup(job_id)              # Remove job files
hk.export_state(path)           # Export state to JSON
```

## License

MIT
