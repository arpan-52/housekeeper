# Housekeeper v2.0

**HPC Job Management Library for PBS and SLURM**

A simple, powerful Python library for managing HPC jobs. Handles the complexity of different PBS versions (OpenPBS, Torque) and SLURM, GPU jobs, modules, and job dependencies.

## Installation

```bash
pip install pyyaml
pip install -e .
```

## Quick Start

```python
from housekeeper import Housekeeper

# Initialize with config
hk = Housekeeper()
hk.set_config('scheduler_config.yaml')

# Submit a job
job = hk.submit(
    command="python my_script.py",
    name="my_job",
    nodes=1,
    ppn=8,
    walltime="04:00:00"
)

# Wait for completion
hk.wait(job.job_id)
```

## Configuration

Create `scheduler_config.yaml` for your cluster:

### PBS/OpenPBS (e.g., NCRA Bhima)

```yaml
scheduler: pbs

pbs:
  resource_style: select  # 'select' for OpenPBS, 'nodes' for Torque
  
  queues:
    default: workq
    gpu: gpu
  
  gpu:
    enabled: true
    host: bhima04      # GPU node
    ngpus: 1
    modules: [cuda/12.3]
  
  directives:
    - "-V"             # Export environment
    - "-j oe"          # Merge stdout/stderr
  
  modules:
    - python/3.10
```

### SLURM (e.g., NRAO)

```yaml
scheduler: slurm

slurm:
  account: null
  
  queues:
    default: batch
    gpu: gpu
  
  gpu:
    enabled: true
    gres: "gpu:1"
    partition: gpu
    modules: [cuda/12.3]
  
  directives:
    - "--export=ALL"
  
  modules:
    - python/3.10
```

## Features

### Job Submission

```python
# Simple job
job = hk.submit(
    command="python script.py",
    name="my_job",
    nodes=1, ppn=4,
    walltime="02:00:00"
)

# With memory
job = hk.submit(
    command="python big_script.py",
    name="big_job",
    mem_gb=64,
    ...
)

# GPU job (uses GPU queue, host, modules automatically)
job = hk.submit(
    command="python train.py",
    name="gpu_job",
    gpu=True
)

# With dependencies
job1 = hk.submit(command="python step1.py", name="step1")
job2 = hk.submit(
    command="python step2.py",
    name="step2",
    after_ok=[job1.job_id]  # Runs only if job1 succeeds
)

# Extra modules for specific job
job = hk.submit(
    command="casa -c script.py",
    name="casa_job",
    extra_modules=["casa/6.5"]
)
```

### Monitoring

```python
# Check status
status = hk.status(job.job_id)
print(f"State: {status.state}")

# Wait for completion
results = hk.wait(job.job_id)
results = hk.wait([job1.job_id, job2.job_id])
results = hk.wait(job.job_id, timeout=300)

# List jobs
active = hk.list_active()
all_jobs = hk.list_jobs()

# Statistics
stats = hk.stats()
# {'pending': 2, 'running': 5, 'completed': 10, 'failed': 1, 'total': 18}
```

### Job Control

```python
hk.cancel(job.job_id)    # Cancel single job
hk.cancel_all()          # Cancel all active jobs
hk.retry(job.job_id)     # Retry failed job
```

### Debug

```python
# Print script without submitting
hk.print_script(
    command="python test.py",
    name="test",
    gpu=True
)
```

## Generated Scripts

### PBS (OpenPBS)
```bash
#!/bin/bash
#PBS -N my_job
#PBS -q workq
#PBS -l select=1:ncpus=8:mem=64gb
#PBS -l walltime=04:00:00
#PBS -V
#PBS -j oe

cd $PBS_O_WORKDIR

module load python/3.10

python my_script.py
```

### PBS (GPU)
```bash
#!/bin/bash
#PBS -N gpu_job
#PBS -q gpu
#PBS -l select=1:ncpus=4:ngpus=1:host=bhima04
#PBS -l walltime=12:00:00
#PBS -V
#PBS -j oe

cd $PBS_O_WORKDIR

module load python/3.10
module load cuda/12.3

python train.py
```

### SLURM
```bash
#!/bin/bash
#SBATCH --job-name=my_job
#SBATCH --partition=batch
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --export=ALL

module load python/3.10

python my_script.py
```

## Database

Housekeeper persists job information to SQLite:
- `jobs_dir/housekeeper.db`

This enables:
- Job tracking across restarts
- Historical statistics
- Retry failed jobs

## API Reference

### Housekeeper

| Method | Description |
|--------|-------------|
| `set_config(config)` | Load scheduler configuration |
| `submit(...)` | Submit a job |
| `status(job_id)` | Get job status |
| `wait(job_ids)` | Wait for jobs to complete |
| `cancel(job_id)` | Cancel a job |
| `cancel_all()` | Cancel all active jobs |
| `retry(job_id)` | Retry a failed job |
| `stats()` | Get job statistics |
| `list_jobs()` | List all jobs |
| `list_active()` | List active jobs |

### Job

| Property | Description |
|----------|-------------|
| `job_id` | Scheduler job ID |
| `name` | Job name |
| `state` | Current state (pending, running, completed, failed) |
| `is_done` | True if job finished |
| `is_running` | True if job running |
| `duration` | Runtime in seconds |

## License

MIT
