#!/usr/bin/env python3
"""
Housekeeper Usage Examples
"""

from housekeeper import Housekeeper, housekeeper

# =============================================================================
# Basic Usage
# =============================================================================

# Create housekeeper with config file
hk = Housekeeper()
hk.set_config('scheduler_config.yaml')

# Or in one line
hk = Housekeeper(config='scheduler_config.yaml')

# Or with convenience function
hk = housekeeper(config='scheduler_config.yaml', jobs_dir='./my_jobs')

# =============================================================================
# Submit Jobs
# =============================================================================

# Simple job
job = hk.submit(
    command="python my_script.py",
    name="my_job",
    nodes=1,
    ppn=4,
    walltime="02:00:00"
)
print(f"Submitted: {job.job_id}")

# Job with memory
job = hk.submit(
    command="python memory_intensive.py",
    name="big_memory_job",
    nodes=1,
    ppn=8,
    walltime="04:00:00",
    mem_gb=64
)

# GPU job (automatically uses GPU queue and modules from config)
job = hk.submit(
    command="python train_model.py",
    name="gpu_training",
    nodes=1,
    ppn=4,
    walltime="12:00:00",
    gpu=True  # This is the key!
)

# Job with dependencies
job1 = hk.submit(command="python step1.py", name="step1")
job2 = hk.submit(
    command="python step2.py",
    name="step2",
    after_ok=[job1.job_id]  # Only runs if job1 succeeds
)

# Job with extra modules
job = hk.submit(
    command="casa -c script.py",
    name="casa_job",
    extra_modules=["casa/6.5", "wsclean/3.4"]
)

# =============================================================================
# Monitor Jobs
# =============================================================================

# Check single job status
status = hk.status(job.job_id)
print(f"Job {job.job_id}: {status.state}")

# Wait for job to complete
results = hk.wait(job.job_id)

# Wait for multiple jobs
results = hk.wait([job1.job_id, job2.job_id])

# Wait with timeout (300 seconds)
results = hk.wait(job.job_id, timeout=300)

# Get all active jobs
active = hk.list_active()
for j in active:
    print(f"{j.name}: {j.state}")

# Get statistics
stats = hk.stats()
print(f"Completed: {stats['completed']}, Failed: {stats['failed']}")

# =============================================================================
# Job Control
# =============================================================================

# Cancel a job
hk.cancel(job.job_id)

# Cancel all jobs
hk.cancel_all()

# Retry a failed job
new_job = hk.retry(failed_job.job_id)

# =============================================================================
# Debug: Generate Script Without Submitting
# =============================================================================

# Just print the script that would be generated
hk.print_script(
    command="python test.py",
    name="test_job",
    nodes=2,
    ppn=8,
    gpu=True
)

# Get script as string
script = hk.generate_script(
    command="python test.py",
    name="test_job",
    nodes=1,
    ppn=4
)
print(script)
