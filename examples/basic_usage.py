#!/usr/bin/env python3
"""
Basic usage example for housekeeper
"""

from housekeeper import housekeeper

# Initialize housekeeper with custom error whitelist
# All jobs go into a single directory
hk = housekeeper(
    jobs_dir="./my_pipeline",
    error_whitelist=[
        "WARN",
        "FutureWarning",
        "DeprecationWarning",
    ]
)

# Submit a simple job with custom subdirectory name
job1 = hk.submit(
    command="python process_data.py --input data.fits",
    name="process",
    job_subdir="preprocessing",  # Custom name instead of random ID
    cpus=4,
    memory="16GB",
    walltime="01:00:00",
    expected_files=["output.fits"]
)

print(f"Submitted job: {job1}")

# Submit a dependent job
job2 = hk.submit(
    command="python analyze.py --input output.fits",
    name="analyze",
    job_subdir="analysis",
    after_ok=[job1],
    cpus=2,
    memory="8GB"
)

print(f"Submitted dependent job: {job2}")

# Monitor both jobs
results = hk.monitor([job1, job2], poll_interval=10)

# Check results
for result in results:
    print(f"Job {result['name']}: {result['status']}")
    if result['status'] == 'failed':
        info = hk.failure_info(result['id'])
        print(f"  Failure reason: {info['failure_reason']}")
        print(f"  Error lines: {info['error_lines'][:3]}")

# Export state for debugging
hk.export_state("pipeline_state.json")

print("\nDirectory structure:")
print("my_pipeline/")
print("├── housekeeper.db")
print("├── preprocessing/")
print("│   ├── process.sh")
print("│   ├── stdout.log")
print("│   └── stderr.log")
print("└── analysis/")
print("    ├── analyze.sh")
print("    ├── stdout.log")
print("    └── stderr.log")
