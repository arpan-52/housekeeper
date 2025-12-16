#!/usr/bin/env python3
"""
Example: Radio astronomy pipeline with automatic retries
"""

from housekeeper import housekeeper

# Custom whitelist for radio astronomy tools
radio_whitelist = [
    "WARN",
    "FutureWarning",
    "Leap second table TAI_UTC seems out-of-date",
    "NumbaPendingDeprecationWarning",
    "CuPy may not function correctly",
    "No sources found in catalog",
]

# Initialize
hk = housekeeper(
    workdir="./radio_pipeline",
    error_whitelist=radio_whitelist,
    whitelist_threshold=3
)

# Step 1: Calibration (with retry on failure)
cal_job = hk.submit(
    command="python calibrate.py --input raw.ms",
    name="calibration",
    cpus=16,
    memory="64GB",
    walltime="04:00:00",
    expected_files=["calibrated.ms/table.dat"],
    max_retries=2  # Retry up to 2 times
)

# Step 2: Imaging (depends on calibration)
img_job = hk.submit(
    command="wsclean -name image -size 8192 8192 -scale 1asec calibrated.ms",
    name="imaging",
    gpus=1,
    cpus=8,
    memory="128GB",
    walltime="06:00:00",
    after_ok=[cal_job],
    expected_files=["image-MFS-image.fits"]
)

# Step 3: Source finding (depends on imaging)
src_job = hk.submit(
    command="python source_find.py --image image-MFS-image.fits",
    name="source_finding",
    cpus=4,
    memory="16GB",
    after_ok=[img_job],
    expected_files=["sources.txt"]
)

print(f"Pipeline submitted:")
print(f"  Calibration: {cal_job}")
print(f"  Imaging: {img_job}")
print(f"  Source finding: {src_job}")

# Monitor all jobs
results = hk.monitor([cal_job, img_job, src_job], poll_interval=30)

# Summary
print("\n=== Pipeline Results ===")
for result in results:
    status = result['status']
    name = result['name']
    
    if status == 'completed':
        print(f"✓ {name}: SUCCESS")
    else:
        print(f"✗ {name}: {status.upper()}")
        info = hk.failure_info(result['id'])
        if info:
            print(f"  Reason: {info['failure_reason']}")

# List all jobs
print("\n=== All Jobs ===")
for job in hk.list_jobs():
    print(f"{job['name']}: {job['status']} (retries: {job['retry_count']}/{job['max_retries']})")
