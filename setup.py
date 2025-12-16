#!/usr/bin/env python3
"""
setup.py for housekeeper
"""

from setuptools import setup, find_packages

setup(
    name="housekeeper",
    version="0.1.0",
    packages=find_packages(),
    python_requires=">=3.8",
    author="housekeeper contributors",
    description="HPC job management with intelligent failure tracking",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    license="MIT",
    keywords="hpc slurm pbs job-scheduler cluster",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Topic :: System :: Distributed Computing",
    ],
)
