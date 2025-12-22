# housekeeper/config.py
"""
Scheduler configuration management for housekeeper.
Handles PBS/SLURM differences, GPU nodes, queues, modules, etc.
"""

import os
import yaml
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any


@dataclass
class GPUConfig:
    """GPU node configuration"""
    enabled: bool = False
    host: Optional[str] = None          # PBS: specific GPU node (e.g., bhima04)
    ngpus: int = 1                       # Number of GPUs to request
    gres: Optional[str] = None          # SLURM: --gres string (e.g., "gpu:1")
    partition: Optional[str] = None      # SLURM: GPU partition
    queue: Optional[str] = None          # PBS: GPU queue
    modules: List[str] = field(default_factory=list)  # GPU-specific modules


@dataclass  
class QueueConfig:
    """Queue/partition configuration"""
    default: str = "batch"
    gpu: Optional[str] = None
    high_mem: Optional[str] = None
    express: Optional[str] = None


@dataclass
class SchedulerConfig:
    """Complete scheduler configuration"""
    scheduler: str = "pbs"  # 'pbs' or 'slurm'
    
    # Working directories
    working_dir: Optional[str] = None
    job_dir: str = "./jobs"
    
    # PBS-specific
    pbs_resource_style: str = "select"  # 'select' (OpenPBS) or 'nodes' (Torque)
    
    # SLURM-specific  
    slurm_account: Optional[str] = None
    
    # Queues
    queues: QueueConfig = field(default_factory=QueueConfig)
    
    # GPU configuration
    gpu: GPUConfig = field(default_factory=GPUConfig)
    
    # Directives added to every job
    directives: List[str] = field(default_factory=list)
    
    # Modules loaded for every job
    modules: List[str] = field(default_factory=list)
    
    # Environment variables
    env_vars: Dict[str, str] = field(default_factory=dict)


def load_config(config_path: str) -> SchedulerConfig:
    """
    Load scheduler configuration from YAML file.
    
    Args:
        config_path: Path to scheduler_config.yaml
    
    Returns:
        SchedulerConfig object
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        data = yaml.safe_load(f)
    
    return parse_config(data)


def parse_config(data: Dict[str, Any]) -> SchedulerConfig:
    """
    Parse config dictionary into SchedulerConfig object.
    
    Handles both PBS and SLURM configurations.
    """
    scheduler_type = data.get('scheduler', 'pbs').lower()
    
    config = SchedulerConfig(
        scheduler=scheduler_type,
        working_dir=data.get('working_dir'),
        job_dir=data.get('job_dir', './jobs')
    )
    
    # Parse scheduler-specific config
    if scheduler_type == 'pbs':
        pbs_data = data.get('pbs', {})
        config.pbs_resource_style = pbs_data.get('resource_style', 'select')
        
        # Queues
        queues = pbs_data.get('queues', {})
        config.queues = QueueConfig(
            default=queues.get('default', 'workq'),
            gpu=queues.get('gpu'),
            high_mem=queues.get('high_mem'),
            express=queues.get('express')
        )
        
        # GPU
        gpu_data = pbs_data.get('gpu', {})
        config.gpu = GPUConfig(
            enabled=gpu_data.get('enabled', False),
            host=gpu_data.get('host'),
            ngpus=gpu_data.get('ngpus', 1),
            queue=queues.get('gpu'),
            modules=gpu_data.get('modules', [])
        )
        
        # Directives and modules
        config.directives = pbs_data.get('directives', [])
        config.modules = pbs_data.get('modules', [])
        config.env_vars = pbs_data.get('env_vars', {})
        
    elif scheduler_type == 'slurm':
        slurm_data = data.get('slurm', {})
        config.slurm_account = slurm_data.get('account')
        
        # Queues (partitions in SLURM)
        queues = slurm_data.get('queues', {})
        config.queues = QueueConfig(
            default=queues.get('default', 'batch'),
            gpu=queues.get('gpu'),
            high_mem=queues.get('high_mem'),
            express=queues.get('express')
        )
        
        # GPU
        gpu_data = slurm_data.get('gpu', {})
        config.gpu = GPUConfig(
            enabled=gpu_data.get('enabled', False),
            gres=gpu_data.get('gres', 'gpu:1'),
            partition=gpu_data.get('partition'),
            modules=gpu_data.get('modules', [])
        )
        
        # Directives and modules
        config.directives = slurm_data.get('directives', [])
        config.modules = slurm_data.get('modules', [])
        config.env_vars = slurm_data.get('env_vars', {})
    
    return config


def create_default_config(scheduler: str = 'pbs') -> Dict[str, Any]:
    """
    Create a default configuration dictionary.
    Useful for generating template config files.
    """
    if scheduler == 'pbs':
        return {
            'scheduler': 'pbs',
            'working_dir': None,
            'job_dir': './jobs',
            'pbs': {
                'resource_style': 'select',  # or 'nodes' for Torque
                'queues': {
                    'default': 'workq',
                    'gpu': 'gpu',
                    'high_mem': None,
                    'express': None
                },
                'gpu': {
                    'enabled': False,
                    'host': 'gpu_node',
                    'ngpus': 1,
                    'modules': ['cuda/12.3']
                },
                'directives': [
                    '#PBS -V',
                    '#PBS -j oe'
                ],
                'modules': [],
                'env_vars': {}
            }
        }
    else:
        return {
            'scheduler': 'slurm',
            'working_dir': None,
            'job_dir': './jobs',
            'slurm': {
                'account': None,
                'queues': {
                    'default': 'batch',
                    'gpu': 'gpu',
                    'high_mem': None,
                    'express': None
                },
                'gpu': {
                    'enabled': False,
                    'gres': 'gpu:1',
                    'partition': 'gpu',
                    'modules': ['cuda/12.3']
                },
                'directives': [
                    '#SBATCH --export=ALL'
                ],
                'modules': [],
                'env_vars': {}
            }
        }


def save_default_config(path: str, scheduler: str = 'pbs'):
    """Save a default config file as template."""
    config = create_default_config(scheduler)
    with open(path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
