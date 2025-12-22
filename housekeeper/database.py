# housekeeper/database.py
"""
SQLite database for job persistence
"""

import os
import sqlite3
from datetime import datetime
from typing import List, Optional, Dict, Any
from contextlib import contextmanager

from .job import Job, JobState


class JobDatabase:
    """SQLite database for persisting job information"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema"""
        os.makedirs(os.path.dirname(self.db_path) if os.path.dirname(self.db_path) else '.', exist_ok=True)
        
        with self._connect() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    internal_id TEXT PRIMARY KEY,
                    job_id TEXT,
                    name TEXT NOT NULL,
                    command TEXT,
                    script_path TEXT,
                    nodes INTEGER DEFAULT 1,
                    ppn INTEGER DEFAULT 1,
                    walltime TEXT DEFAULT '04:00:00',
                    mem_gb INTEGER,
                    gpu INTEGER DEFAULT 0,
                    state TEXT DEFAULT 'pending',
                    exit_code INTEGER,
                    output_file TEXT,
                    error_file TEXT,
                    log_file TEXT,
                    working_dir TEXT,
                    job_subdir TEXT,
                    after_ok TEXT,
                    after_any TEXT,
                    submit_time TEXT,
                    start_time TEXT,
                    end_time TEXT,
                    attempt INTEGER DEFAULT 1,
                    max_retries INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_jobs_job_id ON jobs(job_id)
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_jobs_name ON jobs(name)
            ''')
    
    @contextmanager
    def _connect(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()
    
    def save_job(self, job: Job):
        """Save or update a job"""
        data = job.to_dict()
        data['updated_at'] = datetime.now().isoformat()
        data['gpu'] = 1 if job.gpu else 0
        
        with self._connect() as conn:
            # Check if exists
            existing = conn.execute(
                'SELECT internal_id FROM jobs WHERE internal_id = ?',
                (job.internal_id,)
            ).fetchone()
            
            if existing:
                # Update
                set_clause = ', '.join(f'{k} = ?' for k in data.keys())
                values = list(data.values()) + [job.internal_id]
                conn.execute(
                    f'UPDATE jobs SET {set_clause} WHERE internal_id = ?',
                    values
                )
            else:
                # Insert
                columns = ', '.join(data.keys())
                placeholders = ', '.join('?' * len(data))
                conn.execute(
                    f'INSERT INTO jobs ({columns}) VALUES ({placeholders})',
                    list(data.values())
                )
    
    def get_job(self, internal_id: str) -> Optional[Job]:
        """Get job by internal ID"""
        with self._connect() as conn:
            row = conn.execute(
                'SELECT * FROM jobs WHERE internal_id = ?',
                (internal_id,)
            ).fetchone()
            
            if row:
                return self._row_to_job(row)
        return None
    
    def get_job_by_scheduler_id(self, job_id: str) -> Optional[Job]:
        """Get job by scheduler job ID"""
        with self._connect() as conn:
            row = conn.execute(
                'SELECT * FROM jobs WHERE job_id = ?',
                (job_id,)
            ).fetchone()
            
            if row:
                return self._row_to_job(row)
        return None
    
    def get_jobs_by_state(self, state: JobState) -> List[Job]:
        """Get all jobs with a specific state"""
        with self._connect() as conn:
            rows = conn.execute(
                'SELECT * FROM jobs WHERE state = ?',
                (state.value,)
            ).fetchall()
            
            return [self._row_to_job(row) for row in rows]
    
    def get_all_jobs(self) -> List[Job]:
        """Get all jobs"""
        with self._connect() as conn:
            rows = conn.execute('SELECT * FROM jobs ORDER BY created_at').fetchall()
            return [self._row_to_job(row) for row in rows]
    
    def get_active_jobs(self) -> List[Job]:
        """Get all non-terminal jobs"""
        with self._connect() as conn:
            rows = conn.execute(
                'SELECT * FROM jobs WHERE state IN (?, ?, ?)',
                ('pending', 'submitted', 'running')
            ).fetchall()
            return [self._row_to_job(row) for row in rows]
    
    def delete_job(self, internal_id: str):
        """Delete a job"""
        with self._connect() as conn:
            conn.execute('DELETE FROM jobs WHERE internal_id = ?', (internal_id,))
    
    def clear_all(self):
        """Delete all jobs"""
        with self._connect() as conn:
            conn.execute('DELETE FROM jobs')
    
    def get_stats(self) -> Dict[str, int]:
        """Get job statistics"""
        with self._connect() as conn:
            stats = {}
            for state in JobState:
                count = conn.execute(
                    'SELECT COUNT(*) FROM jobs WHERE state = ?',
                    (state.value,)
                ).fetchone()[0]
                stats[state.value] = count
            
            stats['total'] = conn.execute('SELECT COUNT(*) FROM jobs').fetchone()[0]
            return stats
    
    def _row_to_job(self, row) -> Job:
        """Convert database row to Job object"""
        data = dict(row)
        data['gpu'] = bool(data.get('gpu', 0))
        return Job.from_dict(data)
