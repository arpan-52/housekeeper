"""
SQLite database for persistent job storage
"""

import sqlite3
import json
import threading
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime

from .job import job, job_status, job_resources, failure_type


class database:
    """SQLite database manager for job storage"""
    
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.lock = threading.Lock()
        self._init_db()
    
    def _init_db(self):
        """Initialize database tables"""
        with self.lock:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            # Main jobs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    command TEXT NOT NULL,
                    workdir TEXT NOT NULL,
                    script_path TEXT,
                    scheduler_id TEXT,
                    
                    -- Resources (JSON)
                    resources TEXT NOT NULL,
                    
                    -- Status
                    status TEXT DEFAULT 'pending',
                    exit_code INTEGER,
                    
                    -- Paths
                    stdout_path TEXT,
                    stderr_path TEXT,
                    
                    -- Files (JSON arrays)
                    expected_files TEXT DEFAULT '[]',
                    
                    -- Timestamps
                    created_at TIMESTAMP NOT NULL,
                    submitted_at TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    
                    -- Failure info
                    failure_type TEXT,
                    failure_reason TEXT,
                    error_lines TEXT DEFAULT '[]',
                    
                    -- Retry
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 0,
                    parent_job_id TEXT,
                    
                    -- Environment (JSON)
                    env TEXT DEFAULT '{}'
                )
            """)
            
            # Dependencies table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dependencies (
                    job_id TEXT NOT NULL,
                    depends_on TEXT NOT NULL,
                    dep_type TEXT DEFAULT 'after_ok',
                    PRIMARY KEY (job_id, depends_on, dep_type),
                    FOREIGN KEY (job_id) REFERENCES jobs(id),
                    FOREIGN KEY (depends_on) REFERENCES jobs(id)
                )
            """)
            
            # Create indices
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON jobs(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_scheduler_id ON jobs(scheduler_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON jobs(created_at)")
            
            conn.commit()
            conn.close()
    
    def add_job(self, j: job):
        """Add a new job to database"""
        with self.lock:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO jobs VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, (
                j.id,
                j.name,
                j.command,
                j.workdir,
                j.script_path,
                j.scheduler_id,
                json.dumps(j.resources.to_dict()),
                j.status.value,
                j.exit_code,
                j.stdout_path,
                j.stderr_path,
                json.dumps(j.expected_files),
                j.created_at.isoformat() if j.created_at else None,
                j.submitted_at.isoformat() if j.submitted_at else None,
                j.started_at.isoformat() if j.started_at else None,
                j.completed_at.isoformat() if j.completed_at else None,
                j.failure_type.value if j.failure_type else None,
                j.failure_reason,
                json.dumps(j.error_lines),
                j.retry_count,
                j.max_retries,
                j.parent_job_id,
                json.dumps(j.env),
            ))
            
            # Add dependencies
            for dep_id in j.after_ok:
                cursor.execute(
                    "INSERT OR IGNORE INTO dependencies VALUES (?, ?, ?)",
                    (j.id, dep_id, "after_ok")
                )
            
            for dep_id in j.after_fail:
                cursor.execute(
                    "INSERT OR IGNORE INTO dependencies VALUES (?, ?, ?)",
                    (j.id, dep_id, "after_fail")
                )
            
            for dep_id in j.after_any:
                cursor.execute(
                    "INSERT OR IGNORE INTO dependencies VALUES (?, ?, ?)",
                    (j.id, dep_id, "after_any")
                )
            
            conn.commit()
            conn.close()
    
    def get_job(self, job_id: str) -> Optional[job]:
        """Get a job by ID"""
        with self.lock:
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            
            if not row:
                conn.close()
                return None
            
            # Get dependencies
            cursor.execute(
                "SELECT depends_on, dep_type FROM dependencies WHERE job_id = ?",
                (job_id,)
            )
            deps = cursor.fetchall()
            
            conn.close()
            
            return self._row_to_job(row, deps)
    
    def _row_to_job(self, row, deps) -> job:
        """Convert database row to job object"""
        resources = job_resources.from_dict(json.loads(row['resources']))
        
        # Parse dependencies
        after_ok = [d['depends_on'] for d in deps if d['dep_type'] == 'after_ok']
        after_fail = [d['depends_on'] for d in deps if d['dep_type'] == 'after_fail']
        after_any = [d['depends_on'] for d in deps if d['dep_type'] == 'after_any']
        
        return job(
            id=row['id'],
            name=row['name'],
            command=row['command'],
            workdir=row['workdir'],
            resources=resources,
            script_path=row['script_path'],
            scheduler_id=row['scheduler_id'],
            status=job_status(row['status']),
            exit_code=row['exit_code'],
            stdout_path=row['stdout_path'],
            stderr_path=row['stderr_path'],
            expected_files=json.loads(row['expected_files'] or '[]'),
            after_ok=after_ok,
            after_fail=after_fail,
            after_any=after_any,
            created_at=datetime.fromisoformat(row['created_at']) if row['created_at'] else None,
            submitted_at=datetime.fromisoformat(row['submitted_at']) if row['submitted_at'] else None,
            started_at=datetime.fromisoformat(row['started_at']) if row['started_at'] else None,
            completed_at=datetime.fromisoformat(row['completed_at']) if row['completed_at'] else None,
            failure_type=failure_type(row['failure_type']) if row['failure_type'] else None,
            failure_reason=row['failure_reason'],
            error_lines=json.loads(row['error_lines'] or '[]'),
            retry_count=row['retry_count'],
            max_retries=row['max_retries'],
            parent_job_id=row['parent_job_id'],
            env=json.loads(row['env'] or '{}'),
        )
    
    def update_job(self, job_id: str, **kwargs):
        """Update job fields"""
        with self.lock:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            updates = []
            params = []
            
            for key, value in kwargs.items():
                if key in ['status', 'scheduler_id', 'exit_code', 'script_path',
                          'stdout_path', 'stderr_path', 'submitted_at', 'started_at',
                          'completed_at', 'failure_type', 'failure_reason', 'retry_count']:
                    updates.append(f"{key} = ?")
                    if key == 'status' and hasattr(value, 'value'):
                        params.append(value.value)
                    elif key == 'failure_type' and hasattr(value, 'value'):
                        params.append(value.value)
                    elif key in ['submitted_at', 'started_at', 'completed_at']:
                        params.append(value.isoformat() if value else None)
                    else:
                        params.append(value)
                elif key == 'error_lines':
                    updates.append("error_lines = ?")
                    params.append(json.dumps(value))
            
            if updates:
                params.append(job_id)
                query = f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?"
                cursor.execute(query, params)
            
            conn.commit()
            conn.close()
    
    def list_jobs(self, status: Optional[str] = None, limit: Optional[int] = None) -> List[job]:
        """List all jobs, optionally filtered by status"""
        with self.lock:
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = "SELECT * FROM jobs"
            params = []
            
            if status:
                query += " WHERE status = ?"
                params.append(status)
            
            query += " ORDER BY created_at DESC"
            
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Get all job IDs to fetch their dependencies
            job_ids = [row['id'] for row in rows]
            
            # Fetch all dependencies in one query
            if job_ids:
                placeholders = ','.join('?' * len(job_ids))
                cursor.execute(
                    f"SELECT job_id, depends_on, dep_type FROM dependencies WHERE job_id IN ({placeholders})",
                    job_ids
                )
                all_deps = cursor.fetchall()
                
                # Group dependencies by job_id
                deps_by_job = {}
                for dep in all_deps:
                    job_id = dep['job_id']
                    if job_id not in deps_by_job:
                        deps_by_job[job_id] = []
                    deps_by_job[job_id].append(dep)
            else:
                deps_by_job = {}
            
            conn.close()
            
            jobs = []
            for row in rows:
                deps = deps_by_job.get(row['id'], [])
                jobs.append(self._row_to_job(row, deps))
            
            return jobs
    
    def delete_job(self, job_id: str):
        """Delete a job and its dependencies"""
        with self.lock:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM dependencies WHERE job_id = ?", (job_id,))
            cursor.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
            
            conn.commit()
            conn.close()
    
    def get_dependents(self, job_id: str) -> List[str]:
        """Get jobs that depend on this job"""
        with self.lock:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT DISTINCT job_id FROM dependencies WHERE depends_on = ?",
                (job_id,)
            )
            results = [row[0] for row in cursor.fetchall()]
            
            conn.close()
            return results
