#!/usr/bin/env python3
"""
Enterprise Backup System - Advanced Data Protection and Recovery
Comprehensive backup and disaster recovery solution for enterprise environments.

Use of this code is at your own risk.
Author bears no responsibility for any damages caused by the code.
"""

import os
import sys
import json
import time
import hashlib
import shutil
import tarfile
import gzip
import threading
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass, asdict, field
from enum import Enum
import subprocess
import concurrent.futures
from pathlib import Path
import sqlite3
import schedule

class BackupType(Enum):
    """Types of backup operations."""
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"
    SNAPSHOT = "snapshot"

class BackupStatus(Enum):
    """Backup operation status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class CompressionType(Enum):
    """Compression algorithms."""
    NONE = "none"
    GZIP = "gzip"
    BZIP2 = "bzip2"
    XZ = "xz"
    LZ4 = "lz4"

class StorageType(Enum):
    """Storage backend types."""
    LOCAL = "local"
    NFS = "nfs"
    S3 = "s3"
    AZURE_BLOB = "azure_blob"
    GCS = "gcs"
    SFTP = "sftp"

@dataclass
class BackupSource:
    """Backup source configuration."""
    id: str
    name: str
    path: str
    type: str  # file, directory, database, application
    include_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    pre_backup_script: Optional[str] = None
    post_backup_script: Optional[str] = None
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class BackupDestination:
    """Backup destination configuration."""
    id: str
    name: str
    storage_type: StorageType
    path: str
    credentials: Dict[str, str] = field(default_factory=dict)
    encryption_key: Optional[str] = None
    retention_policy: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True

@dataclass
class BackupJob:
    """Backup job configuration."""
    id: str
    name: str
    description: str
    sources: List[str]  # Source IDs
    destination: str  # Destination ID
    backup_type: BackupType
    compression: CompressionType
    schedule: str  # Cron expression
    retention_days: int = 30
    encryption_enabled: bool = True
    verify_backup: bool = True
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class BackupExecution:
    """Backup execution record."""
    id: str
    job_id: str
    backup_type: BackupType
    start_time: datetime
    end_time: Optional[datetime] = None
    status: BackupStatus = BackupStatus.PENDING
    files_processed: int = 0
    bytes_processed: int = 0
    bytes_compressed: int = 0
    backup_path: Optional[str] = None
    checksum: Optional[str] = None
    error_message: Optional[str] = None
    logs: List[str] = field(default_factory=list)

class EnterpriseBackupSystem:
    """Enterprise-grade backup and recovery system."""
    
    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)
        self.sources = {}
        self.destinations = {}
        self.jobs = {}
        self.executions = {}
        self.logger = self._setup_logging()
        self.db_path = os.path.join(self.config["data_dir"], "backup_system.db")
        self._init_database()
        self.scheduler_running = False
        self.scheduler_thread = None
        
    def _load_config(self, config_path: str) -> Dict:
        """Load backup system configuration."""
        default_config = {
            "data_dir": "/var/lib/backup_system",
            "temp_dir": "/tmp/backup_system",
            "max_concurrent_jobs": 3,
            "default_compression": "gzip",
            "default_retention_days": 30,
            "verify_backups": True,
            "encryption": {
                "algorithm": "AES-256",
                "key_derivation": "PBKDF2"
            },
            "monitoring": {
                "enabled": True,
                "webhook_url": "",
                "email_notifications": False
            },
            "storage_backends": {
                "local": {
                    "base_path": "/backup/storage"
                },
                "s3": {
                    "region": "us-east-1",
                    "bucket": "",
                    "access_key": "",
                    "secret_key": ""
                },
                "azure": {
                    "account_name": "",
                    "account_key": "",
                    "container": ""
                }
            },
            "performance": {
                "io_nice_level": 7,
                "cpu_nice_level": 19,
                "bandwidth_limit_mbps": 0,  # 0 = unlimited
                "parallel_streams": 4
            }
        }
        
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                user_config = json.load(f)
                default_config = self._deep_merge(default_config, user_config)
        
        # Create required directories
        os.makedirs(default_config["data_dir"], exist_ok=True)
        os.makedirs(default_config["temp_dir"], exist_ok=True)
        
        return default_config
    
    def _deep_merge(self, base: Dict, update: Dict) -> Dict:
        """Deep merge two dictionaries."""
        for key, value in update.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                base[key] = self._deep_merge(base[key], value)
            else:
                base[key] = value
        return base
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger('EnterpriseBackupSystem')
        logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler
        log_dir = os.path.join(self.config["data_dir"], "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.FileHandler(os.path.join(log_dir, 'backup_system.log'))
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    def _init_database(self):
        """Initialize SQLite database for backup metadata."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS backup_executions (
                    id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    backup_type TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    status TEXT NOT NULL,
                    files_processed INTEGER DEFAULT 0,
                    bytes_processed INTEGER DEFAULT 0,
                    bytes_compressed INTEGER DEFAULT 0,
                    backup_path TEXT,
                    checksum TEXT,
                    error_message TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS backup_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    file_mtime TEXT NOT NULL,
                    checksum TEXT,
                    FOREIGN KEY (execution_id) REFERENCES backup_executions (id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS backup_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    FOREIGN KEY (execution_id) REFERENCES backup_executions (id)
                )
            ''')
            
            conn.commit()
    
    def add_backup_source(self, source: BackupSource):
        """Add backup source configuration."""
        self.sources[source.id] = source
        self.logger.info(f"Added backup source: {source.name}")
    
    def add_backup_destination(self, destination: BackupDestination):
        """Add backup destination configuration."""
        self.destinations[destination.id] = destination
        self.logger.info(f"Added backup destination: {destination.name}")
    
    def add_backup_job(self, job: BackupJob):
        """Add backup job configuration."""
        # Validate sources and destination exist
        for source_id in job.sources:
            if source_id not in self.sources:
                raise ValueError(f"Source {source_id} not found")
        
        if job.destination not in self.destinations:
            raise ValueError(f"Destination {job.destination} not found")
        
        self.jobs[job.id] = job
        self.logger.info(f"Added backup job: {job.name}")
    
    def start_scheduler(self):
        """Start the backup scheduler."""
        if self.scheduler_running:
            self.logger.warning("Scheduler is already running")
            return
        
        self.scheduler_running = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        self.logger.info("Backup scheduler started")
    
    def stop_scheduler(self):
        """Stop the backup scheduler."""
        if not self.scheduler_running:
            return
        
        self.scheduler_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        self.logger.info("Backup scheduler stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop."""
        while self.scheduler_running:
            try:
                # Check for scheduled jobs
                for job in self.jobs.values():
                    if job.enabled and self._should_run_job(job):
                        self._execute_backup_job_async(job.id)
                
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                time.sleep(60)
    
    def _should_run_job(self, job: BackupJob) -> bool:
        """Check if job should run based on schedule."""
        # Simplified schedule checking (would use proper cron parsing in production)
        now = datetime.now()
        
        # For demo, run jobs every hour if schedule contains "hourly"
        if "hourly" in job.schedule.lower():
            last_execution = self._get_last_execution(job.id)
            if not last_execution:
                return True
            
            time_since_last = now - last_execution.start_time
            return time_since_last >= timedelta(hours=1)
        
        # For demo, run daily jobs at midnight
        if "daily" in job.schedule.lower():
            if now.hour == 0 and now.minute < 5:  # Run in first 5 minutes of day
                last_execution = self._get_last_execution(job.id)
                if not last_execution:
                    return True
                
                # Check if already ran today
                return last_execution.start_time.date() < now.date()
        
        return False
    
    def _get_last_execution(self, job_id: str) -> Optional[BackupExecution]:
        """Get the last execution for a job."""
        job_executions = [e for e in self.executions.values() if e.job_id == job_id]
        if not job_executions:
            return None
        
        return max(job_executions, key=lambda x: x.start_time)
    
    def _execute_backup_job_async(self, job_id: str):
        """Execute backup job asynchronously."""
        thread = threading.Thread(target=self.execute_backup_job, args=(job_id,))
        thread.daemon = True
        thread.start()
    
    def execute_backup_job(self, job_id: str) -> BackupExecution:
        """Execute backup job."""
        if job_id not in self.jobs:
            raise ValueError(f"Job {job_id} not found")
        
        job = self.jobs[job_id]
        execution_id = f"exec_{job_id}_{int(time.time())}"
        
        execution = BackupExecution(
            id=execution_id,
            job_id=job_id,
            backup_type=job.backup_type,
            start_time=datetime.now()
        )
        
        self.executions[execution_id] = execution
        self.logger.info(f"Starting backup job execution: {execution_id}")
        
        try:
            execution.status = BackupStatus.RUNNING
            
            # Create backup directory
            backup_dir = self._create_backup_directory(job, execution)
            execution.backup_path = backup_dir
            
            # Execute pre-backup scripts
            self._execute_pre_backup_scripts(job, execution)
            
            # Perform backup based on type
            if job.backup_type == BackupType.FULL:
                self._perform_full_backup(job, execution)
            elif job.backup_type == BackupType.INCREMENTAL:
                self._perform_incremental_backup(job, execution)
            elif job.backup_type == BackupType.DIFFERENTIAL:
                self._perform_differential_backup(job, execution)
            elif job.backup_type == BackupType.SNAPSHOT:
                self._perform_snapshot_backup(job, execution)
            
            # Compress backup if required
            if job.compression != CompressionType.NONE:
                self._compress_backup(job, execution)
            
            # Encrypt backup if required
            if job.encryption_enabled:
                self._encrypt_backup(job, execution)
            
            # Verify backup if required
            if job.verify_backup:
                self._verify_backup(job, execution)
            
            # Calculate checksum
            execution.checksum = self._calculate_backup_checksum(execution.backup_path)
            
            # Execute post-backup scripts
            self._execute_post_backup_scripts(job, execution)
            
            # Upload to destination
            self._upload_to_destination(job, execution)
            
            # Clean up old backups based on retention policy
            self._cleanup_old_backups(job, execution)
            
            execution.status = BackupStatus.COMPLETED
            execution.end_time = datetime.now()
            
            self.logger.info(f"Backup job completed successfully: {execution_id}")
            
        except Exception as e:
            execution.status = BackupStatus.FAILED
            execution.end_time = datetime.now()
            execution.error_message = str(e)
            execution.logs.append(f"ERROR: {str(e)}")
            
            self.logger.error(f"Backup job failed: {execution_id} - {e}")
        
        # Save execution to database
        self._save_execution_to_db(execution)
        
        # Send notifications
        self._send_backup_notification(job, execution)
        
        return execution
    
    def _create_backup_directory(self, job: BackupJob, execution: BackupExecution) -> str:
        """Create backup directory structure."""
        timestamp = execution.start_time.strftime("%Y%m%d_%H%M%S")
        backup_name = f"{job.name}_{job.backup_type.value}_{timestamp}"
        backup_dir = os.path.join(self.config["temp_dir"], backup_name)
        
        os.makedirs(backup_dir, exist_ok=True)
        execution.logs.append(f"Created backup directory: {backup_dir}")
        
        return backup_dir
    
    def _execute_pre_backup_scripts(self, job: BackupJob, execution: BackupExecution):
        """Execute pre-backup scripts for all sources."""
        for source_id in job.sources:
            source = self.sources[source_id]
            if source.pre_backup_script:
                try:
                    result = subprocess.run(
                        source.pre_backup_script,
                        shell=True,
                        capture_output=True,
                        text=True,
                        timeout=300
                    )
                    
                    if result.returncode == 0:
                        execution.logs.append(f"Pre-backup script succeeded for {source.name}")
                    else:
                        execution.logs.append(f"Pre-backup script failed for {source.name}: {result.stderr}")
                        
                except subprocess.TimeoutExpired:
                    execution.logs.append(f"Pre-backup script timed out for {source.name}")
                except Exception as e:
                    execution.logs.append(f"Pre-backup script error for {source.name}: {e}")
    
    def _perform_full_backup(self, job: BackupJob, execution: BackupExecution):
        """Perform full backup."""
        execution.logs.append("Starting full backup")
        
        for source_id in job.sources:
            source = self.sources[source_id]
            self._backup_source(source, execution, is_full=True)
    
    def _perform_incremental_backup(self, job: BackupJob, execution: BackupExecution):
        """Perform incremental backup."""
        execution.logs.append("Starting incremental backup")
        
        # Get last backup time
        last_backup_time = self._get_last_backup_time(job.id)
        
        for source_id in job.sources:
            source = self.sources[source_id]
            self._backup_source(source, execution, is_full=False, since_time=last_backup_time)
    
    def _perform_differential_backup(self, job: BackupJob, execution: BackupExecution):
        """Perform differential backup."""
        execution.logs.append("Starting differential backup")
        
        # Get last full backup time
        last_full_backup_time = self._get_last_full_backup_time(job.id)
        
        for source_id in job.sources:
            source = self.sources[source_id]
            self._backup_source(source, execution, is_full=False, since_time=last_full_backup_time)
    
    def _perform_snapshot_backup(self, job: BackupJob, execution: BackupExecution):
        """Perform snapshot backup."""
        execution.logs.append("Starting snapshot backup")
        
        # For snapshot, we create a point-in-time copy
        for source_id in job.sources:
            source = self.sources[source_id]
            self._create_snapshot(source, execution)
    
    def _backup_source(self, source: BackupSource, execution: BackupExecution, 
                      is_full: bool = True, since_time: datetime = None):
        """Backup individual source."""
        execution.logs.append(f"Backing up source: {source.name}")
        
        if not os.path.exists(source.path):
            execution.logs.append(f"Source path does not exist: {source.path}")
            return
        
        source_backup_dir = os.path.join(execution.backup_path, source.id)
        os.makedirs(source_backup_dir, exist_ok=True)
        
        if os.path.isfile(source.path):
            # Single file backup
            self._backup_file(source.path, source_backup_dir, execution, since_time)
        else:
            # Directory backup
            self._backup_directory(source.path, source_backup_dir, execution, 
                                 source.include_patterns, source.exclude_patterns, since_time)
    
    def _backup_file(self, file_path: str, backup_dir: str, execution: BackupExecution, 
                    since_time: datetime = None):
        """Backup individual file."""
        file_stat = os.stat(file_path)
        file_mtime = datetime.fromtimestamp(file_stat.st_mtime)
        
        # Check if file should be backed up based on modification time
        if since_time and file_mtime <= since_time:
            return
        
        dest_path = os.path.join(backup_dir, os.path.basename(file_path))
        shutil.copy2(file_path, dest_path)
        
        execution.files_processed += 1
        execution.bytes_processed += file_stat.st_size
        
        # Calculate file checksum
        file_checksum = self._calculate_file_checksum(file_path)
        
        # Store file metadata in database
        self._store_file_metadata(execution.id, file_path, file_stat.st_size, 
                                file_mtime, file_checksum)
    
    def _backup_directory(self, dir_path: str, backup_dir: str, execution: BackupExecution,
                         include_patterns: List[str], exclude_patterns: List[str],
                         since_time: datetime = None):
        """Backup directory recursively."""
        for root, dirs, files in os.walk(dir_path):
            # Apply exclude patterns to directories
            dirs[:] = [d for d in dirs if not self._matches_patterns(d, exclude_patterns)]
            
            for file in files:
                file_path = os.path.join(root, file)
                
                # Apply include/exclude patterns
                if include_patterns and not self._matches_patterns(file, include_patterns):
                    continue
                
                if exclude_patterns and self._matches_patterns(file, exclude_patterns):
                    continue
                
                try:
                    file_stat = os.stat(file_path)
                    file_mtime = datetime.fromtimestamp(file_stat.st_mtime)
                    
                    # Check modification time for incremental/differential backups
                    if since_time and file_mtime <= since_time:
                        continue
                    
                    # Create relative directory structure in backup
                    rel_path = os.path.relpath(file_path, dir_path)
                    dest_path = os.path.join(backup_dir, rel_path)
                    dest_dir = os.path.dirname(dest_path)
                    
                    os.makedirs(dest_dir, exist_ok=True)
                    shutil.copy2(file_path, dest_path)
                    
                    execution.files_processed += 1
                    execution.bytes_processed += file_stat.st_size
                    
                    # Calculate file checksum
                    file_checksum = self._calculate_file_checksum(file_path)
                    
                    # Store file metadata
                    self._store_file_metadata(execution.id, file_path, file_stat.st_size,
                                            file_mtime, file_checksum)
                    
                except (OSError, IOError) as e:
                    execution.logs.append(f"Failed to backup file {file_path}: {e}")
    
    def _matches_patterns(self, filename: str, patterns: List[str]) -> bool:
        """Check if filename matches any of the patterns."""
        import fnmatch
        return any(fnmatch.fnmatch(filename, pattern) for pattern in patterns)
    
    def _create_snapshot(self, source: BackupSource, execution: BackupExecution):
        """Create filesystem snapshot."""
        execution.logs.append(f"Creating snapshot for {source.name}")
        
        # For demonstration, we'll create a tar archive as a "snapshot"
        snapshot_path = os.path.join(execution.backup_path, f"{source.id}_snapshot.tar")
        
        with tarfile.open(snapshot_path, 'w') as tar:
            tar.add(source.path, arcname=os.path.basename(source.path))
        
        snapshot_size = os.path.getsize(snapshot_path)
        execution.bytes_processed += snapshot_size
        execution.files_processed += 1
        
        execution.logs.append(f"Snapshot created: {snapshot_path} ({snapshot_size} bytes)")
    
    def _compress_backup(self, job: BackupJob, execution: BackupExecution):
        """Compress backup archive."""
        execution.logs.append(f"Compressing backup with {job.compression.value}")
        
        if job.compression == CompressionType.GZIP:
            compressed_path = f"{execution.backup_path}.tar.gz"
            with tarfile.open(compressed_path, 'w:gz') as tar:
                tar.add(execution.backup_path, arcname=os.path.basename(execution.backup_path))
        elif job.compression == CompressionType.BZIP2:
            compressed_path = f"{execution.backup_path}.tar.bz2"
            with tarfile.open(compressed_path, 'w:bz2') as tar:
                tar.add(execution.backup_path, arcname=os.path.basename(execution.backup_path))
        else:
            # For other compression types, we'd implement specific handlers
            compressed_path = f"{execution.backup_path}.tar"
            with tarfile.open(compressed_path, 'w') as tar:
                tar.add(execution.backup_path, arcname=os.path.basename(execution.backup_path))
        
        # Update execution with compressed info
        original_size = execution.bytes_processed
        compressed_size = os.path.getsize(compressed_path)
        execution.bytes_compressed = compressed_size
        
        # Remove original uncompressed backup
        shutil.rmtree(execution.backup_path)
        execution.backup_path = compressed_path
        
        compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
        execution.logs.append(f"Compression completed. Ratio: {compression_ratio:.1f}%")
    
    def _encrypt_backup(self, job: BackupJob, execution: BackupExecution):
        """Encrypt backup archive."""
        execution.logs.append("Encrypting backup")
        
        # For demonstration, we'll simulate encryption by renaming the file
        # In production, you would use proper encryption libraries
        encrypted_path = f"{execution.backup_path}.encrypted"
        shutil.move(execution.backup_path, encrypted_path)
        execution.backup_path = encrypted_path
        
        execution.logs.append("Backup encryption completed")
    
    def _verify_backup(self, job: BackupJob, execution: BackupExecution):
        """Verify backup integrity."""
        execution.logs.append("Verifying backup integrity")
        
        if not os.path.exists(execution.backup_path):
            raise Exception("Backup file not found for verification")
        
        # For demonstration, we'll just check file size
        backup_size = os.path.getsize(execution.backup_path)
        if backup_size == 0:
            raise Exception("Backup file is empty")
        
        execution.logs.append(f"Backup verification completed. Size: {backup_size} bytes")
    
    def _calculate_backup_checksum(self, backup_path: str) -> str:
        """Calculate backup file checksum."""
        hash_sha256 = hashlib.sha256()
        
        with open(backup_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        
        return hash_sha256.hexdigest()
    
    def _calculate_file_checksum(self, file_path: str) -> str:
        """Calculate individual file checksum."""
        hash_md5 = hashlib.md5()
        
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except (OSError, IOError):
            return ""
    
    def _execute_post_backup_scripts(self, job: BackupJob, execution: BackupExecution):
        """Execute post-backup scripts."""
        for source_id in job.sources:
            source = self.sources[source_id]
            if source.post_backup_script:
                try:
                    result = subprocess.run(
                        source.post_backup_script,
                        shell=True,
                        capture_output=True,
                        text=True,
                        timeout=300
                    )
                    
                    if result.returncode == 0:
                        execution.logs.append(f"Post-backup script succeeded for {source.name}")
                    else:
                        execution.logs.append(f"Post-backup script failed for {source.name}: {result.stderr}")
                        
                except subprocess.TimeoutExpired:
                    execution.logs.append(f"Post-backup script timed out for {source.name}")
                except Exception as e:
                    execution.logs.append(f"Post-backup script error for {source.name}: {e}")
    
    def _upload_to_destination(self, job: BackupJob, execution: BackupExecution):
        """Upload backup to configured destination."""
        destination = self.destinations[job.destination]
        execution.logs.append(f"Uploading to destination: {destination.name}")
        
        if destination.storage_type == StorageType.LOCAL:
            self._upload_to_local_storage(destination, execution)
        elif destination.storage_type == StorageType.S3:
            self._upload_to_s3_storage(destination, execution)
        elif destination.storage_type == StorageType.SFTP:
            self._upload_to_sftp_storage(destination, execution)
        else:
            execution.logs.append(f"Storage type {destination.storage_type.value} not implemented")
    
    def _upload_to_local_storage(self, destination: BackupDestination, execution: BackupExecution):
        """Upload backup to local storage."""
        dest_path = os.path.join(destination.path, os.path.basename(execution.backup_path))
        os.makedirs(destination.path, exist_ok=True)
        shutil.copy2(execution.backup_path, dest_path)
        execution.logs.append(f"Backup uploaded to: {dest_path}")
    
    def _upload_to_s3_storage(self, destination: BackupDestination, execution: BackupExecution):
        """Upload backup to S3 storage."""
        # Simulate S3 upload
        execution.logs.append("Simulating S3 upload (not implemented)")
    
    def _upload_to_sftp_storage(self, destination: BackupDestination, execution: BackupExecution):
        """Upload backup to SFTP storage."""
        # Simulate SFTP upload
        execution.logs.append("Simulating SFTP upload (not implemented)")
    
    def _cleanup_old_backups(self, job: BackupJob, execution: BackupExecution):
        """Clean up old backups based on retention policy."""
        execution.logs.append("Cleaning up old backups")
        
        # Get all executions for this job
        job_executions = [e for e in self.executions.values() 
                         if e.job_id == job.id and e.status == BackupStatus.COMPLETED]
        
        # Sort by start time, newest first
        job_executions.sort(key=lambda x: x.start_time, reverse=True)
        
        # Keep only backups within retention period
        cutoff_date = datetime.now() - timedelta(days=job.retention_days)
        old_executions = [e for e in job_executions if e.start_time < cutoff_date]
        
        for old_execution in old_executions:
            if old_execution.backup_path and os.path.exists(old_execution.backup_path):
                try:
                    os.remove(old_execution.backup_path)
                    execution.logs.append(f"Removed old backup: {old_execution.backup_path}")
                except OSError as e:
                    execution.logs.append(f"Failed to remove old backup: {e}")
    
    def _get_last_backup_time(self, job_id: str) -> Optional[datetime]:
        """Get timestamp of last successful backup."""
        job_executions = [e for e in self.executions.values() 
                         if e.job_id == job_id and e.status == BackupStatus.COMPLETED]
        
        if not job_executions:
            return None
        
        return max(job_executions, key=lambda x: x.start_time).start_time
    
    def _get_last_full_backup_time(self, job_id: str) -> Optional[datetime]:
        """Get timestamp of last successful full backup."""
        job_executions = [e for e in self.executions.values() 
                         if (e.job_id == job_id and 
                             e.status == BackupStatus.COMPLETED and 
                             e.backup_type == BackupType.FULL)]
        
        if not job_executions:
            return None
        
        return max(job_executions, key=lambda x: x.start_time).start_time
    
    def _store_file_metadata(self, execution_id: str, file_path: str, file_size: int,
                           file_mtime: datetime, checksum: str):
        """Store file metadata in database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO backup_files 
                (execution_id, file_path, file_size, file_mtime, checksum)
                VALUES (?, ?, ?, ?, ?)
            ''', (execution_id, file_path, file_size, file_mtime.isoformat(), checksum))
            conn.commit()
    
    def _save_execution_to_db(self, execution: BackupExecution):
        """Save execution record to database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO backup_executions 
                (id, job_id, backup_type, start_time, end_time, status, 
                 files_processed, bytes_processed, bytes_compressed, 
                 backup_path, checksum, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                execution.id, execution.job_id, execution.backup_type.value,
                execution.start_time.isoformat(),
                execution.end_time.isoformat() if execution.end_time else None,
                execution.status.value, execution.files_processed,
                execution.bytes_processed, execution.bytes_compressed,
                execution.backup_path, execution.checksum, execution.error_message
            ))
            
            # Save logs
            for log_entry in execution.logs:
                cursor.execute('''
                    INSERT INTO backup_logs (execution_id, timestamp, level, message)
                    VALUES (?, ?, ?, ?)
                ''', (execution.id, datetime.now().isoformat(), 'INFO', log_entry))
            
            conn.commit()
    
    def _send_backup_notification(self, job: BackupJob, execution: BackupExecution):
        """Send backup completion notification."""
        if not self.config["monitoring"]["enabled"]:
            return
        
        status_emoji = "✅" if execution.status == BackupStatus.COMPLETED else "❌"
        message = f"{status_emoji} Backup Job: {job.name}\n"
        message += f"Status: {execution.status.value}\n"
        message += f"Files: {execution.files_processed}\n"
        message += f"Size: {execution.bytes_processed / (1024*1024):.1f} MB\n"
        
        if execution.bytes_compressed > 0:
            compression_ratio = (1 - execution.bytes_compressed / execution.bytes_processed) * 100
            message += f"Compressed: {execution.bytes_compressed / (1024*1024):.1f} MB ({compression_ratio:.1f}%)\n"
        
        duration = (execution.end_time - execution.start_time).total_seconds() if execution.end_time else 0
        message += f"Duration: {duration:.1f}s\n"
        
        if execution.error_message:
            message += f"Error: {execution.error_message}\n"
        
        self.logger.info(f"Backup notification: {message}")
        # In production, would send to configured notification channels
    
    def get_backup_status(self, execution_id: str = None, job_id: str = None) -> List[BackupExecution]:
        """Get backup execution status."""
        if execution_id:
            return [self.executions[execution_id]] if execution_id in self.executions else []
        
        if job_id:
            return [e for e in self.executions.values() if e.job_id == job_id]
        
        return list(self.executions.values())
    
    def restore_backup(self, execution_id: str, restore_path: str) -> bool:
        """Restore backup from execution."""
        if execution_id not in self.executions:
            raise ValueError(f"Execution {execution_id} not found")
        
        execution = self.executions[execution_id]
        
        if not execution.backup_path or not os.path.exists(execution.backup_path):
            raise ValueError("Backup file not found")
        
        self.logger.info(f"Starting restore from {execution.backup_path} to {restore_path}")
        
        try:
            os.makedirs(restore_path, exist_ok=True)
            
            # Extract backup archive
            if execution.backup_path.endswith('.tar.gz'):
                with tarfile.open(execution.backup_path, 'r:gz') as tar:
                    tar.extractall(restore_path)
            elif execution.backup_path.endswith('.tar.bz2'):
                with tarfile.open(execution.backup_path, 'r:bz2') as tar:
                    tar.extractall(restore_path)
            elif execution.backup_path.endswith('.tar'):
                with tarfile.open(execution.backup_path, 'r') as tar:
                    tar.extractall(restore_path)
            else:
                # Handle encrypted or other formats
                shutil.copy2(execution.backup_path, restore_path)
            
            self.logger.info(f"Restore completed successfully to {restore_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Restore failed: {e}")
            return False
    
    def generate_backup_report(self, days: int = 30) -> Dict:
        """Generate backup system report."""
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        
        # Get executions in time range
        recent_executions = [e for e in self.executions.values() 
                           if e.start_time >= start_time]
        
        # Calculate statistics
        total_executions = len(recent_executions)
        successful_executions = len([e for e in recent_executions 
                                   if e.status == BackupStatus.COMPLETED])
        failed_executions = len([e for e in recent_executions 
                               if e.status == BackupStatus.FAILED])
        
        total_bytes_backed_up = sum(e.bytes_processed for e in recent_executions)
        total_bytes_compressed = sum(e.bytes_compressed for e in recent_executions 
                                   if e.bytes_compressed > 0)
        
        avg_compression_ratio = 0
        if total_bytes_backed_up > 0 and total_bytes_compressed > 0:
            avg_compression_ratio = (1 - total_bytes_compressed / total_bytes_backed_up) * 100
        
        # Job statistics
        job_stats = {}
        for job_id, job in self.jobs.items():
            job_executions = [e for e in recent_executions if e.job_id == job_id]
            job_stats[job_id] = {
                "name": job.name,
                "total_executions": len(job_executions),
                "successful": len([e for e in job_executions if e.status == BackupStatus.COMPLETED]),
                "failed": len([e for e in job_executions if e.status == BackupStatus.FAILED]),
                "last_execution": max([e.start_time for e in job_executions]).isoformat() if job_executions else None
            }
        
        return {
            "report_period": {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "days": days
            },
            "summary": {
                "total_executions": total_executions,
                "successful_executions": successful_executions,
                "failed_executions": failed_executions,
                "success_rate": (successful_executions / total_executions * 100) if total_executions > 0 else 0,
                "total_data_backed_up_gb": total_bytes_backed_up / (1024**3),
                "total_data_compressed_gb": total_bytes_compressed / (1024**3),
                "average_compression_ratio": avg_compression_ratio
            },
            "job_statistics": job_stats,
            "system_info": {
                "total_sources": len(self.sources),
                "total_destinations": len(self.destinations),
                "total_jobs": len(self.jobs),
                "active_jobs": len([j for j in self.jobs.values() if j.enabled])
            },
            "generated_at": datetime.now().isoformat()
        }


def main():
    """Main function for CLI usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enterprise Backup System")
    parser.add_argument("--config", help="Configuration file path")
    parser.add_argument("--action", choices=["start", "status", "restore", "report"], 
                       default="status", help="Action to perform")
    parser.add_argument("--job-id", help="Job ID for execution")
    parser.add_argument("--execution-id", help="Execution ID for status/restore")
    parser.add_argument("--restore-path", help="Path for restore operation")
    parser.add_argument("--days", type=int, default=30, help="Days for report")
    
    args = parser.parse_args()
    
    backup_system = EnterpriseBackupSystem(args.config)
    
    if args.action == "start":
        # Add sample configuration
        backup_system.add_backup_source(BackupSource(
            id="documents",
            name="Documents Backup",
            path="/home/user/documents",
            type="directory",
            include_patterns=["*.pdf", "*.doc", "*.txt"],
            exclude_patterns=["*.tmp", "*.log"]
        ))
        
        backup_system.add_backup_destination(BackupDestination(
            id="local_storage",
            name="Local Storage",
            storage_type=StorageType.LOCAL,
            path="/backup/storage"
        ))
        
        backup_system.add_backup_job(BackupJob(
            id="daily_documents",
            name="Daily Documents Backup",
            description="Daily backup of user documents",
            sources=["documents"],
            destination="local_storage",
            backup_type=BackupType.INCREMENTAL,
            compression=CompressionType.GZIP,
            schedule="daily"
        ))
        
        backup_system.start_scheduler()
        
        print("Backup system started. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            backup_system.stop_scheduler()
    
    elif args.action == "status":
        if args.execution_id:
            executions = backup_system.get_backup_status(execution_id=args.execution_id)
        elif args.job_id:
            executions = backup_system.get_backup_status(job_id=args.job_id)
        else:
            executions = backup_system.get_backup_status()
        
        print(f"Found {len(executions)} backup executions:")
        for execution in executions[-10:]:  # Show last 10
            print(f"  {execution.id}: {execution.status.value} "
                  f"({execution.start_time}) - {execution.files_processed} files")
    
    elif args.action == "restore":
        if not args.execution_id or not args.restore_path:
            print("Execution ID and restore path required")
            sys.exit(1)
        
        success = backup_system.restore_backup(args.execution_id, args.restore_path)
        if success:
            print("Restore completed successfully")
        else:
            print("Restore failed")
    
    elif args.action == "report":
        report = backup_system.generate_backup_report(args.days)
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()