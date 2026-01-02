#!/usr/bin/env python3
"""
Enterprise Database Manager - Multi-Database Operations Platform
Advanced database management system supporting multiple database engines.

Use of this code is at your own risk.
Author bears no responsibility for any damages caused by the code.
"""

import os
import sys
import json
import time
import asyncio
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Callable, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
import psycopg2
import pymongo
import redis
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, DateTime, Text, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import QueuePool

class DatabaseType(Enum):
    """Supported database types."""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    MONGODB = "mongodb"
    REDIS = "redis"
    ORACLE = "oracle"
    MSSQL = "mssql"

class OperationType(Enum):
    """Database operation types."""
    SELECT = "select"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    CREATE_TABLE = "create_table"
    DROP_TABLE = "drop_table"
    CREATE_INDEX = "create_index"
    BACKUP = "backup"
    RESTORE = "restore"
    MIGRATE = "migrate"

class ConnectionStatus(Enum):
    """Database connection status."""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    ERROR = "error"
    TIMEOUT = "timeout"

@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    id: str
    name: str
    type: DatabaseType
    host: str
    port: int
    database: str
    username: str
    password: str
    ssl_enabled: bool = False
    connection_timeout: int = 30
    max_connections: int = 20
    pool_size: int = 10
    pool_timeout: int = 30
    options: Dict[str, Any] = field(default_factory=dict)

@dataclass
class QueryResult:
    """Database query result."""
    success: bool
    rows_affected: int
    execution_time_ms: float
    data: List[Dict[str, Any]] = field(default_factory=list)
    error_message: Optional[str] = None
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.now)

@dataclass
class BackupResult:
    """Database backup operation result."""
    success: bool
    backup_id: str
    file_path: str
    file_size_mb: float
    duration_seconds: float
    timestamp: datetime
    error_message: Optional[str] = None
    compression_ratio: float = 0.0

@dataclass
class MigrationScript:
    """Database migration script."""
    id: str
    name: str
    version: str
    up_script: str
    down_script: str
    dependencies: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)

class DatabaseConnection:
    """Database connection wrapper."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        self.engine = None
        self.session_maker = None
        self.status = ConnectionStatus.DISCONNECTED
        self.last_error = None
        self.logger = logging.getLogger(f'DatabaseConnection-{config.name}')
        
    async def connect(self) -> bool:
        """Establish database connection."""
        try:
            self.logger.info(f"Connecting to {self.config.type.value} database: {self.config.name}")
            
            if self.config.type == DatabaseType.POSTGRESQL:
                await self._connect_postgresql()
            elif self.config.type == DatabaseType.MYSQL:
                await self._connect_mysql()
            elif self.config.type == DatabaseType.SQLITE:
                await self._connect_sqlite()
            elif self.config.type == DatabaseType.MONGODB:
                await self._connect_mongodb()
            elif self.config.type == DatabaseType.REDIS:
                await self._connect_redis()
            else:
                raise NotImplementedError(f"Database type {self.config.type.value} not implemented")
            
            self.status = ConnectionStatus.CONNECTED
            self.logger.info(f"Successfully connected to database: {self.config.name}")
            return True
            
        except Exception as e:
            self.status = ConnectionStatus.ERROR
            self.last_error = str(e)
            self.logger.error(f"Failed to connect to database {self.config.name}: {e}")
            return False
    
    async def _connect_postgresql(self):
        """Connect to PostgreSQL database."""
        connection_string = (
            f"postgresql://{self.config.username}:{self.config.password}@"
            f"{self.config.host}:{self.config.port}/{self.config.database}"
        )
        
        if self.config.ssl_enabled:
            connection_string += "?sslmode=require"
        
        self.engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_connections - self.config.pool_size,
            pool_timeout=self.config.pool_timeout,
            pool_pre_ping=True
        )
        
        self.session_maker = sessionmaker(bind=self.engine)
        
        # Test connection
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    
    async def _connect_mysql(self):
        """Connect to MySQL database."""
        connection_string = (
            f"mysql+pymysql://{self.config.username}:{self.config.password}@"
            f"{self.config.host}:{self.config.port}/{self.config.database}"
        )
        
        self.engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_connections - self.config.pool_size,
            pool_timeout=self.config.pool_timeout,
            pool_pre_ping=True
        )
        
        self.session_maker = sessionmaker(bind=self.engine)
        
        # Test connection
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    
    async def _connect_sqlite(self):
        """Connect to SQLite database."""
        db_path = f"{self.config.host}/{self.config.database}.db"
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        connection_string = f"sqlite:///{db_path}"
        
        self.engine = create_engine(
            connection_string,
            pool_timeout=self.config.pool_timeout,
            connect_args={"check_same_thread": False}
        )
        
        self.session_maker = sessionmaker(bind=self.engine)
        
        # Test connection
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    
    async def _connect_mongodb(self):
        """Connect to MongoDB database."""
        connection_string = f"mongodb://{self.config.username}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.database}"
        
        self.connection = pymongo.MongoClient(
            connection_string,
            serverSelectionTimeoutMS=self.config.connection_timeout * 1000,
            maxPoolSize=self.config.max_connections
        )
        
        # Test connection
        self.connection.admin.command('ping')
    
    async def _connect_redis(self):
        """Connect to Redis database."""
        self.connection = redis.Redis(
            host=self.config.host,
            port=self.config.port,
            password=self.config.password,
            db=int(self.config.database),
            socket_timeout=self.config.connection_timeout,
            socket_connect_timeout=self.config.connection_timeout,
            max_connections=self.config.max_connections
        )
        
        # Test connection
        self.connection.ping()
    
    async def disconnect(self):
        """Close database connection."""
        try:
            if self.engine:
                self.engine.dispose()
            if self.connection:
                self.connection.close()
            
            self.status = ConnectionStatus.DISCONNECTED
            self.logger.info(f"Disconnected from database: {self.config.name}")
            
        except Exception as e:
            self.logger.error(f"Error disconnecting from database {self.config.name}: {e}")
    
    async def execute_query(self, query: str, parameters: Dict = None) -> QueryResult:
        """Execute database query."""
        start_time = time.time()
        result = QueryResult(success=False, rows_affected=0, execution_time_ms=0)
        
        try:
            if self.config.type in [DatabaseType.POSTGRESQL, DatabaseType.MYSQL, DatabaseType.SQLITE]:
                result = await self._execute_sql_query(query, parameters)
            elif self.config.type == DatabaseType.MONGODB:
                result = await self._execute_mongo_query(query, parameters)
            elif self.config.type == DatabaseType.REDIS:
                result = await self._execute_redis_query(query, parameters)
            
        except Exception as e:
            result.error_message = str(e)
            self.logger.error(f"Query execution failed: {e}")
        
        finally:
            result.execution_time_ms = (time.time() - start_time) * 1000
        
        return result
    
    async def _execute_sql_query(self, query: str, parameters: Dict = None) -> QueryResult:
        """Execute SQL query."""
        result = QueryResult(success=False, rows_affected=0, execution_time_ms=0)
        
        with self.engine.connect() as conn:
            if parameters:
                db_result = conn.execute(text(query), parameters)
            else:
                db_result = conn.execute(text(query))
            
            # Handle different query types
            if query.strip().upper().startswith('SELECT'):
                result.data = [dict(row._mapping) for row in db_result]
                result.rows_affected = len(result.data)
            else:
                result.rows_affected = db_result.rowcount
            
            result.success = True
        
        return result
    
    async def _execute_mongo_query(self, operation: str, parameters: Dict = None) -> QueryResult:
        """Execute MongoDB operation."""
        result = QueryResult(success=False, rows_affected=0, execution_time_ms=0)
        
        db = self.connection[self.config.database]
        
        # Parse operation (simplified)
        if operation.startswith('find'):
            collection_name = parameters.get('collection', 'default')
            query = parameters.get('query', {})
            cursor = db[collection_name].find(query)
            result.data = list(cursor)
            result.rows_affected = len(result.data)
        elif operation.startswith('insert'):
            collection_name = parameters.get('collection', 'default')
            document = parameters.get('document', {})
            db[collection_name].insert_one(document)
            result.rows_affected = 1
        
        result.success = True
        return result
    
    async def _execute_redis_query(self, command: str, parameters: Dict = None) -> QueryResult:
        """Execute Redis command."""
        result = QueryResult(success=False, rows_affected=0, execution_time_ms=0)
        
        # Parse command (simplified)
        parts = command.split()
        cmd = parts[0].upper()
        
        if cmd == 'GET':
            key = parts[1] if len(parts) > 1 else parameters.get('key')
            value = self.connection.get(key)
            result.data = [{'key': key, 'value': value}]
        elif cmd == 'SET':
            key = parts[1] if len(parts) > 1 else parameters.get('key')
            value = parts[2] if len(parts) > 2 else parameters.get('value')
            self.connection.set(key, value)
            result.rows_affected = 1
        
        result.success = True
        return result

class EnterpriseDatabaseManager:
    """Enterprise database management system."""
    
    def __init__(self, config_path: str = None):
        self.connections = {}
        self.migration_history = {}
        self.backup_history = {}
        self.query_cache = {}
        self.logger = self._setup_logging()
        self.executor = ThreadPoolExecutor(max_workers=20)
        self.config = self._load_config(config_path)
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger('EnterpriseDatabaseManager')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def _load_config(self, config_path: str) -> Dict:
        """Load database manager configuration."""
        default_config = {
            "cache_enabled": True,
            "cache_ttl_seconds": 300,
            "backup_retention_days": 30,
            "migration_timeout_seconds": 3600,
            "query_timeout_seconds": 300,
            "connection_pool_size": 10,
            "max_concurrent_operations": 50
        }
        
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                user_config = json.load(f)
                default_config.update(user_config)
        
        return default_config
    
    async def add_database(self, config: DatabaseConfig) -> bool:
        """Add database connection to manager."""
        try:
            connection = DatabaseConnection(config)
            
            if await connection.connect():
                self.connections[config.id] = connection
                self.logger.info(f"Added database connection: {config.name}")
                return True
            else:
                self.logger.error(f"Failed to add database connection: {config.name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error adding database {config.name}: {e}")
            return False
    
    async def remove_database(self, database_id: str) -> bool:
        """Remove database connection from manager."""
        try:
            if database_id in self.connections:
                await self.connections[database_id].disconnect()
                del self.connections[database_id]
                self.logger.info(f"Removed database connection: {database_id}")
                return True
            else:
                self.logger.warning(f"Database connection not found: {database_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error removing database {database_id}: {e}")
            return False
    
    async def execute_query(self, database_id: str, query: str, 
                          parameters: Dict = None, use_cache: bool = True) -> QueryResult:
        """Execute query on specified database."""
        if database_id not in self.connections:
            return QueryResult(
                success=False,
                rows_affected=0,
                execution_time_ms=0,
                error_message=f"Database connection not found: {database_id}"
            )
        
        # Check cache for SELECT queries
        if use_cache and self.config["cache_enabled"] and query.strip().upper().startswith('SELECT'):
            cache_key = self._generate_cache_key(database_id, query, parameters)
            cached_result = self._get_cached_result(cache_key)
            if cached_result:
                self.logger.debug(f"Returning cached result for query: {query[:50]}...")
                return cached_result
        
        connection = self.connections[database_id]
        result = await connection.execute_query(query, parameters)
        
        # Cache successful SELECT results
        if (result.success and use_cache and self.config["cache_enabled"] 
            and query.strip().upper().startswith('SELECT')):
            cache_key = self._generate_cache_key(database_id, query, parameters)
            self._cache_result(cache_key, result)
        
        return result
    
    def _generate_cache_key(self, database_id: str, query: str, parameters: Dict = None) -> str:
        """Generate cache key for query."""
        key_data = f"{database_id}:{query}:{json.dumps(parameters or {}, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _get_cached_result(self, cache_key: str) -> Optional[QueryResult]:
        """Get cached query result."""
        if cache_key in self.query_cache:
            cached_data, timestamp = self.query_cache[cache_key]
            
            # Check if cache is still valid
            if datetime.now() - timestamp < timedelta(seconds=self.config["cache_ttl_seconds"]):
                return cached_data
            else:
                # Remove expired cache entry
                del self.query_cache[cache_key]
        
        return None
    
    def _cache_result(self, cache_key: str, result: QueryResult):
        """Cache query result."""
        self.query_cache[cache_key] = (result, datetime.now())
        
        # Limit cache size
        max_cache_size = 1000
        if len(self.query_cache) > max_cache_size:
            # Remove oldest entries
            sorted_cache = sorted(self.query_cache.items(), key=lambda x: x[1][1])
            for key, _ in sorted_cache[:100]:  # Remove 100 oldest entries
                del self.query_cache[key]
    
    async def execute_batch_queries(self, database_id: str, 
                                  queries: List[Tuple[str, Dict]]) -> List[QueryResult]:
        """Execute multiple queries in batch."""
        if database_id not in self.connections:
            error_result = QueryResult(
                success=False,
                rows_affected=0,
                execution_time_ms=0,
                error_message=f"Database connection not found: {database_id}"
            )
            return [error_result] * len(queries)
        
        results = []
        
        # Execute queries concurrently
        futures = []
        for query, parameters in queries:
            future = self.executor.submit(
                asyncio.run, 
                self.execute_query(database_id, query, parameters, use_cache=False)
            )
            futures.append(future)
        
        for future in as_completed(futures):
            try:
                result = future.result(timeout=self.config["query_timeout_seconds"])
                results.append(result)
            except Exception as e:
                error_result = QueryResult(
                    success=False,
                    rows_affected=0,
                    execution_time_ms=0,
                    error_message=str(e)
                )
                results.append(error_result)
        
        return results
    
    async def create_backup(self, database_id: str, backup_path: str = None) -> BackupResult:
        """Create database backup."""
        start_time = time.time()
        
        if database_id not in self.connections:
            return BackupResult(
                success=False,
                backup_id="",
                file_path="",
                file_size_mb=0,
                duration_seconds=0,
                timestamp=datetime.now(),
                error_message=f"Database connection not found: {database_id}"
            )
        
        connection = self.connections[database_id]
        backup_id = f"backup_{database_id}_{int(time.time())}"
        
        if not backup_path:
            backup_path = f"/tmp/backups/{backup_id}.sql"
        
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        
        try:
            if connection.config.type == DatabaseType.POSTGRESQL:
                await self._create_postgresql_backup(connection, backup_path)
            elif connection.config.type == DatabaseType.MYSQL:
                await self._create_mysql_backup(connection, backup_path)
            elif connection.config.type == DatabaseType.SQLITE:
                await self._create_sqlite_backup(connection, backup_path)
            else:
                raise NotImplementedError(f"Backup not implemented for {connection.config.type.value}")
            
            # Get file size
            file_size_mb = os.path.getsize(backup_path) / (1024 * 1024)
            duration_seconds = time.time() - start_time
            
            result = BackupResult(
                success=True,
                backup_id=backup_id,
                file_path=backup_path,
                file_size_mb=file_size_mb,
                duration_seconds=duration_seconds,
                timestamp=datetime.now()
            )
            
            self.backup_history[backup_id] = result
            self.logger.info(f"Backup created successfully: {backup_id}")
            
            return result
            
        except Exception as e:
            return BackupResult(
                success=False,
                backup_id=backup_id,
                file_path=backup_path,
                file_size_mb=0,
                duration_seconds=time.time() - start_time,
                timestamp=datetime.now(),
                error_message=str(e)
            )
    
    async def _create_postgresql_backup(self, connection: DatabaseConnection, backup_path: str):
        """Create PostgreSQL backup using pg_dump."""
        cmd = [
            'pg_dump',
            f'--host={connection.config.host}',
            f'--port={connection.config.port}',
            f'--username={connection.config.username}',
            f'--dbname={connection.config.database}',
            '--no-password',
            '--verbose',
            '--clean',
            '--no-acl',
            '--no-owner',
            f'--file={backup_path}'
        ]
        
        env = os.environ.copy()
        env['PGPASSWORD'] = connection.config.password
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"pg_dump failed: {stderr.decode()}")
    
    async def _create_mysql_backup(self, connection: DatabaseConnection, backup_path: str):
        """Create MySQL backup using mysqldump."""
        cmd = [
            'mysqldump',
            f'--host={connection.config.host}',
            f'--port={connection.config.port}',
            f'--user={connection.config.username}',
            f'--password={connection.config.password}',
            '--single-transaction',
            '--routines',
            '--triggers',
            connection.config.database
        ]
        
        with open(backup_path, 'w') as f:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=f,
                stderr=asyncio.subprocess.PIPE
            )
            
            _, stderr = await process.communicate()
            
            if process.returncode != 0:
                raise Exception(f"mysqldump failed: {stderr.decode()}")
    
    async def _create_sqlite_backup(self, connection: DatabaseConnection, backup_path: str):
        """Create SQLite backup."""
        source_path = f"{connection.config.host}/{connection.config.database}.db"
        
        # Simple file copy for SQLite
        import shutil
        shutil.copy2(source_path, backup_path)
    
    async def restore_backup(self, database_id: str, backup_path: str) -> bool:
        """Restore database from backup."""
        if database_id not in self.connections:
            self.logger.error(f"Database connection not found: {database_id}")
            return False
        
        if not os.path.exists(backup_path):
            self.logger.error(f"Backup file not found: {backup_path}")
            return False
        
        connection = self.connections[database_id]
        
        try:
            if connection.config.type == DatabaseType.POSTGRESQL:
                await self._restore_postgresql_backup(connection, backup_path)
            elif connection.config.type == DatabaseType.MYSQL:
                await self._restore_mysql_backup(connection, backup_path)
            elif connection.config.type == DatabaseType.SQLITE:
                await self._restore_sqlite_backup(connection, backup_path)
            else:
                raise NotImplementedError(f"Restore not implemented for {connection.config.type.value}")
            
            self.logger.info(f"Database restored successfully from: {backup_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Database restore failed: {e}")
            return False
    
    async def _restore_postgresql_backup(self, connection: DatabaseConnection, backup_path: str):
        """Restore PostgreSQL backup using psql."""
        cmd = [
            'psql',
            f'--host={connection.config.host}',
            f'--port={connection.config.port}',
            f'--username={connection.config.username}',
            f'--dbname={connection.config.database}',
            '--no-password',
            f'--file={backup_path}'
        ]
        
        env = os.environ.copy()
        env['PGPASSWORD'] = connection.config.password
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"psql restore failed: {stderr.decode()}")
    
    async def _restore_mysql_backup(self, connection: DatabaseConnection, backup_path: str):
        """Restore MySQL backup using mysql."""
        cmd = [
            'mysql',
            f'--host={connection.config.host}',
            f'--port={connection.config.port}',
            f'--user={connection.config.username}',
            f'--password={connection.config.password}',
            connection.config.database
        ]
        
        with open(backup_path, 'r') as f:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=f,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                raise Exception(f"mysql restore failed: {stderr.decode()}")
    
    async def _restore_sqlite_backup(self, connection: DatabaseConnection, backup_path: str):
        """Restore SQLite backup."""
        target_path = f"{connection.config.host}/{connection.config.database}.db"
        
        # Simple file copy for SQLite
        import shutil
        shutil.copy2(backup_path, target_path)
    
    def get_connection_status(self) -> Dict[str, Dict]:
        """Get status of all database connections."""
        status = {}
        
        for db_id, connection in self.connections.items():
            status[db_id] = {
                'name': connection.config.name,
                'type': connection.config.type.value,
                'status': connection.status.value,
                'host': connection.config.host,
                'port': connection.config.port,
                'database': connection.config.database,
                'last_error': connection.last_error
            }
        
        return status
    
    def get_backup_history(self) -> List[Dict]:
        """Get backup history."""
        return [asdict(backup) for backup in self.backup_history.values()]
    
    def cleanup_old_backups(self, retention_days: int = None):
        """Clean up old backup files."""
        if retention_days is None:
            retention_days = self.config["backup_retention_days"]
        
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        for backup_id, backup in list(self.backup_history.items()):
            if backup.timestamp < cutoff_date:
                try:
                    if os.path.exists(backup.file_path):
                        os.remove(backup.file_path)
                    del self.backup_history[backup_id]
                    self.logger.info(f"Cleaned up old backup: {backup_id}")
                except Exception as e:
                    self.logger.error(f"Failed to cleanup backup {backup_id}: {e}")


async def main():
    """Example usage of Enterprise Database Manager."""
    # Initialize database manager
    db_manager = EnterpriseDatabaseManager()
    
    # Configure PostgreSQL database
    pg_config = DatabaseConfig(
        id="main_postgres",
        name="Main PostgreSQL Database",
        type=DatabaseType.POSTGRESQL,
        host="localhost",
        port=5432,
        database="enterprise_db",
        username="postgres",
        password="password",
        ssl_enabled=False,
        max_connections=20
    )
    
    # Configure SQLite database
    sqlite_config = DatabaseConfig(
        id="local_sqlite",
        name="Local SQLite Database",
        type=DatabaseType.SQLITE,
        host="/tmp/databases",
        port=0,
        database="local_db",
        username="",
        password="",
        max_connections=1
    )
    
    try:
        # Add databases
        await db_manager.add_database(pg_config)
        await db_manager.add_database(sqlite_config)
        
        # Check connection status
        status = db_manager.get_connection_status()
        print("Database connections:")
        for db_id, info in status.items():
            print(f"  {db_id}: {info['status']} ({info['type']})")
        
        # Execute sample queries
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        result = await db_manager.execute_query("local_sqlite", create_table_query)
        if result.success:
            print("✅ Table created successfully")
        
        # Insert sample data
        insert_query = """
        INSERT INTO users (username, email) VALUES (?, ?)
        """
        
        result = await db_manager.execute_query(
            "local_sqlite", 
            insert_query, 
            {"username": "admin", "email": "admin@example.com"}
        )
        
        if result.success:
            print(f"✅ Inserted {result.rows_affected} row(s)")
        
        # Query data
        select_query = "SELECT * FROM users"
        result = await db_manager.execute_query("local_sqlite", select_query)
        
        if result.success:
            print(f"✅ Retrieved {result.rows_affected} row(s)")
            for row in result.data:
                print(f"  User: {row}")
        
        # Create backup
        backup_result = await db_manager.create_backup("local_sqlite")
        if backup_result.success:
            print(f"✅ Backup created: {backup_result.backup_id}")
            print(f"  File: {backup_result.file_path}")
            print(f"  Size: {backup_result.file_size_mb:.2f} MB")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        # Cleanup
        for db_id in list(db_manager.connections.keys()):
            await db_manager.remove_database(db_id)


if __name__ == "__main__":
    asyncio.run(main())