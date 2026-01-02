#!/usr/bin/env python3
"""
Distributed Task Scheduler - Enterprise Task Management and Orchestration
Advanced distributed task scheduling system with high availability and scalability.

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
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Callable, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
import hashlib
import pickle
import redis
import schedule
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import psutil
import socket

class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"
    SCHEDULED = "scheduled"

class TaskPriority(Enum):
    """Task priority levels."""
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4
    BACKGROUND = 5

class ExecutionMode(Enum):
    """Task execution modes."""
    SYNC = "sync"
    ASYNC = "async"
    PARALLEL = "parallel"
    DISTRIBUTED = "distributed"

@dataclass
class TaskDefinition:
    """Task definition and configuration."""
    id: str
    name: str
    function_name: str
    module_path: str
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    execution_mode: ExecutionMode = ExecutionMode.SYNC
    max_retries: int = 3
    retry_delay: int = 60
    timeout: int = 3600
    schedule_expression: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    created_by: str = "system"

@dataclass
class TaskExecution:
    """Task execution instance."""
    execution_id: str
    task_id: str
    status: TaskStatus
    worker_id: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    result: Any = None
    error_message: Optional[str] = None
    retry_count: int = 0
    logs: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)

@dataclass
class WorkerNode:
    """Distributed worker node."""
    worker_id: str
    hostname: str
    ip_address: str
    port: int
    status: str
    capabilities: List[str]
    max_concurrent_tasks: int
    current_task_count: int
    cpu_usage: float
    memory_usage: float
    last_heartbeat: datetime
    tags: List[str] = field(default_factory=list)

class TaskQueue:
    """Distributed task queue implementation."""
    
    def __init__(self, redis_client: redis.Redis, queue_name: str = "default"):
        self.redis_client = redis_client
        self.queue_name = queue_name
        self.logger = logging.getLogger(f'TaskQueue-{queue_name}')
        
    def enqueue(self, task: TaskDefinition, execution_id: str = None) -> str:
        """Add task to queue."""
        if not execution_id:
            execution_id = str(uuid.uuid4())
        
        execution = TaskExecution(
            execution_id=execution_id,
            task_id=task.id,
            status=TaskStatus.PENDING
        )
        
        # Store task definition and execution
        task_key = f"task:{task.id}"
        execution_key = f"execution:{execution_id}"
        
        self.redis_client.hset(task_key, mapping=self._serialize_task(task))
        self.redis_client.hset(execution_key, mapping=self._serialize_execution(execution))
        
        # Add to priority queue
        priority_score = task.priority.value
        queue_key = f"queue:{self.queue_name}"
        
        self.redis_client.zadd(queue_key, {execution_id: priority_score})
        
        self.logger.info(f"Enqueued task {task.id} with execution {execution_id}")
        
        return execution_id
    
    def dequeue(self, worker_id: str) -> Optional[Tuple[TaskDefinition, TaskExecution]]:
        """Get next task from queue."""
        queue_key = f"queue:{self.queue_name}"
        
        # Get highest priority task (lowest score)
        result = self.redis_client.zpopmin(queue_key, count=1)
        
        if not result:
            return None
        
        execution_id, _ = result[0]
        execution_id = execution_id.decode('utf-8')
        
        # Get execution and task data
        execution_key = f"execution:{execution_id}"
        execution_data = self.redis_client.hgetall(execution_key)
        
        if not execution_data:
            return None
        
        execution = self._deserialize_execution(execution_data)
        
        task_key = f"task:{execution.task_id}"
        task_data = self.redis_client.hgetall(task_key)
        
        if not task_data:
            return None
        
        task = self._deserialize_task(task_data)
        
        # Update execution status
        execution.status = TaskStatus.RUNNING
        execution.worker_id = worker_id
        execution.started_at = datetime.now()
        
        self.redis_client.hset(execution_key, mapping=self._serialize_execution(execution))
        
        self.logger.info(f"Dequeued task {task.id} for worker {worker_id}")
        
        return task, execution
    
    def update_execution(self, execution: TaskExecution):
        """Update task execution status."""
        execution_key = f"execution:{execution.execution_id}"
        self.redis_client.hset(execution_key, mapping=self._serialize_execution(execution))
    
    def get_execution(self, execution_id: str) -> Optional[TaskExecution]:
        """Get task execution by ID."""
        execution_key = f"execution:{execution_id}"
        execution_data = self.redis_client.hgetall(execution_key)
        
        if not execution_data:
            return None
        
        return self._deserialize_execution(execution_data)
    
    def _serialize_task(self, task: TaskDefinition) -> Dict[str, str]:
        """Serialize task definition for Redis storage."""
        return {
            'data': json.dumps(asdict(task), default=str)
        }
    
    def _deserialize_task(self, data: Dict) -> TaskDefinition:
        """Deserialize task definition from Redis."""
        task_data = json.loads(data[b'data'].decode('utf-8'))
        
        # Convert datetime strings back to datetime objects
        if 'created_at' in task_data:
            task_data['created_at'] = datetime.fromisoformat(task_data['created_at'])
        
        # Convert enums
        task_data['priority'] = TaskPriority(task_data['priority'])
        task_data['execution_mode'] = ExecutionMode(task_data['execution_mode'])
        
        return TaskDefinition(**task_data)
    
    def _serialize_execution(self, execution: TaskExecution) -> Dict[str, str]:
        """Serialize task execution for Redis storage."""
        return {
            'data': json.dumps(asdict(execution), default=str)
        }
    
    def _deserialize_execution(self, data: Dict) -> TaskExecution:
        """Deserialize task execution from Redis."""
        execution_data = json.loads(data[b'data'].decode('utf-8'))
        
        # Convert datetime strings back to datetime objects
        for field in ['started_at', 'completed_at']:
            if execution_data.get(field):
                execution_data[field] = datetime.fromisoformat(execution_data[field])
        
        # Convert enum
        execution_data['status'] = TaskStatus(execution_data['status'])
        
        return TaskExecution(**execution_data)

class TaskWorker:
    """Distributed task worker."""
    
    def __init__(self, worker_id: str, redis_client: redis.Redis, 
                 max_concurrent_tasks: int = 5):
        self.worker_id = worker_id
        self.redis_client = redis_client
        self.max_concurrent_tasks = max_concurrent_tasks
        self.current_tasks = {}
        self.running = False
        self.logger = logging.getLogger(f'TaskWorker-{worker_id}')
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_tasks)
        self.task_queue = TaskQueue(redis_client)
        
        # Register worker
        self._register_worker()
    
    def _register_worker(self):
        """Register worker in distributed system."""
        worker_info = WorkerNode(
            worker_id=self.worker_id,
            hostname=socket.gethostname(),
            ip_address=socket.gethostbyname(socket.gethostname()),
            port=0,  # Not using network communication in this example
            status="active",
            capabilities=["python", "general"],
            max_concurrent_tasks=self.max_concurrent_tasks,
            current_task_count=0,
            cpu_usage=psutil.cpu_percent(),
            memory_usage=psutil.virtual_memory().percent,
            last_heartbeat=datetime.now()
        )
        
        worker_key = f"worker:{self.worker_id}"
        self.redis_client.hset(worker_key, mapping={
            'data': json.dumps(asdict(worker_info), default=str)
        })
        
        # Set expiration for worker registration
        self.redis_client.expire(worker_key, 300)  # 5 minutes
        
        self.logger.info(f"Registered worker {self.worker_id}")
    
    def start(self):
        """Start worker to process tasks."""
        self.running = True
        self.logger.info(f"Starting worker {self.worker_id}")
        
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()
        
        # Main worker loop
        while self.running:
            try:
                if len(self.current_tasks) < self.max_concurrent_tasks:
                    # Try to get a task from queue
                    task_data = self.task_queue.dequeue(self.worker_id)
                    
                    if task_data:
                        task, execution = task_data
                        
                        # Submit task for execution
                        future = self.executor.submit(self._execute_task, task, execution)
                        self.current_tasks[execution.execution_id] = future
                        
                        self.logger.info(f"Started executing task {task.id}")
                
                # Check completed tasks
                completed_executions = []
                for execution_id, future in self.current_tasks.items():
                    if future.done():
                        completed_executions.append(execution_id)
                
                # Remove completed tasks
                for execution_id in completed_executions:
                    del self.current_tasks[execution_id]
                
                time.sleep(1)  # Polling interval
                
            except Exception as e:
                self.logger.error(f"Worker loop error: {e}")
                time.sleep(5)
    
    def stop(self):
        """Stop worker gracefully."""
        self.running = False
        self.logger.info(f"Stopping worker {self.worker_id}")
        
        # Wait for current tasks to complete
        for future in self.current_tasks.values():
            try:
                future.result(timeout=30)
            except Exception as e:
                self.logger.error(f"Error waiting for task completion: {e}")
        
        self.executor.shutdown(wait=True)
    
    def _execute_task(self, task: TaskDefinition, execution: TaskExecution):
        """Execute a single task."""
        start_time = time.time()
        
        try:
            self.logger.info(f"Executing task {task.id}")
            
            # Import and execute the task function
            module = __import__(task.module_path, fromlist=[task.function_name])
            task_function = getattr(module, task.function_name)
            
            # Execute with timeout
            if task.execution_mode == ExecutionMode.ASYNC:
                result = asyncio.run(task_function(*task.args, **task.kwargs))
            else:
                result = task_function(*task.args, **task.kwargs)
            
            # Update execution with success
            execution.status = TaskStatus.COMPLETED
            execution.result = result
            execution.completed_at = datetime.now()
            execution.duration_seconds = time.time() - start_time
            
            self.logger.info(f"Task {task.id} completed successfully")
            
        except Exception as e:
            # Handle task failure
            execution.status = TaskStatus.FAILED
            execution.error_message = str(e)
            execution.completed_at = datetime.now()
            execution.duration_seconds = time.time() - start_time
            
            self.logger.error(f"Task {task.id} failed: {e}")
            
            # Check if retry is needed
            if execution.retry_count < task.max_retries:
                execution.retry_count += 1
                execution.status = TaskStatus.RETRYING
                
                # Re-enqueue for retry after delay
                retry_delay = task.retry_delay * (2 ** execution.retry_count)  # Exponential backoff
                
                def retry_task():
                    time.sleep(retry_delay)
                    self.task_queue.enqueue(task, execution.execution_id)
                
                retry_thread = threading.Thread(target=retry_task)
                retry_thread.daemon = True
                retry_thread.start()
                
                self.logger.info(f"Task {task.id} scheduled for retry {execution.retry_count}/{task.max_retries}")
        
        finally:
            # Update execution status
            self.task_queue.update_execution(execution)
    
    def _heartbeat_loop(self):
        """Send periodic heartbeat to maintain worker registration."""
        while self.running:
            try:
                self._register_worker()
                time.sleep(60)  # Heartbeat every minute
            except Exception as e:
                self.logger.error(f"Heartbeat error: {e}")
                time.sleep(10)

class TaskScheduler:
    """Task scheduler for cron-like scheduling."""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client
        self.task_queue = TaskQueue(redis_client)
        self.scheduled_tasks = {}
        self.running = False
        self.logger = logging.getLogger('TaskScheduler')
    
    def add_scheduled_task(self, task: TaskDefinition):
        """Add a scheduled task."""
        if not task.schedule_expression:
            raise ValueError("Task must have schedule_expression for scheduling")
        
        self.scheduled_tasks[task.id] = task
        
        # Parse schedule expression and set up schedule
        self._setup_schedule(task)
        
        self.logger.info(f"Added scheduled task {task.id}: {task.schedule_expression}")
    
    def _setup_schedule(self, task: TaskDefinition):
        """Setup schedule for task based on expression."""
        expression = task.schedule_expression
        
        # Simple schedule expression parsing (would be more sophisticated in production)
        if expression.startswith("every "):
            parts = expression.split()
            
            if len(parts) >= 3:
                interval = int(parts[1])
                unit = parts[2]
                
                if unit in ["second", "seconds"]:
                    schedule.every(interval).seconds.do(self._execute_scheduled_task, task.id)
                elif unit in ["minute", "minutes"]:
                    schedule.every(interval).minutes.do(self._execute_scheduled_task, task.id)
                elif unit in ["hour", "hours"]:
                    schedule.every(interval).hours.do(self._execute_scheduled_task, task.id)
                elif unit in ["day", "days"]:
                    schedule.every(interval).days.do(self._execute_scheduled_task, task.id)
        
        elif expression.startswith("daily at "):
            time_str = expression.replace("daily at ", "")
            schedule.every().day.at(time_str).do(self._execute_scheduled_task, task.id)
        
        elif expression.startswith("weekly on "):
            day_time = expression.replace("weekly on ", "")
            parts = day_time.split(" at ")
            
            if len(parts) == 2:
                day, time_str = parts
                getattr(schedule.every(), day.lower()).at(time_str).do(self._execute_scheduled_task, task.id)
    
    def _execute_scheduled_task(self, task_id: str):
        """Execute a scheduled task."""
        if task_id in self.scheduled_tasks:
            task = self.scheduled_tasks[task_id]
            execution_id = self.task_queue.enqueue(task)
            self.logger.info(f"Scheduled task {task_id} enqueued with execution {execution_id}")
    
    def start(self):
        """Start the scheduler."""
        self.running = True
        self.logger.info("Starting task scheduler")
        
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                time.sleep(5)
    
    def stop(self):
        """Stop the scheduler."""
        self.running = False
        schedule.clear()
        self.logger.info("Task scheduler stopped")

class DistributedTaskScheduler:
    """Main distributed task scheduler system."""
    
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379, 
                 redis_db: int = 0):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self.task_queue = TaskQueue(self.redis_client)
        self.scheduler = TaskScheduler(self.redis_client)
        self.workers = {}
        self.logger = self._setup_logging()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger('DistributedTaskScheduler')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def submit_task(self, task: TaskDefinition) -> str:
        """Submit a task for execution."""
        execution_id = self.task_queue.enqueue(task)
        self.logger.info(f"Submitted task {task.id} for execution")
        return execution_id
    
    def schedule_task(self, task: TaskDefinition):
        """Schedule a recurring task."""
        self.scheduler.add_scheduled_task(task)
        self.logger.info(f"Scheduled task {task.id}")
    
    def start_worker(self, worker_id: str = None, max_concurrent_tasks: int = 5) -> str:
        """Start a new worker."""
        if not worker_id:
            worker_id = f"worker_{uuid.uuid4().hex[:8]}"
        
        worker = TaskWorker(worker_id, self.redis_client, max_concurrent_tasks)
        self.workers[worker_id] = worker
        
        # Start worker in separate thread
        worker_thread = threading.Thread(target=worker.start)
        worker_thread.daemon = True
        worker_thread.start()
        
        self.logger.info(f"Started worker {worker_id}")
        
        return worker_id
    
    def stop_worker(self, worker_id: str):
        """Stop a worker."""
        if worker_id in self.workers:
            self.workers[worker_id].stop()
            del self.workers[worker_id]
            self.logger.info(f"Stopped worker {worker_id}")
    
    def start_scheduler(self):
        """Start the task scheduler."""
        scheduler_thread = threading.Thread(target=self.scheduler.start)
        scheduler_thread.daemon = True
        scheduler_thread.start()
        
        self.logger.info("Started task scheduler")
    
    def get_task_status(self, execution_id: str) -> Optional[TaskExecution]:
        """Get status of a task execution."""
        return self.task_queue.get_execution(execution_id)
    
    def get_worker_status(self) -> List[Dict]:
        """Get status of all workers."""
        workers = []
        
        # Get all worker keys from Redis
        worker_keys = self.redis_client.keys("worker:*")
        
        for key in worker_keys:
            worker_data = self.redis_client.hgetall(key)
            if worker_data:
                worker_info = json.loads(worker_data[b'data'].decode('utf-8'))
                workers.append(worker_info)
        
        return workers
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        queue_key = f"queue:{self.task_queue.queue_name}"
        queue_length = self.redis_client.zcard(queue_key)
        
        # Get execution counts by status
        execution_keys = self.redis_client.keys("execution:*")
        status_counts = {}
        
        for key in execution_keys:
            execution_data = self.redis_client.hgetall(key)
            if execution_data:
                execution_info = json.loads(execution_data[b'data'].decode('utf-8'))
                status = execution_info['status']
                status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            "queue_length": queue_length,
            "execution_counts": status_counts,
            "active_workers": len(self.get_worker_status())
        }


# Example task functions
def example_cpu_intensive_task(n: int) -> int:
    """Example CPU-intensive task."""
    result = 0
    for i in range(n):
        result += i * i
    return result

def example_io_task(filename: str, content: str) -> str:
    """Example I/O task."""
    with open(filename, 'w') as f:
        f.write(content)
    return f"Written {len(content)} characters to {filename}"

async def example_async_task(delay: int) -> str:
    """Example async task."""
    await asyncio.sleep(delay)
    return f"Async task completed after {delay} seconds"

def example_failing_task() -> str:
    """Example task that always fails."""
    raise Exception("This task is designed to fail")


def main():
    """Example usage of Distributed Task Scheduler."""
    # Initialize scheduler
    scheduler = DistributedTaskScheduler()
    
    try:
        # Start workers
        worker1_id = scheduler.start_worker("worker-1", max_concurrent_tasks=3)
        worker2_id = scheduler.start_worker("worker-2", max_concurrent_tasks=2)
        
        # Start scheduler for recurring tasks
        scheduler.start_scheduler()
        
        print("🚀 Distributed Task Scheduler started")
        print(f"   Workers: {worker1_id}, {worker2_id}")
        
        # Submit some tasks
        tasks = [
            TaskDefinition(
                id="cpu_task_1",
                name="CPU Intensive Task",
                function_name="example_cpu_intensive_task",
                module_path="__main__",
                args=[1000000],
                priority=TaskPriority.HIGH
            ),
            TaskDefinition(
                id="io_task_1",
                name="I/O Task",
                function_name="example_io_task",
                module_path="__main__",
                args=["/tmp/test_file.txt", "Hello, World!"],
                priority=TaskPriority.NORMAL
            ),
            TaskDefinition(
                id="async_task_1",
                name="Async Task",
                function_name="example_async_task",
                module_path="__main__",
                args=[3],
                execution_mode=ExecutionMode.ASYNC,
                priority=TaskPriority.LOW
            ),
            TaskDefinition(
                id="failing_task_1",
                name="Failing Task",
                function_name="example_failing_task",
                module_path="__main__",
                max_retries=2,
                priority=TaskPriority.NORMAL
            )
        ]
        
        # Submit tasks
        execution_ids = []
        for task in tasks:
            execution_id = scheduler.submit_task(task)
            execution_ids.append(execution_id)
            print(f"📋 Submitted task: {task.name} (ID: {execution_id})")
        
        # Schedule a recurring task
        recurring_task = TaskDefinition(
            id="recurring_task_1",
            name="Recurring Task",
            function_name="example_cpu_intensive_task",
            module_path="__main__",
            args=[10000],
            schedule_expression="every 30 seconds"
        )
        
        scheduler.schedule_task(recurring_task)
        print("⏰ Scheduled recurring task")
        
        # Monitor task execution
        print("\n📊 Monitoring task execution...")
        
        for _ in range(30):  # Monitor for 30 seconds
            time.sleep(1)
            
            # Check task statuses
            completed_count = 0
            for execution_id in execution_ids:
                execution = scheduler.get_task_status(execution_id)
                if execution and execution.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                    completed_count += 1
            
            # Get queue stats
            stats = scheduler.get_queue_stats()
            
            print(f"\r   Queue: {stats['queue_length']} | "
                  f"Workers: {stats['active_workers']} | "
                  f"Completed: {completed_count}/{len(execution_ids)}", end="")
            
            if completed_count == len(execution_ids):
                break
        
        print("\n\n✅ Task execution completed")
        
        # Show final results
        print("\n📋 Final Results:")
        for execution_id in execution_ids:
            execution = scheduler.get_task_status(execution_id)
            if execution:
                status_emoji = "✅" if execution.status == TaskStatus.COMPLETED else "❌"
                print(f"   {status_emoji} {execution.task_id}: {execution.status.value}")
                if execution.error_message:
                    print(f"      Error: {execution.error_message}")
                if execution.duration_seconds:
                    print(f"      Duration: {execution.duration_seconds:.2f}s")
        
        # Show worker status
        print("\n👷 Worker Status:")
        workers = scheduler.get_worker_status()
        for worker in workers:
            print(f"   🔧 {worker['worker_id']}: {worker['status']}")
            print(f"      CPU: {worker['cpu_usage']:.1f}% | Memory: {worker['memory_usage']:.1f}%")
        
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        # Cleanup
        for worker_id in list(scheduler.workers.keys()):
            scheduler.stop_worker(worker_id)
        
        scheduler.scheduler.stop()


if __name__ == "__main__":
    main()