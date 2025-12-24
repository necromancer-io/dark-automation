#!/usr/bin/env python3
"""
CI/CD Pipeline Manager - Enterprise-grade Continuous Integration and Deployment
Advanced pipeline orchestration system for automated software delivery.

Use of this code is at your own risk.
Author bears no responsibility for any damages caused by the code.
"""

import os
import sys
import json
import yaml
import time
import asyncio
import logging
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass, asdict, field
from enum import Enum
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import tempfile
import shutil

class PipelineStage(Enum):
    """Pipeline stage types."""
    SOURCE = "source"
    BUILD = "build"
    TEST = "test"
    SECURITY_SCAN = "security_scan"
    QUALITY_GATE = "quality_gate"
    PACKAGE = "build"
    DEPLOY_STAGING = "deploy_staging"
    INTEGRATION_TEST = "integration_test"
    DEPLOY_PRODUCTION = "deploy_production"
    MONITORING = "monitoring"

class PipelineStatus(Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"

class DeploymentEnvironment(Enum):
    """Deployment environments."""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"

@dataclass
class PipelineStep:
    """Individual pipeline step definition."""
    id: str
    name: str
    stage: PipelineStage
    command: str
    environment: Dict[str, str] = field(default_factory=dict)
    working_directory: str = "."
    timeout: int = 300
    retry_count: int = 0
    continue_on_failure: bool = False
    artifacts: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)

@dataclass
class PipelineExecution:
    """Pipeline execution instance."""
    id: str
    pipeline_id: str
    commit_hash: str
    branch: str
    triggered_by: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: PipelineStatus = PipelineStatus.PENDING
    steps: Dict[str, Dict] = field(default_factory=dict)
    artifacts: Dict[str, str] = field(default_factory=dict)
    logs: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Pipeline:
    """CI/CD Pipeline definition."""
    id: str
    name: str
    description: str
    repository: str
    branch_patterns: List[str]
    steps: List[PipelineStep]
    environment_variables: Dict[str, str] = field(default_factory=dict)
    notifications: Dict[str, Any] = field(default_factory=dict)
    quality_gates: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

class CICDPipelineManager:
    """Enterprise-grade CI/CD Pipeline Manager."""
    
    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)
        self.pipelines = {}
        self.executions = {}
        self.artifacts_store = {}
        self.logger = self._setup_logging()
        self.executor = ThreadPoolExecutor(max_workers=self.config.get("max_concurrent_pipelines", 5))
        self.workspace_dir = self.config.get("workspace_dir", "/tmp/cicd_workspace")
        os.makedirs(self.workspace_dir, exist_ok=True)
        
    def _load_config(self, config_path: str) -> Dict:
        """Load CI/CD configuration."""
        default_config = {
            "workspace_dir": "/tmp/cicd_workspace",
            "artifacts_retention_days": 30,
            "max_concurrent_pipelines": 5,
            "default_timeout": 3600,
            "notification_channels": {
                "slack": {
                    "enabled": False,
                    "webhook_url": "",
                    "channels": ["#ci-cd", "#alerts"]
                },
                "email": {
                    "enabled": False,
                    "smtp_server": "",
                    "recipients": []
                }
            },
            "quality_gates": {
                "code_coverage_threshold": 80,
                "security_scan_threshold": "medium",
                "performance_threshold": 2000
            },
            "deployment_strategies": {
                "blue_green": {
                    "enabled": True,
                    "health_check_timeout": 300
                },
                "canary": {
                    "enabled": True,
                    "traffic_percentage": 10,
                    "monitoring_duration": 600
                },
                "rolling": {
                    "enabled": True,
                    "batch_size": 2,
                    "max_unavailable": 1
                }
            },
            "integrations": {
                "git": {
                    "provider": "github",
                    "webhook_secret": ""
                },
                "container_registry": {
                    "provider": "docker_hub",
                    "registry_url": "docker.io"
                },
                "kubernetes": {
                    "enabled": False,
                    "cluster_config": ""
                },
                "monitoring": {
                    "prometheus_url": "",
                    "grafana_url": ""
                }
            }
        }
        
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                    user_config = yaml.safe_load(f)
                else:
                    user_config = json.load(f)
                
                default_config = self._deep_merge(default_config, user_config)
        
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
        logger = logging.getLogger('CICDPipelineManager')
        logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler
        log_dir = os.path.join(self.workspace_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.FileHandler(os.path.join(log_dir, 'cicd_manager.log'))
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    def create_pipeline(self, pipeline_spec: Dict) -> Pipeline:
        """Create new CI/CD pipeline from specification."""
        self.logger.info(f"Creating pipeline: {pipeline_spec.get('name', 'Unnamed')}")
        
        pipeline_id = pipeline_spec.get("id", f"pipeline_{int(time.time())}")
        
        # Parse pipeline steps
        steps = []
        for step_spec in pipeline_spec.get("steps", []):
            step = PipelineStep(
                id=step_spec["id"],
                name=step_spec["name"],
                stage=PipelineStage(step_spec["stage"]),
                command=step_spec["command"],
                environment=step_spec.get("environment", {}),
                working_directory=step_spec.get("working_directory", "."),
                timeout=step_spec.get("timeout", 300),
                retry_count=step_spec.get("retry_count", 0),
                continue_on_failure=step_spec.get("continue_on_failure", False),
                artifacts=step_spec.get("artifacts", []),
                dependencies=step_spec.get("dependencies", [])
            )
            steps.append(step)
        
        pipeline = Pipeline(
            id=pipeline_id,
            name=pipeline_spec["name"],
            description=pipeline_spec.get("description", ""),
            repository=pipeline_spec["repository"],
            branch_patterns=pipeline_spec.get("branch_patterns", ["main", "master"]),
            steps=steps,
            environment_variables=pipeline_spec.get("environment_variables", {}),
            notifications=pipeline_spec.get("notifications", {}),
            quality_gates=pipeline_spec.get("quality_gates", {})
        )
        
        self.pipelines[pipeline_id] = pipeline
        self.logger.info(f"Created pipeline {pipeline_id} with {len(steps)} steps")
        
        return pipeline
    
    def trigger_pipeline(self, pipeline_id: str, commit_hash: str, branch: str, 
                        triggered_by: str = "manual") -> PipelineExecution:
        """Trigger pipeline execution."""
        if pipeline_id not in self.pipelines:
            raise ValueError(f"Pipeline {pipeline_id} not found")
        
        pipeline = self.pipelines[pipeline_id]
        
        # Check if branch matches patterns
        if not self._branch_matches_patterns(branch, pipeline.branch_patterns):
            raise ValueError(f"Branch {branch} does not match pipeline patterns")
        
        execution_id = f"exec_{pipeline_id}_{int(time.time())}"
        
        execution = PipelineExecution(
            id=execution_id,
            pipeline_id=pipeline_id,
            commit_hash=commit_hash,
            branch=branch,
            triggered_by=triggered_by,
            start_time=datetime.now()
        )
        
        self.executions[execution_id] = execution
        self.logger.info(f"Triggered pipeline execution {execution_id}")
        
        # Start execution asynchronously
        self.executor.submit(self._execute_pipeline, execution_id)
        
        return execution
    
    def _branch_matches_patterns(self, branch: str, patterns: List[str]) -> bool:
        """Check if branch matches any of the patterns."""
        import fnmatch
        return any(fnmatch.fnmatch(branch, pattern) for pattern in patterns)
    
    def _execute_pipeline(self, execution_id: str):
        """Execute pipeline asynchronously."""
        execution = self.executions[execution_id]
        pipeline = self.pipelines[execution.pipeline_id]
        
        try:
            execution.status = PipelineStatus.RUNNING
            self.logger.info(f"Starting pipeline execution {execution_id}")
            
            # Create workspace for this execution
            workspace = os.path.join(self.workspace_dir, execution_id)
            os.makedirs(workspace, exist_ok=True)
            
            # Clone repository
            self._clone_repository(pipeline.repository, execution.commit_hash, workspace)
            
            # Execute steps in order
            for step in pipeline.steps:
                if not self._should_execute_step(step, execution):
                    execution.steps[step.id] = {
                        "status": PipelineStatus.SKIPPED.value,
                        "start_time": datetime.now().isoformat(),
                        "end_time": datetime.now().isoformat(),
                        "message": "Step skipped due to dependencies"
                    }
                    continue
                
                step_result = self._execute_step(step, execution, workspace)
                execution.steps[step.id] = step_result
                
                # Check if step failed and should stop pipeline
                if (step_result["status"] == PipelineStatus.FAILED.value and 
                    not step.continue_on_failure):
                    execution.status = PipelineStatus.FAILED
                    break
                
                # Check quality gates
                if not self._check_quality_gates(step, step_result, pipeline.quality_gates):
                    execution.status = PipelineStatus.FAILED
                    execution.logs.append(f"Quality gate failed for step {step.id}")
                    break
            
            # Set final status
            if execution.status == PipelineStatus.RUNNING:
                execution.status = PipelineStatus.SUCCESS
            
            execution.end_time = datetime.now()
            
            # Generate metrics
            execution.metrics = self._generate_execution_metrics(execution, pipeline)
            
            # Send notifications
            self._send_notifications(execution, pipeline)
            
            # Cleanup workspace
            if self.config.get("cleanup_workspace", True):
                shutil.rmtree(workspace, ignore_errors=True)
            
            self.logger.info(f"Pipeline execution {execution_id} completed with status {execution.status.value}")
            
        except Exception as e:
            execution.status = PipelineStatus.FAILED
            execution.end_time = datetime.now()
            execution.logs.append(f"Pipeline execution failed: {str(e)}")
            self.logger.error(f"Pipeline execution {execution_id} failed: {e}")
    
    def _clone_repository(self, repository: str, commit_hash: str, workspace: str):
        """Clone repository to workspace."""
        self.logger.info(f"Cloning repository {repository} to {workspace}")
        
        # Simulate git clone
        repo_dir = os.path.join(workspace, "source")
        os.makedirs(repo_dir, exist_ok=True)
        
        # Create dummy source files for simulation
        with open(os.path.join(repo_dir, "README.md"), 'w') as f:
            f.write(f"# Repository\n\nCommit: {commit_hash}\n")
        
        with open(os.path.join(repo_dir, "src", "main.py"), 'w') as f:
            os.makedirs(os.path.dirname(f.name), exist_ok=True)
            f.write("#!/usr/bin/env python3\nprint('Hello, World!')\n")
    
    def _should_execute_step(self, step: PipelineStep, execution: PipelineExecution) -> bool:
        """Check if step should be executed based on dependencies."""
        for dependency in step.dependencies:
            if dependency not in execution.steps:
                return False
            if execution.steps[dependency]["status"] != PipelineStatus.SUCCESS.value:
                return False
        return True
    
    def _execute_step(self, step: PipelineStep, execution: PipelineExecution, workspace: str) -> Dict:
        """Execute individual pipeline step."""
        self.logger.info(f"Executing step {step.id}: {step.name}")
        
        start_time = datetime.now()
        step_result = {
            "status": PipelineStatus.RUNNING.value,
            "start_time": start_time.isoformat(),
            "logs": [],
            "artifacts": [],
            "metrics": {}
        }
        
        try:
            # Prepare environment
            env = os.environ.copy()
            env.update(step.environment)
            
            # Set working directory
            work_dir = os.path.join(workspace, "source", step.working_directory)
            if not os.path.exists(work_dir):
                os.makedirs(work_dir, exist_ok=True)
            
            # Execute command with retry logic
            for attempt in range(step.retry_count + 1):
                try:
                    result = self._run_command(step.command, work_dir, env, step.timeout)
                    step_result["logs"].extend(result["logs"])
                    
                    if result["return_code"] == 0:
                        step_result["status"] = PipelineStatus.SUCCESS.value
                        break
                    else:
                        if attempt == step.retry_count:
                            step_result["status"] = PipelineStatus.FAILED.value
                            step_result["logs"].append(f"Command failed after {attempt + 1} attempts")
                        else:
                            step_result["logs"].append(f"Attempt {attempt + 1} failed, retrying...")
                            time.sleep(2 ** attempt)  # Exponential backoff
                
                except subprocess.TimeoutExpired:
                    step_result["status"] = PipelineStatus.FAILED.value
                    step_result["logs"].append(f"Step timed out after {step.timeout} seconds")
                    break
            
            # Collect artifacts
            step_result["artifacts"] = self._collect_artifacts(step.artifacts, work_dir, execution.id)
            
            # Generate step metrics
            step_result["metrics"] = self._generate_step_metrics(step, step_result, start_time)
            
        except Exception as e:
            step_result["status"] = PipelineStatus.FAILED.value
            step_result["logs"].append(f"Step execution error: {str(e)}")
        
        step_result["end_time"] = datetime.now().isoformat()
        return step_result
    
    def _run_command(self, command: str, work_dir: str, env: Dict, timeout: int) -> Dict:
        """Run shell command and capture output."""
        self.logger.debug(f"Running command: {command}")
        
        # Simulate different types of commands
        if "build" in command.lower():
            return self._simulate_build_command(command, work_dir)
        elif "test" in command.lower():
            return self._simulate_test_command(command, work_dir)
        elif "security" in command.lower() or "scan" in command.lower():
            return self._simulate_security_scan_command(command, work_dir)
        elif "deploy" in command.lower():
            return self._simulate_deploy_command(command, work_dir)
        else:
            return self._simulate_generic_command(command, work_dir)
    
    def _simulate_build_command(self, command: str, work_dir: str) -> Dict:
        """Simulate build command execution."""
        logs = [
            "Starting build process...",
            "Downloading dependencies...",
            "Compiling source code...",
            "Running static analysis...",
            "Build completed successfully"
        ]
        
        # Create build artifacts
        build_dir = os.path.join(work_dir, "build")
        os.makedirs(build_dir, exist_ok=True)
        
        with open(os.path.join(build_dir, "app.jar"), 'w') as f:
            f.write("# Simulated build artifact\n")
        
        return {"return_code": 0, "logs": logs}
    
    def _simulate_test_command(self, command: str, work_dir: str) -> Dict:
        """Simulate test command execution."""
        logs = [
            "Initializing test environment...",
            "Running unit tests...",
            "✓ TestUserAuthentication.test_login_success",
            "✓ TestUserAuthentication.test_login_failure",
            "✓ TestDataProcessor.test_process_valid_data",
            "✓ TestDataProcessor.test_process_invalid_data",
            "Running integration tests...",
            "✓ TestAPIEndpoints.test_user_creation",
            "✓ TestAPIEndpoints.test_data_retrieval",
            "Test Results: 6 passed, 0 failed",
            "Code Coverage: 85.2%"
        ]
        
        # Create test reports
        reports_dir = os.path.join(work_dir, "test-reports")
        os.makedirs(reports_dir, exist_ok=True)
        
        test_report = {
            "tests_run": 6,
            "tests_passed": 6,
            "tests_failed": 0,
            "code_coverage": 85.2,
            "duration": 45.3
        }
        
        with open(os.path.join(reports_dir, "test-results.json"), 'w') as f:
            json.dump(test_report, f, indent=2)
        
        return {"return_code": 0, "logs": logs}
    
    def _simulate_security_scan_command(self, command: str, work_dir: str) -> Dict:
        """Simulate security scan command execution."""
        logs = [
            "Starting security scan...",
            "Scanning for vulnerabilities...",
            "Checking dependencies for known CVEs...",
            "Running SAST analysis...",
            "Running DAST analysis...",
            "Security scan completed",
            "Found 0 critical vulnerabilities",
            "Found 1 medium vulnerability",
            "Found 3 low vulnerabilities"
        ]
        
        # Create security report
        security_dir = os.path.join(work_dir, "security-reports")
        os.makedirs(security_dir, exist_ok=True)
        
        security_report = {
            "scan_date": datetime.now().isoformat(),
            "vulnerabilities": {
                "critical": 0,
                "high": 0,
                "medium": 1,
                "low": 3
            },
            "details": [
                {
                    "severity": "medium",
                    "type": "dependency",
                    "description": "Outdated library version detected"
                }
            ]
        }
        
        with open(os.path.join(security_dir, "security-report.json"), 'w') as f:
            json.dump(security_report, f, indent=2)
        
        return {"return_code": 0, "logs": logs}
    
    def _simulate_deploy_command(self, command: str, work_dir: str) -> Dict:
        """Simulate deployment command execution."""
        logs = [
            "Starting deployment process...",
            "Preparing deployment build...",
            "Uploading artifacts to registry...",
            "Updating infrastructure configuration...",
            "Rolling out new version...",
            "Running health checks...",
            "Deployment completed successfully",
            "Application is healthy and serving traffic"
        ]
        
        return {"return_code": 0, "logs": logs}
    
    def _simulate_generic_command(self, command: str, work_dir: str) -> Dict:
        """Simulate generic command execution."""
        logs = [
            f"Executing: {command}",
            "Command completed successfully"
        ]
        
        return {"return_code": 0, "logs": logs}
    
    def _collect_artifacts(self, artifact_patterns: List[str], work_dir: str, execution_id: str) -> List[str]:
        """Collect build artifacts."""
        artifacts = []
        
        for pattern in artifact_patterns:
            # Simulate artifact collection
            artifact_path = os.path.join(work_dir, pattern)
            if os.path.exists(artifact_path):
                # Store artifact in artifacts store
                artifact_id = f"{execution_id}_{os.path.basename(pattern)}"
                self.artifacts_store[artifact_id] = artifact_path
                artifacts.append(artifact_id)
        
        return artifacts
    
    def _generate_step_metrics(self, step: PipelineStep, step_result: Dict, start_time: datetime) -> Dict:
        """Generate metrics for pipeline step."""
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return {
            "duration_seconds": duration,
            "stage": step.stage.value,
            "success": step_result["status"] == PipelineStatus.SUCCESS.value,
            "retry_count": step.retry_count,
            "artifacts_count": len(step_result.get("artifacts", []))
        }
    
    def _check_quality_gates(self, step: PipelineStep, step_result: Dict, quality_gates: Dict) -> bool:
        """Check quality gates for pipeline step."""
        if step.stage == PipelineStage.TEST:
            # Check code coverage
            coverage_threshold = quality_gates.get("code_coverage_threshold", 80)
            # Simulate coverage check (would parse actual test reports)
            simulated_coverage = 85.2
            if simulated_coverage < coverage_threshold:
                return False
        
        elif step.stage == PipelineStage.SECURITY_SCAN:
            # Check security scan results
            security_threshold = quality_gates.get("security_scan_threshold", "medium")
            # Simulate security check (would parse actual security reports)
            # For simulation, assume no critical vulnerabilities found
            return True
        
        return True
    
    def _generate_execution_metrics(self, execution: PipelineExecution, pipeline: Pipeline) -> Dict:
        """Generate metrics for pipeline execution."""
        if not execution.end_time:
            return {}
        
        duration = (execution.end_time - execution.start_time).total_seconds()
        
        step_metrics = {}
        total_steps = len(pipeline.steps)
        successful_steps = 0
        
        for step_id, step_result in execution.steps.items():
            if step_result["status"] == PipelineStatus.SUCCESS.value:
                successful_steps += 1
            
            step_metrics[step_id] = step_result.get("metrics", {})
        
        return {
            "total_duration_seconds": duration,
            "total_steps": total_steps,
            "successful_steps": successful_steps,
            "success_rate": (successful_steps / total_steps) * 100 if total_steps > 0 else 0,
            "step_metrics": step_metrics,
            "artifacts_generated": len(execution.artifacts)
        }
    
    def _send_notifications(self, execution: PipelineExecution, pipeline: Pipeline):
        """Send pipeline execution notifications."""
        if not pipeline.notifications:
            return
        
        message = self._create_notification_message(execution, pipeline)
        
        # Slack notification
        if (pipeline.notifications.get("slack", {}).get("enabled") and 
            self.config["notification_channels"]["slack"]["enabled"]):
            self._send_slack_notification(message, pipeline.notifications["slack"])
        
        # Email notification
        if (pipeline.notifications.get("email", {}).get("enabled") and 
            self.config["notification_channels"]["email"]["enabled"]):
            self._send_email_notification(message, pipeline.notifications["email"])
    
    def _create_notification_message(self, execution: PipelineExecution, pipeline: Pipeline) -> str:
        """Create notification message."""
        status_emoji = "✅" if execution.status == PipelineStatus.SUCCESS else "❌"
        
        message = f"{status_emoji} Pipeline '{pipeline.name}' {execution.status.value}\n"
        message += f"Execution ID: {execution.id}\n"
        message += f"Branch: {execution.branch}\n"
        message += f"Commit: {execution.commit_hash[:8]}\n"
        message += f"Duration: {execution.metrics.get('total_duration_seconds', 0):.1f}s\n"
        
        if execution.status == PipelineStatus.FAILED:
            failed_steps = [step_id for step_id, result in execution.steps.items() 
                          if result["status"] == PipelineStatus.FAILED.value]
            if failed_steps:
                message += f"Failed steps: {', '.join(failed_steps)}\n"
        
        return message
    
    def _send_slack_notification(self, message: str, slack_config: Dict):
        """Send Slack notification."""
        self.logger.info(f"Sending Slack notification: {message[:100]}...")
        # In real implementation, would use Slack webhook
    
    def _send_email_notification(self, message: str, email_config: Dict):
        """Send email notification."""
        self.logger.info(f"Sending email notification: {message[:100]}...")
        # In real implementation, would use SMTP
    
    def get_pipeline_execution(self, execution_id: str) -> Optional[PipelineExecution]:
        """Get pipeline execution by ID."""
        return self.executions.get(execution_id)
    
    def list_pipeline_executions(self, pipeline_id: str = None, 
                                status: PipelineStatus = None) -> List[PipelineExecution]:
        """List pipeline executions with optional filtering."""
        executions = list(self.executions.values())
        
        if pipeline_id:
            executions = [e for e in executions if e.pipeline_id == pipeline_id]
        
        if status:
            executions = [e for e in executions if e.status == status]
        
        return sorted(executions, key=lambda x: x.start_time, reverse=True)
    
    def cancel_pipeline_execution(self, execution_id: str) -> bool:
        """Cancel running pipeline execution."""
        if execution_id not in self.executions:
            return False
        
        execution = self.executions[execution_id]
        if execution.status == PipelineStatus.RUNNING:
            execution.status = PipelineStatus.CANCELLED
            execution.end_time = datetime.now()
            self.logger.info(f"Cancelled pipeline execution {execution_id}")
            return True
        
        return False
    
    def get_pipeline_metrics(self, pipeline_id: str, days: int = 30) -> Dict:
        """Get pipeline metrics for specified time period."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        executions = [e for e in self.executions.values() 
                     if e.pipeline_id == pipeline_id and e.start_time >= cutoff_date]
        
        if not executions:
            return {"message": "No executions found for the specified period"}
        
        total_executions = len(executions)
        successful_executions = len([e for e in executions if e.status == PipelineStatus.SUCCESS])
        failed_executions = len([e for e in executions if e.status == PipelineStatus.FAILED])
        
        durations = [e.metrics.get("total_duration_seconds", 0) for e in executions 
                    if e.metrics and "total_duration_seconds" in e.metrics]
        
        avg_duration = sum(durations) / len(durations) if durations else 0
        
        return {
            "pipeline_id": pipeline_id,
            "period_days": days,
            "total_executions": total_executions,
            "successful_executions": successful_executions,
            "failed_executions": failed_executions,
            "success_rate": (successful_executions / total_executions) * 100 if total_executions > 0 else 0,
            "average_duration_seconds": avg_duration,
            "executions_per_day": total_executions / days if days > 0 else 0
        }
    
    def cleanup_old_artifacts(self, retention_days: int = None):
        """Clean up old artifacts based on retention policy."""
        if retention_days is None:
            retention_days = self.config.get("artifacts_retention_days", 30)
        
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        artifacts_to_remove = []
        for artifact_id, artifact_path in self.artifacts_store.items():
            # Check artifact age (simplified - would use actual file timestamps)
            execution_id = artifact_id.split('_')[0]
            if execution_id in self.executions:
                execution = self.executions[execution_id]
                if execution.start_time < cutoff_date:
                    artifacts_to_remove.append(artifact_id)
        
        for artifact_id in artifacts_to_remove:
            artifact_path = self.artifacts_store.pop(artifact_id)
            if os.path.exists(artifact_path):
                os.remove(artifact_path)
        
        self.logger.info(f"Cleaned up {len(artifacts_to_remove)} old artifacts")
    
    def export_pipeline_configuration(self, pipeline_id: str, format: str = "yaml") -> str:
        """Export pipeline configuration."""
        if pipeline_id not in self.pipelines:
            raise ValueError(f"Pipeline {pipeline_id} not found")
        
        pipeline = self.pipelines[pipeline_id]
        pipeline_dict = asdict(pipeline)
        
        if format.lower() == "yaml":
            return yaml.dump(pipeline_dict, default_flow_style=False)
        else:
            return json.dumps(pipeline_dict, indent=2, default=str)
    
    def import_pipeline_configuration(self, config_data: str, format: str = "yaml") -> Pipeline:
        """Import pipeline configuration."""
        if format.lower() == "yaml":
            pipeline_spec = yaml.safe_load(config_data)
        else:
            pipeline_spec = json.loads(config_data)
        
        return self.create_pipeline(pipeline_spec)


def main():
    """Main function for CLI usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="CI/CD Pipeline Manager")
    parser.add_argument("--config", help="Configuration file path")
    parser.add_argument("--action", choices=["create", "trigger", "status", "metrics", "cleanup"], 
                       default="status", help="Action to perform")
    parser.add_argument("--pipeline-spec", help="Pipeline specification file")
    parser.add_argument("--pipeline-id", help="Pipeline ID")
    parser.add_argument("--execution-id", help="Execution ID")
    parser.add_argument("--commit", help="Commit hash")
    parser.add_argument("--branch", default="main", help="Branch name")
    parser.add_argument("--days", type=int, default=30, help="Number of days for metrics")
    
    args = parser.parse_args()
    
    manager = CICDPipelineManager(args.config)
    
    if args.action == "create":
        if not args.pipeline_spec:
            print("Pipeline specification file required")
            sys.exit(1)
        
        with open(args.pipeline_spec, 'r') as f:
            if args.pipeline_spec.endswith('.yaml') or args.pipeline_spec.endswith('.yml'):
                spec = yaml.safe_load(f)
            else:
                spec = json.load(f)
        
        pipeline = manager.create_pipeline(spec)
        print(f"Created pipeline: {pipeline.id}")
    
    elif args.action == "trigger":
        if not args.pipeline_id or not args.commit:
            print("Pipeline ID and commit hash required")
            sys.exit(1)
        
        execution = manager.trigger_pipeline(args.pipeline_id, args.commit, args.branch)
        print(f"Triggered execution: {execution.id}")
    
    elif args.action == "status":
        if args.execution_id:
            execution = manager.get_pipeline_execution(args.execution_id)
            if execution:
                print(json.dumps(asdict(execution), indent=2, default=str))
            else:
                print(f"Execution {args.execution_id} not found")
        elif args.pipeline_id:
            executions = manager.list_pipeline_executions(args.pipeline_id)
            print(f"Found {len(executions)} executions for pipeline {args.pipeline_id}")
            for execution in executions[:10]:  # Show last 10
                print(f"  {execution.id}: {execution.status.value} ({execution.start_time})")
        else:
            print("Available pipelines:")
            for pipeline_id, pipeline in manager.pipelines.items():
                print(f"  {pipeline_id}: {pipeline.name}")
    
    elif args.action == "metrics":
        if not args.pipeline_id:
            print("Pipeline ID required for metrics")
            sys.exit(1)
        
        metrics = manager.get_pipeline_metrics(args.pipeline_id, args.days)
        print(json.dumps(metrics, indent=2))
    
    elif args.action == "cleanup":
        manager.cleanup_old_artifacts(args.days)
        print(f"Cleaned up artifacts older than {args.days} days")


if __name__ == "__main__":
    main()