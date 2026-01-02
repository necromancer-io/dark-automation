#!/usr/bin/env python3
"""
Infrastructure Orchestrator - Enterprise-grade Infrastructure Management
Advanced orchestration system for cloud and on-premises infrastructure.

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
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

class ResourceType(Enum):
    """Supported resource types."""
    COMPUTE = "compute"
    STORAGE = "storage"
    NETWORK = "network"
    DATABASE = "database"
    LOAD_BALANCER = "load_balancer"
    SECURITY_GROUP = "security_group"
    CONTAINER = "container"
    KUBERNETES = "kubernetes"

class ResourceState(Enum):
    """Resource states."""
    PENDING = "pending"
    CREATING = "creating"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    TERMINATED = "terminated"
    ERROR = "error"

@dataclass
class Resource:
    """Infrastructure resource definition."""
    id: str
    name: str
    type: ResourceType
    state: ResourceState
    provider: str
    region: str
    config: Dict[str, Any]
    tags: Dict[str, str]
    created_at: datetime
    updated_at: datetime
    dependencies: List[str] = None
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []

@dataclass
class DeploymentPlan:
    """Infrastructure deployment plan."""
    id: str
    name: str
    description: str
    resources: List[Resource]
    execution_order: List[str]
    rollback_plan: List[str]
    estimated_duration: int
    cost_estimate: float
    created_at: datetime

class InfrastructureOrchestrator:
    """Advanced infrastructure orchestration system."""
    
    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)
        self.resources = {}
        self.deployment_history = []
        self.logger = self._setup_logging()
        self.executor = ThreadPoolExecutor(max_workers=self.config.get("max_workers", 10))
        
    def _load_config(self, config_path: str) -> Dict:
        """Load orchestrator configuration."""
        default_config = {
            "providers": {
                "aws": {
                    "enabled": True,
                    "regions": ["us-east-1", "us-west-2", "eu-west-1"],
                    "default_region": "us-east-1"
                },
                "azure": {
                    "enabled": False,
                    "regions": ["eastus", "westus2", "westeurope"],
                    "default_region": "eastus"
                },
                "gcp": {
                    "enabled": False,
                    "regions": ["us-central1", "us-east1", "europe-west1"],
                    "default_region": "us-central1"
                },
                "vmware": {
                    "enabled": True,
                    "vcenter_host": "vcenter.local",
                    "datacenter": "Datacenter1"
                }
            },
            "deployment": {
                "parallel_execution": True,
                "max_workers": 10,
                "timeout": 3600,
                "retry_attempts": 3,
                "rollback_on_failure": True
            },
            "monitoring": {
                "enabled": True,
                "metrics_interval": 60,
                "health_check_interval": 300
            },
            "security": {
                "encryption_at_rest": True,
                "encryption_in_transit": True,
                "access_logging": True,
                "compliance_checks": True
            }
        }
        
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                    user_config = yaml.safe_load(f)
                else:
                    user_config = json.load(f)
                
                # Deep merge configurations
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
        logger = logging.getLogger('InfrastructureOrchestrator')
        logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler
        file_handler = logging.FileHandler('infrastructure_orchestrator.log')
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    def create_deployment_plan(self, infrastructure_spec: Dict) -> DeploymentPlan:
        """Create deployment plan from infrastructure specification."""
        self.logger.info("Creating deployment plan")
        
        plan_id = f"plan_{int(time.time())}"
        resources = []
        
        # Parse infrastructure specification
        for resource_spec in infrastructure_spec.get("resources", []):
            resource = Resource(
                id=resource_spec["id"],
                name=resource_spec["name"],
                type=ResourceType(resource_spec["type"]),
                state=ResourceState.PENDING,
                provider=resource_spec["provider"],
                region=resource_spec.get("region", self._get_default_region(resource_spec["provider"])),
                config=resource_spec.get("config", {}),
                tags=resource_spec.get("tags", {}),
                created_at=datetime.now(),
                updated_at=datetime.now(),
                dependencies=resource_spec.get("dependencies", [])
            )
            resources.append(resource)
        
        # Calculate execution order based on dependencies
        execution_order = self._calculate_execution_order(resources)
        rollback_plan = list(reversed(execution_order))
        
        # Estimate deployment duration and cost
        estimated_duration = self._estimate_deployment_duration(resources)
        cost_estimate = self._estimate_deployment_cost(resources)
        
        plan = DeploymentPlan(
            id=plan_id,
            name=infrastructure_spec.get("name", f"Deployment {plan_id}"),
            description=infrastructure_spec.get("description", ""),
            resources=resources,
            execution_order=execution_order,
            rollback_plan=rollback_plan,
            estimated_duration=estimated_duration,
            cost_estimate=cost_estimate,
            created_at=datetime.now()
        )
        
        self.logger.info(f"Created deployment plan {plan_id} with {len(resources)} resources")
        return plan
    
    def _get_default_region(self, provider: str) -> str:
        """Get default region for provider."""
        return self.config["providers"].get(provider, {}).get("default_region", "us-east-1")
    
    def _calculate_execution_order(self, resources: List[Resource]) -> List[str]:
        """Calculate optimal execution order based on dependencies."""
        # Topological sort for dependency resolution
        in_degree = {resource.id: 0 for resource in resources}
        graph = {resource.id: [] for resource in resources}
        
        # Build dependency graph
        for resource in resources:
            for dep in resource.dependencies:
                if dep in graph:
                    graph[dep].append(resource.id)
                    in_degree[resource.id] += 1
        
        # Topological sort
        queue = [resource_id for resource_id, degree in in_degree.items() if degree == 0]
        execution_order = []
        
        while queue:
            current = queue.pop(0)
            execution_order.append(current)
            
            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        return execution_order
    
    def _estimate_deployment_duration(self, resources: List[Resource]) -> int:
        """Estimate deployment duration in seconds."""
        # Base time estimates per resource type (in seconds)
        time_estimates = {
            ResourceType.COMPUTE: 300,
            ResourceType.STORAGE: 120,
            ResourceType.NETWORK: 60,
            ResourceType.DATABASE: 600,
            ResourceType.LOAD_BALANCER: 180,
            ResourceType.SECURITY_GROUP: 30,
            ResourceType.CONTAINER: 90,
            ResourceType.KUBERNETES: 480
        }
        
        total_time = 0
        for resource in resources:
            base_time = time_estimates.get(resource.type, 120)
            # Add complexity factor based on configuration
            complexity_factor = len(resource.config) * 0.1 + 1
            total_time += int(base_time * complexity_factor)
        
        # Account for parallel execution
        if self.config["deployment"]["parallel_execution"]:
            total_time = int(total_time * 0.6)  # 40% reduction for parallelization
        
        return total_time
    
    def _estimate_deployment_cost(self, resources: List[Resource]) -> float:
        """Estimate deployment cost in USD."""
        # Base cost estimates per resource type (monthly)
        cost_estimates = {
            ResourceType.COMPUTE: 50.0,
            ResourceType.STORAGE: 10.0,
            ResourceType.NETWORK: 5.0,
            ResourceType.DATABASE: 100.0,
            ResourceType.LOAD_BALANCER: 25.0,
            ResourceType.SECURITY_GROUP: 0.0,
            ResourceType.CONTAINER: 30.0,
            ResourceType.KUBERNETES: 75.0
        }
        
        total_cost = 0.0
        for resource in resources:
            base_cost = cost_estimates.get(resource.type, 20.0)
            # Add configuration-based cost factors
            if "instance_type" in resource.config:
                if "large" in resource.config["instance_type"]:
                    base_cost *= 2
                elif "xlarge" in resource.config["instance_type"]:
                    base_cost *= 4
            
            total_cost += base_cost
        
        return round(total_cost, 2)
    
    async def execute_deployment_plan(self, plan: DeploymentPlan) -> Dict:
        """Execute deployment plan asynchronously."""
        self.logger.info(f"Starting execution of deployment plan {plan.id}")
        
        start_time = datetime.now()
        execution_results = {
            "plan_id": plan.id,
            "start_time": start_time.isoformat(),
            "status": "running",
            "resources": {},
            "errors": []
        }
        
        try:
            if self.config["deployment"]["parallel_execution"]:
                await self._execute_parallel_deployment(plan, execution_results)
            else:
                await self._execute_sequential_deployment(plan, execution_results)
            
            execution_results["status"] = "completed"
            
        except Exception as e:
            self.logger.error(f"Deployment failed: {e}")
            execution_results["status"] = "failed"
            execution_results["errors"].append(str(e))
            
            if self.config["deployment"]["rollback_on_failure"]:
                await self._execute_rollback(plan, execution_results)
        
        execution_results["end_time"] = datetime.now().isoformat()
        execution_results["duration"] = (datetime.now() - start_time).total_seconds()
        
        self.deployment_history.append(execution_results)
        return execution_results
    
    async def _execute_parallel_deployment(self, plan: DeploymentPlan, results: Dict):
        """Execute deployment plan in parallel."""
        # Group resources by dependency level
        dependency_levels = self._group_by_dependency_level(plan.resources)
        
        for level, resource_ids in dependency_levels.items():
            self.logger.info(f"Executing dependency level {level} with {len(resource_ids)} resources")
            
            # Execute resources at the same dependency level in parallel
            tasks = []
            for resource_id in resource_ids:
                resource = next(r for r in plan.resources if r.id == resource_id)
                task = asyncio.create_task(self._deploy_resource(resource))
                tasks.append((resource_id, task))
            
            # Wait for all tasks in this level to complete
            for resource_id, task in tasks:
                try:
                    result = await task
                    results["resources"][resource_id] = result
                    self.logger.info(f"Successfully deployed resource {resource_id}")
                except Exception as e:
                    error_msg = f"Failed to deploy resource {resource_id}: {e}"
                    self.logger.error(error_msg)
                    results["errors"].append(error_msg)
                    raise e
    
    async def _execute_sequential_deployment(self, plan: DeploymentPlan, results: Dict):
        """Execute deployment plan sequentially."""
        for resource_id in plan.execution_order:
            resource = next(r for r in plan.resources if r.id == resource_id)
            
            try:
                result = await self._deploy_resource(resource)
                results["resources"][resource_id] = result
                self.logger.info(f"Successfully deployed resource {resource_id}")
            except Exception as e:
                error_msg = f"Failed to deploy resource {resource_id}: {e}"
                self.logger.error(error_msg)
                results["errors"].append(error_msg)
                raise e
    
    def _group_by_dependency_level(self, resources: List[Resource]) -> Dict[int, List[str]]:
        """Group resources by dependency level for parallel execution."""
        levels = {}
        resource_levels = {}
        
        # Calculate dependency level for each resource
        def calculate_level(resource_id: str, visited: set = None) -> int:
            if visited is None:
                visited = set()
            
            if resource_id in visited:
                return 0  # Circular dependency, treat as level 0
            
            if resource_id in resource_levels:
                return resource_levels[resource_id]
            
            visited.add(resource_id)
            resource = next((r for r in resources if r.id == resource_id), None)
            
            if not resource or not resource.dependencies:
                level = 0
            else:
                max_dep_level = 0
                for dep in resource.dependencies:
                    dep_level = calculate_level(dep, visited.copy())
                    max_dep_level = max(max_dep_level, dep_level)
                level = max_dep_level + 1
            
            resource_levels[resource_id] = level
            return level
        
        # Calculate levels for all resources
        for resource in resources:
            level = calculate_level(resource.id)
            if level not in levels:
                levels[level] = []
            levels[level].append(resource.id)
        
        return levels
    
    async def _deploy_resource(self, resource: Resource) -> Dict:
        """Deploy individual resource."""
        self.logger.info(f"Deploying resource {resource.id} ({resource.type.value})")
        
        resource.state = ResourceState.CREATING
        resource.updated_at = datetime.now()
        
        # Simulate deployment based on provider and resource type
        deployment_time = self._get_deployment_time(resource)
        
        # Simulate deployment process
        await asyncio.sleep(deployment_time)
        
        # Simulate deployment result
        if resource.provider == "aws":
            result = await self._deploy_aws_resource(resource)
        elif resource.provider == "azure":
            result = await self._deploy_azure_resource(resource)
        elif resource.provider == "gcp":
            result = await self._deploy_gcp_resource(resource)
        elif resource.provider == "vmware":
            result = await self._deploy_vmware_resource(resource)
        else:
            raise ValueError(f"Unsupported provider: {resource.provider}")
        
        resource.state = ResourceState.RUNNING
        resource.updated_at = datetime.now()
        self.resources[resource.id] = resource
        
        return result
    
    def _get_deployment_time(self, resource: Resource) -> float:
        """Get simulated deployment time for resource."""
        base_times = {
            ResourceType.COMPUTE: 2.0,
            ResourceType.STORAGE: 1.0,
            ResourceType.NETWORK: 0.5,
            ResourceType.DATABASE: 5.0,
            ResourceType.LOAD_BALANCER: 1.5,
            ResourceType.SECURITY_GROUP: 0.2,
            ResourceType.CONTAINER: 0.8,
            ResourceType.KUBERNETES: 3.0
        }
        return base_times.get(resource.type, 1.0)
    
    async def _deploy_aws_resource(self, resource: Resource) -> Dict:
        """Deploy AWS resource."""
        # Simulate AWS deployment
        return {
            "provider": "aws",
            "resource_id": f"aws-{resource.id}",
            "arn": f"arn:aws:{resource.type.value}:{resource.region}:123456789012:{resource.name}",
            "status": "running",
            "endpoint": f"https://{resource.name}.{resource.region}.amazonaws.com"
        }
    
    async def _deploy_azure_resource(self, resource: Resource) -> Dict:
        """Deploy Azure resource."""
        # Simulate Azure deployment
        return {
            "provider": "azure",
            "resource_id": f"azure-{resource.id}",
            "resource_group": "default-rg",
            "status": "running",
            "endpoint": f"https://{resource.name}.{resource.region}.cloudapp.azure.com"
        }
    
    async def _deploy_gcp_resource(self, resource: Resource) -> Dict:
        """Deploy GCP resource."""
        # Simulate GCP deployment
        return {
            "provider": "gcp",
            "resource_id": f"gcp-{resource.id}",
            "project": "default-project",
            "status": "running",
            "endpoint": f"https://{resource.name}.{resource.region}.googlecloud.com"
        }
    
    async def _deploy_vmware_resource(self, resource: Resource) -> Dict:
        """Deploy VMware resource."""
        # Simulate VMware deployment
        return {
            "provider": "vmware",
            "resource_id": f"vm-{resource.id}",
            "datacenter": self.config["providers"]["vmware"]["datacenter"],
            "status": "running",
            "ip_address": f"192.168.1.{hash(resource.id) % 254 + 1}"
        }
    
    async def _execute_rollback(self, plan: DeploymentPlan, results: Dict):
        """Execute rollback plan."""
        self.logger.info(f"Starting rollback for deployment plan {plan.id}")
        
        rollback_results = []
        
        for resource_id in plan.rollback_plan:
            if resource_id in results["resources"]:
                try:
                    await self._destroy_resource(resource_id)
                    rollback_results.append({"resource_id": resource_id, "status": "destroyed"})
                except Exception as e:
                    rollback_results.append({"resource_id": resource_id, "status": "failed", "error": str(e)})
        
        results["rollback"] = rollback_results
    
    async def _destroy_resource(self, resource_id: str):
        """Destroy deployed resource."""
        if resource_id in self.resources:
            resource = self.resources[resource_id]
            resource.state = ResourceState.STOPPING
            
            # Simulate destruction time
            await asyncio.sleep(0.5)
            
            resource.state = ResourceState.TERMINATED
            del self.resources[resource_id]
    
    def get_deployment_status(self, plan_id: str) -> Optional[Dict]:
        """Get deployment status."""
        for deployment in self.deployment_history:
            if deployment["plan_id"] == plan_id:
                return deployment
        return None
    
    def list_resources(self, provider: str = None, resource_type: ResourceType = None) -> List[Resource]:
        """List deployed resources with optional filtering."""
        resources = list(self.resources.values())
        
        if provider:
            resources = [r for r in resources if r.provider == provider]
        
        if resource_type:
            resources = [r for r in resources if r.type == resource_type]
        
        return resources
    
    def generate_infrastructure_report(self) -> Dict:
        """Generate comprehensive infrastructure report."""
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_resources": len(self.resources),
                "resources_by_provider": {},
                "resources_by_type": {},
                "resources_by_state": {}
            },
            "deployments": {
                "total_deployments": len(self.deployment_history),
                "successful_deployments": len([d for d in self.deployment_history if d["status"] == "completed"]),
                "failed_deployments": len([d for d in self.deployment_history if d["status"] == "failed"])
            },
            "cost_analysis": self._generate_cost_analysis(),
            "security_compliance": self._check_security_compliance(),
            "recommendations": self._generate_recommendations()
        }
        
        # Count resources by various dimensions
        for resource in self.resources.values():
            # By provider
            if resource.provider not in report["summary"]["resources_by_provider"]:
                report["summary"]["resources_by_provider"][resource.provider] = 0
            report["summary"]["resources_by_provider"][resource.provider] += 1
            
            # By type
            type_name = resource.type.value
            if type_name not in report["summary"]["resources_by_type"]:
                report["summary"]["resources_by_type"][type_name] = 0
            report["summary"]["resources_by_type"][type_name] += 1
            
            # By state
            state_name = resource.state.value
            if state_name not in report["summary"]["resources_by_state"]:
                report["summary"]["resources_by_state"][state_name] = 0
            report["summary"]["resources_by_state"][state_name] += 1
        
        return report
    
    def _generate_cost_analysis(self) -> Dict:
        """Generate cost analysis report."""
        total_monthly_cost = 0.0
        cost_by_provider = {}
        cost_by_type = {}
        
        for resource in self.resources.values():
            # Estimate monthly cost (simplified)
            monthly_cost = self._estimate_resource_monthly_cost(resource)
            total_monthly_cost += monthly_cost
            
            # By provider
            if resource.provider not in cost_by_provider:
                cost_by_provider[resource.provider] = 0.0
            cost_by_provider[resource.provider] += monthly_cost
            
            # By type
            type_name = resource.type.value
            if type_name not in cost_by_type:
                cost_by_type[type_name] = 0.0
            cost_by_type[type_name] += monthly_cost
        
        return {
            "total_monthly_cost": round(total_monthly_cost, 2),
            "cost_by_provider": cost_by_provider,
            "cost_by_type": cost_by_type,
            "projected_annual_cost": round(total_monthly_cost * 12, 2)
        }
    
    def _estimate_resource_monthly_cost(self, resource: Resource) -> float:
        """Estimate monthly cost for a resource."""
        base_costs = {
            ResourceType.COMPUTE: 50.0,
            ResourceType.STORAGE: 10.0,
            ResourceType.NETWORK: 5.0,
            ResourceType.DATABASE: 100.0,
            ResourceType.LOAD_BALANCER: 25.0,
            ResourceType.SECURITY_GROUP: 0.0,
            ResourceType.CONTAINER: 30.0,
            ResourceType.KUBERNETES: 75.0
        }
        return base_costs.get(resource.type, 20.0)
    
    def _check_security_compliance(self) -> Dict:
        """Check security compliance across resources."""
        compliance_checks = {
            "encryption_at_rest": 0,
            "encryption_in_transit": 0,
            "access_logging": 0,
            "network_isolation": 0,
            "backup_configured": 0
        }
        
        total_resources = len(self.resources)
        
        for resource in self.resources.values():
            # Simulate compliance checks
            if resource.config.get("encryption", False):
                compliance_checks["encryption_at_rest"] += 1
            
            if resource.config.get("ssl_enabled", False):
                compliance_checks["encryption_in_transit"] += 1
            
            if resource.config.get("logging_enabled", False):
                compliance_checks["access_logging"] += 1
            
            if resource.config.get("vpc_id") or resource.config.get("subnet_id"):
                compliance_checks["network_isolation"] += 1
            
            if resource.config.get("backup_enabled", False):
                compliance_checks["backup_configured"] += 1
        
        # Calculate compliance percentages
        compliance_percentages = {}
        for check, count in compliance_checks.items():
            compliance_percentages[check] = round((count / total_resources * 100), 2) if total_resources > 0 else 0
        
        return {
            "compliance_checks": compliance_checks,
            "compliance_percentages": compliance_percentages,
            "overall_compliance_score": round(sum(compliance_percentages.values()) / len(compliance_percentages), 2)
        }
    
    def _generate_recommendations(self) -> List[Dict]:
        """Generate optimization recommendations."""
        recommendations = []
        
        # Cost optimization recommendations
        high_cost_resources = [r for r in self.resources.values() 
                             if self._estimate_resource_monthly_cost(r) > 100]
        
        if high_cost_resources:
            recommendations.append({
                "type": "cost_optimization",
                "priority": "high",
                "title": "High-cost resources detected",
                "description": f"Found {len(high_cost_resources)} resources with high monthly costs",
                "action": "Review resource sizing and consider optimization"
            })
        
        # Security recommendations
        unencrypted_resources = [r for r in self.resources.values() 
                               if not r.config.get("encryption", False)]
        
        if unencrypted_resources:
            recommendations.append({
                "type": "security",
                "priority": "critical",
                "title": "Unencrypted resources found",
                "description": f"Found {len(unencrypted_resources)} resources without encryption",
                "action": "Enable encryption for all sensitive resources"
            })
        
        # Performance recommendations
        if len(self.resources) > 50:
            recommendations.append({
                "type": "performance",
                "priority": "medium",
                "title": "Large infrastructure detected",
                "description": "Consider implementing resource tagging and monitoring",
                "action": "Implement comprehensive monitoring and alerting"
            })
        
        return recommendations


def main():
    """Main function for CLI usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Infrastructure Orchestrator")
    parser.add_argument("--config", help="Configuration file path")
    parser.add_argument("--spec", help="Infrastructure specification file")
    parser.add_argument("--action", choices=["plan", "deploy", "status", "report"], 
                       default="plan", help="Action to perform")
    parser.add_argument("--plan-id", help="Deployment plan ID for status/deploy actions")
    
    args = parser.parse_args()
    
    orchestrator = InfrastructureOrchestrator(args.config)
    
    if args.action == "plan":
        if not args.spec:
            print("Infrastructure specification file required for planning")
            sys.exit(1)
        
        with open(args.spec, 'r') as f:
            if args.spec.endswith('.yaml') or args.spec.endswith('.yml'):
                spec = yaml.safe_load(f)
            else:
                spec = json.load(f)
        
        plan = orchestrator.create_deployment_plan(spec)
        print(json.dumps(asdict(plan), indent=2, default=str))
    
    elif args.action == "deploy":
        if not args.plan_id:
            print("Plan ID required for deployment")
            sys.exit(1)
        
        # In a real implementation, you would load the plan from storage
        print(f"Deployment of plan {args.plan_id} would be executed here")
    
    elif args.action == "status":
        if args.plan_id:
            status = orchestrator.get_deployment_status(args.plan_id)
            if status:
                print(json.dumps(status, indent=2))
            else:
                print(f"No deployment found with ID {args.plan_id}")
        else:
            resources = orchestrator.list_resources()
            print(f"Total resources: {len(resources)}")
            for resource in resources:
                print(f"  {resource.id}: {resource.type.value} ({resource.state.value})")
    
    elif args.action == "report":
        report = orchestrator.generate_infrastructure_report()
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()