#!/usr/bin/env python3
"""
Cloud Resource Optimizer - Multi-Cloud Cost and Performance Optimization
Advanced cloud resource management and optimization across AWS, Azure, GCP.

Use of this code is at your own risk.
Author bears no responsibility for any damages caused by the code.
"""

import os
import sys
import json
import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
import boto3
import azure.identity
import azure.mgmt.resource
from google.cloud import compute_v1
from concurrent.futures import ThreadPoolExecutor, as_completed

class CloudProvider(Enum):
    """Supported cloud providers."""
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"
    ALIBABA = "alibaba"
    ORACLE = "oracle"

class ResourceType(Enum):
    """Cloud resource types."""
    COMPUTE = "compute"
    STORAGE = "storage"
    DATABASE = "database"
    NETWORK = "network"
    CONTAINER = "container"
    SERVERLESS = "serverless"
    AI_ML = "ai_ml"

class OptimizationAction(Enum):
    """Optimization actions."""
    RESIZE = "resize"
    TERMINATE = "terminate"
    SCHEDULE = "schedule"
    MIGRATE = "migrate"
    RESERVED_INSTANCE = "reserved_instance"
    SPOT_INSTANCE = "spot_instance"

@dataclass
class CloudResource:
    """Cloud resource representation."""
    id: str
    name: str
    provider: CloudProvider
    type: ResourceType
    region: str
    instance_type: str
    state: str
    cost_per_hour: float
    cpu_utilization: float
    memory_utilization: float
    network_utilization: float
    storage_utilization: float
    tags: Dict[str, str] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    last_accessed: Optional[datetime] = None

@dataclass
class OptimizationRecommendation:
    """Resource optimization recommendation."""
    resource_id: str
    action: OptimizationAction
    current_cost: float
    projected_cost: float
    savings_percent: float
    confidence_score: float
    reason: str
    implementation_effort: str
    risk_level: str
    estimated_savings_monthly: float

@dataclass
class CostAnalysis:
    """Cost analysis result."""
    total_monthly_cost: float
    cost_by_provider: Dict[str, float]
    cost_by_service: Dict[str, float]
    cost_by_region: Dict[str, float]
    top_expensive_resources: List[CloudResource]
    cost_trend: List[Tuple[datetime, float]]
    optimization_potential: float

class CloudResourceOptimizer:
    """Multi-cloud resource optimizer."""
    
    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)
        self.resources = {}
        self.recommendations = {}
        self.cost_history = []
        self.logger = self._setup_logging()
        self.executor = ThreadPoolExecutor(max_workers=20)
        
        # Initialize cloud clients
        self.aws_client = None
        self.azure_client = None
        self.gcp_client = None
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger('CloudResourceOptimizer')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def _load_config(self, config_path: str) -> Dict:
        """Load optimizer configuration."""
        default_config = {
            "aws": {
                "enabled": True,
                "regions": ["us-east-1", "us-west-2", "eu-west-1"],
                "access_key_id": "",
                "secret_access_key": "",
                "session_token": ""
            },
            "azure": {
                "enabled": True,
                "subscription_id": "",
                "tenant_id": "",
                "client_id": "",
                "client_secret": ""
            },
            "gcp": {
                "enabled": True,
                "project_id": "",
                "credentials_path": "",
                "regions": ["us-central1", "us-east1", "europe-west1"]
            },
            "optimization": {
                "cpu_threshold_low": 10.0,
                "cpu_threshold_high": 80.0,
                "memory_threshold_low": 20.0,
                "memory_threshold_high": 85.0,
                "idle_threshold_days": 7,
                "cost_threshold_monthly": 100.0,
                "min_confidence_score": 0.7
            },
            "notifications": {
                "email_enabled": False,
                "slack_enabled": False,
                "webhook_enabled": False
            }
        }
        
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
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
    
    async def initialize_cloud_clients(self):
        """Initialize cloud provider clients."""
        try:
            # Initialize AWS client
            if self.config["aws"]["enabled"]:
                self.aws_client = boto3.Session(
                    aws_access_key_id=self.config["aws"]["access_key_id"],
                    aws_secret_access_key=self.config["aws"]["secret_access_key"],
                    aws_session_token=self.config["aws"]["session_token"]
                )
                self.logger.info("AWS client initialized")
            
            # Initialize Azure client
            if self.config["azure"]["enabled"]:
                credential = azure.identity.ClientSecretCredential(
                    tenant_id=self.config["azure"]["tenant_id"],
                    client_id=self.config["azure"]["client_id"],
                    client_secret=self.config["azure"]["client_secret"]
                )
                self.azure_client = azure.mgmt.resource.ResourceManagementClient(
                    credential, self.config["azure"]["subscription_id"]
                )
                self.logger.info("Azure client initialized")
            
            # Initialize GCP client
            if self.config["gcp"]["enabled"]:
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.config["gcp"]["credentials_path"]
                self.gcp_client = compute_v1.InstancesClient()
                self.logger.info("GCP client initialized")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize cloud clients: {e}")
    
    async def discover_resources(self) -> Dict[str, List[CloudResource]]:
        """Discover resources across all cloud providers."""
        discovered_resources = {
            "aws": [],
            "azure": [],
            "gcp": []
        }
        
        tasks = []
        
        if self.aws_client:
            tasks.append(self._discover_aws_resources())
        
        if self.azure_client:
            tasks.append(self._discover_azure_resources())
        
        if self.gcp_client:
            tasks.append(self._discover_gcp_resources())
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Resource discovery failed: {result}")
            else:
                provider = ["aws", "azure", "gcp"][i]
                discovered_resources[provider] = result
        
        # Store discovered resources
        for provider, resources in discovered_resources.items():
            for resource in resources:
                self.resources[resource.id] = resource
        
        total_resources = sum(len(resources) for resources in discovered_resources.values())
        self.logger.info(f"Discovered {total_resources} resources across all providers")
        
        return discovered_resources
    
    async def _discover_aws_resources(self) -> List[CloudResource]:
        """Discover AWS resources."""
        resources = []
        
        try:
            for region in self.config["aws"]["regions"]:
                # EC2 instances
                ec2 = self.aws_client.client('ec2', region_name=region)
                response = ec2.describe_instances()
                
                for reservation in response['Reservations']:
                    for instance in reservation['Instances']:
                        if instance['State']['Name'] != 'terminated':
                            resource = CloudResource(
                                id=instance['InstanceId'],
                                name=self._get_aws_instance_name(instance),
                                provider=CloudProvider.AWS,
                                type=ResourceType.COMPUTE,
                                region=region,
                                instance_type=instance['InstanceType'],
                                state=instance['State']['Name'],
                                cost_per_hour=self._get_aws_instance_cost(instance['InstanceType'], region),
                                cpu_utilization=await self._get_aws_cpu_utilization(instance['InstanceId'], region),
                                memory_utilization=0.0,  # Would need CloudWatch agent
                                network_utilization=0.0,
                                storage_utilization=0.0,
                                tags={tag['Key']: tag['Value'] for tag in instance.get('Tags', [])},
                                created_at=instance['LaunchTime']
                            )
                            resources.append(resource)
                
                # RDS instances
                rds = self.aws_client.client('rds', region_name=region)
                response = rds.describe_db_instances()
                
                for db_instance in response['DBInstances']:
                    resource = CloudResource(
                        id=db_instance['DBInstanceIdentifier'],
                        name=db_instance['DBInstanceIdentifier'],
                        provider=CloudProvider.AWS,
                        type=ResourceType.DATABASE,
                        region=region,
                        instance_type=db_instance['DBInstanceClass'],
                        state=db_instance['DBInstanceStatus'],
                        cost_per_hour=self._get_aws_rds_cost(db_instance['DBInstanceClass'], region),
                        cpu_utilization=await self._get_aws_rds_cpu_utilization(
                            db_instance['DBInstanceIdentifier'], region
                        ),
                        memory_utilization=0.0,
                        network_utilization=0.0,
                        storage_utilization=0.0,
                        tags={tag['Key']: tag['Value'] for tag in db_instance.get('TagList', [])},
                        created_at=db_instance['InstanceCreateTime']
                    )
                    resources.append(resource)
                
        except Exception as e:
            self.logger.error(f"AWS resource discovery failed: {e}")
        
        return resources
    
    async def _discover_azure_resources(self) -> List[CloudResource]:
        """Discover Azure resources."""
        resources = []
        
        try:
            # Get all resource groups
            resource_groups = self.azure_client.resource_groups.list()
            
            for rg in resource_groups:
                # Get resources in each resource group
                rg_resources = self.azure_client.resources.list_by_resource_group(rg.name)
                
                for azure_resource in rg_resources:
                    if azure_resource.type == 'Microsoft.Compute/virtualMachines':
                        resource = CloudResource(
                            id=azure_resource.id,
                            name=azure_resource.name,
                            provider=CloudProvider.AZURE,
                            type=ResourceType.COMPUTE,
                            region=azure_resource.location,
                            instance_type="Standard_D2s_v3",  # Would need to get actual size
                            state="running",  # Would need to get actual state
                            cost_per_hour=0.1,  # Would need Azure pricing API
                            cpu_utilization=0.0,  # Would need Azure Monitor
                            memory_utilization=0.0,
                            network_utilization=0.0,
                            storage_utilization=0.0,
                            tags=azure_resource.tags or {}
                        )
                        resources.append(resource)
                
        except Exception as e:
            self.logger.error(f"Azure resource discovery failed: {e}")
        
        return resources
    
    async def _discover_gcp_resources(self) -> List[CloudResource]:
        """Discover GCP resources."""
        resources = []
        
        try:
            project_id = self.config["gcp"]["project_id"]
            
            for region in self.config["gcp"]["regions"]:
                # List compute instances
                request = compute_v1.ListInstancesRequest(
                    project=project_id,
                    zone=f"{region}-a"  # Simplified zone selection
                )
                
                page_result = self.gcp_client.list(request=request)
                
                for instance in page_result:
                    resource = CloudResource(
                        id=str(instance.id),
                        name=instance.name,
                        provider=CloudProvider.GCP,
                        type=ResourceType.COMPUTE,
                        region=region,
                        instance_type=instance.machine_type.split('/')[-1],
                        state=instance.status,
                        cost_per_hour=0.05,  # Would need GCP pricing API
                        cpu_utilization=0.0,  # Would need Stackdriver
                        memory_utilization=0.0,
                        network_utilization=0.0,
                        storage_utilization=0.0,
                        tags={item.key: item.value for item in instance.labels.items()},
                        created_at=datetime.fromisoformat(instance.creation_timestamp.rstrip('Z'))
                    )
                    resources.append(resource)
                
        except Exception as e:
            self.logger.error(f"GCP resource discovery failed: {e}")
        
        return resources
    
    def _get_aws_instance_name(self, instance: Dict) -> str:
        """Get AWS instance name from tags."""
        for tag in instance.get('Tags', []):
            if tag['Key'] == 'Name':
                return tag['Value']
        return instance['InstanceId']
    
    def _get_aws_instance_cost(self, instance_type: str, region: str) -> float:
        """Get AWS instance hourly cost (simplified pricing)."""
        # Simplified pricing - would use AWS Pricing API in production
        pricing_map = {
            't2.micro': 0.0116,
            't2.small': 0.023,
            't2.medium': 0.0464,
            't3.micro': 0.0104,
            't3.small': 0.0208,
            't3.medium': 0.0416,
            'm5.large': 0.096,
            'm5.xlarge': 0.192,
            'c5.large': 0.085,
            'c5.xlarge': 0.17
        }
        return pricing_map.get(instance_type, 0.1)
    
    def _get_aws_rds_cost(self, instance_class: str, region: str) -> float:
        """Get AWS RDS hourly cost (simplified pricing)."""
        pricing_map = {
            'db.t3.micro': 0.017,
            'db.t3.small': 0.034,
            'db.t3.medium': 0.068,
            'db.m5.large': 0.192,
            'db.m5.xlarge': 0.384
        }
        return pricing_map.get(instance_class, 0.1)
    
    async def _get_aws_cpu_utilization(self, instance_id: str, region: str) -> float:
        """Get AWS instance CPU utilization from CloudWatch."""
        try:
            cloudwatch = self.aws_client.client('cloudwatch', region_name=region)
            
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=24)
            
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='CPUUtilization',
                Dimensions=[
                    {
                        'Name': 'InstanceId',
                        'Value': instance_id
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Average']
            )
            
            if response['Datapoints']:
                return sum(dp['Average'] for dp in response['Datapoints']) / len(response['Datapoints'])
            
        except Exception as e:
            self.logger.warning(f"Failed to get CPU utilization for {instance_id}: {e}")
        
        return 0.0
    
    async def _get_aws_rds_cpu_utilization(self, db_instance_id: str, region: str) -> float:
        """Get AWS RDS CPU utilization from CloudWatch."""
        try:
            cloudwatch = self.aws_client.client('cloudwatch', region_name=region)
            
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=24)
            
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='CPUUtilization',
                Dimensions=[
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': db_instance_id
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Average']
            )
            
            if response['Datapoints']:
                return sum(dp['Average'] for dp in response['Datapoints']) / len(response['Datapoints'])
            
        except Exception as e:
            self.logger.warning(f"Failed to get RDS CPU utilization for {db_instance_id}: {e}")
        
        return 0.0
    
    async def analyze_costs(self) -> CostAnalysis:
        """Analyze costs across all resources."""
        total_monthly_cost = 0.0
        cost_by_provider = {}
        cost_by_service = {}
        cost_by_region = {}
        
        for resource in self.resources.values():
            monthly_cost = resource.cost_per_hour * 24 * 30
            total_monthly_cost += monthly_cost
            
            # Cost by provider
            provider_key = resource.provider.value
            cost_by_provider[provider_key] = cost_by_provider.get(provider_key, 0) + monthly_cost
            
            # Cost by service
            service_key = resource.type.value
            cost_by_service[service_key] = cost_by_service.get(service_key, 0) + monthly_cost
            
            # Cost by region
            region_key = resource.region
            cost_by_region[region_key] = cost_by_region.get(region_key, 0) + monthly_cost
        
        # Get top expensive resources
        sorted_resources = sorted(
            self.resources.values(),
            key=lambda r: r.cost_per_hour * 24 * 30,
            reverse=True
        )
        top_expensive_resources = sorted_resources[:10]
        
        # Calculate optimization potential
        optimization_potential = await self._calculate_optimization_potential()
        
        return CostAnalysis(
            total_monthly_cost=total_monthly_cost,
            cost_by_provider=cost_by_provider,
            cost_by_service=cost_by_service,
            cost_by_region=cost_by_region,
            top_expensive_resources=top_expensive_resources,
            cost_trend=[],  # Would be populated from historical data
            optimization_potential=optimization_potential
        )
    
    async def _calculate_optimization_potential(self) -> float:
        """Calculate total optimization potential."""
        recommendations = await self.generate_recommendations()
        return sum(rec.estimated_savings_monthly for rec in recommendations)
    
    async def generate_recommendations(self) -> List[OptimizationRecommendation]:
        """Generate optimization recommendations for all resources."""
        recommendations = []
        
        for resource in self.resources.values():
            resource_recommendations = await self._analyze_resource_optimization(resource)
            recommendations.extend(resource_recommendations)
        
        # Sort by potential savings
        recommendations.sort(key=lambda r: r.estimated_savings_monthly, reverse=True)
        
        # Store recommendations
        for rec in recommendations:
            self.recommendations[f"{rec.resource_id}_{rec.action.value}"] = rec
        
        self.logger.info(f"Generated {len(recommendations)} optimization recommendations")
        
        return recommendations
    
    async def _analyze_resource_optimization(self, resource: CloudResource) -> List[OptimizationRecommendation]:
        """Analyze optimization opportunities for a single resource."""
        recommendations = []
        
        # Check for underutilized resources
        if (resource.cpu_utilization < self.config["optimization"]["cpu_threshold_low"] and
            resource.memory_utilization < self.config["optimization"]["memory_threshold_low"]):
            
            # Recommend downsizing
            recommendations.append(OptimizationRecommendation(
                resource_id=resource.id,
                action=OptimizationAction.RESIZE,
                current_cost=resource.cost_per_hour * 24 * 30,
                projected_cost=resource.cost_per_hour * 0.5 * 24 * 30,
                savings_percent=50.0,
                confidence_score=0.8,
                reason="Low CPU and memory utilization detected",
                implementation_effort="Low",
                risk_level="Low",
                estimated_savings_monthly=resource.cost_per_hour * 0.5 * 24 * 30
            ))
        
        # Check for idle resources
        if (resource.last_accessed and 
            (datetime.now() - resource.last_accessed).days > self.config["optimization"]["idle_threshold_days"]):
            
            recommendations.append(OptimizationRecommendation(
                resource_id=resource.id,
                action=OptimizationAction.TERMINATE,
                current_cost=resource.cost_per_hour * 24 * 30,
                projected_cost=0.0,
                savings_percent=100.0,
                confidence_score=0.9,
                reason=f"Resource idle for {(datetime.now() - resource.last_accessed).days} days",
                implementation_effort="Low",
                risk_level="Medium",
                estimated_savings_monthly=resource.cost_per_hour * 24 * 30
            ))
        
        # Check for spot instance opportunities
        if (resource.provider == CloudProvider.AWS and 
            resource.type == ResourceType.COMPUTE and
            resource.state == "running"):
            
            recommendations.append(OptimizationRecommendation(
                resource_id=resource.id,
                action=OptimizationAction.SPOT_INSTANCE,
                current_cost=resource.cost_per_hour * 24 * 30,
                projected_cost=resource.cost_per_hour * 0.3 * 24 * 30,
                savings_percent=70.0,
                confidence_score=0.7,
                reason="Workload suitable for spot instances",
                implementation_effort="Medium",
                risk_level="Medium",
                estimated_savings_monthly=resource.cost_per_hour * 0.7 * 24 * 30
            ))
        
        # Check for reserved instance opportunities
        if (resource.cost_per_hour * 24 * 30 > self.config["optimization"]["cost_threshold_monthly"]):
            
            recommendations.append(OptimizationRecommendation(
                resource_id=resource.id,
                action=OptimizationAction.RESERVED_INSTANCE,
                current_cost=resource.cost_per_hour * 24 * 30,
                projected_cost=resource.cost_per_hour * 0.6 * 24 * 30,
                savings_percent=40.0,
                confidence_score=0.85,
                reason="High monthly cost suitable for reserved instances",
                implementation_effort="Low",
                risk_level="Low",
                estimated_savings_monthly=resource.cost_per_hour * 0.4 * 24 * 30
            ))
        
        # Filter by confidence score
        min_confidence = self.config["optimization"]["min_confidence_score"]
        return [rec for rec in recommendations if rec.confidence_score >= min_confidence]
    
    async def implement_recommendation(self, recommendation_id: str) -> bool:
        """Implement an optimization recommendation."""
        if recommendation_id not in self.recommendations:
            self.logger.error(f"Recommendation not found: {recommendation_id}")
            return False
        
        recommendation = self.recommendations[recommendation_id]
        resource = self.resources.get(recommendation.resource_id)
        
        if not resource:
            self.logger.error(f"Resource not found: {recommendation.resource_id}")
            return False
        
        try:
            if recommendation.action == OptimizationAction.RESIZE:
                return await self._resize_resource(resource, recommendation)
            elif recommendation.action == OptimizationAction.TERMINATE:
                return await self._terminate_resource(resource)
            elif recommendation.action == OptimizationAction.SPOT_INSTANCE:
                return await self._convert_to_spot_instance(resource)
            elif recommendation.action == OptimizationAction.RESERVED_INSTANCE:
                return await self._purchase_reserved_instance(resource)
            else:
                self.logger.warning(f"Action not implemented: {recommendation.action}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to implement recommendation {recommendation_id}: {e}")
            return False
    
    async def _resize_resource(self, resource: CloudResource, recommendation: OptimizationRecommendation) -> bool:
        """Resize a cloud resource."""
        self.logger.info(f"Resizing resource {resource.id}")
        
        if resource.provider == CloudProvider.AWS:
            return await self._resize_aws_instance(resource)
        elif resource.provider == CloudProvider.AZURE:
            return await self._resize_azure_instance(resource)
        elif resource.provider == CloudProvider.GCP:
            return await self._resize_gcp_instance(resource)
        
        return False
    
    async def _resize_aws_instance(self, resource: CloudResource) -> bool:
        """Resize AWS EC2 instance."""
        try:
            ec2 = self.aws_client.client('ec2', region_name=resource.region)
            
            # Stop instance
            ec2.stop_instances(InstanceIds=[resource.id])
            
            # Wait for instance to stop
            waiter = ec2.get_waiter('instance_stopped')
            waiter.wait(InstanceIds=[resource.id])
            
            # Modify instance type (simplified - would need proper size mapping)
            new_instance_type = self._get_smaller_instance_type(resource.instance_type)
            
            ec2.modify_instance_attribute(
                InstanceId=resource.id,
                InstanceType={'Value': new_instance_type}
            )
            
            # Start instance
            ec2.start_instances(InstanceIds=[resource.id])
            
            self.logger.info(f"Successfully resized AWS instance {resource.id} to {new_instance_type}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to resize AWS instance {resource.id}: {e}")
            return False
    
    async def _resize_azure_instance(self, resource: CloudResource) -> bool:
        """Resize Azure VM."""
        self.logger.info(f"Azure VM resize not implemented for {resource.id}")
        return False
    
    async def _resize_gcp_instance(self, resource: CloudResource) -> bool:
        """Resize GCP instance."""
        self.logger.info(f"GCP instance resize not implemented for {resource.id}")
        return False
    
    def _get_smaller_instance_type(self, current_type: str) -> str:
        """Get smaller instance type for downsizing."""
        # Simplified mapping - would need comprehensive mapping in production
        downsize_map = {
            't3.medium': 't3.small',
            't3.large': 't3.medium',
            'm5.large': 'm5.medium',
            'm5.xlarge': 'm5.large',
            'c5.large': 'c5.medium',
            'c5.xlarge': 'c5.large'
        }
        return downsize_map.get(current_type, current_type)
    
    async def _terminate_resource(self, resource: CloudResource) -> bool:
        """Terminate a cloud resource."""
        self.logger.warning(f"Terminating resource {resource.id}")
        
        if resource.provider == CloudProvider.AWS:
            return await self._terminate_aws_resource(resource)
        elif resource.provider == CloudProvider.AZURE:
            return await self._terminate_azure_resource(resource)
        elif resource.provider == CloudProvider.GCP:
            return await self._terminate_gcp_resource(resource)
        
        return False
    
    async def _terminate_aws_resource(self, resource: CloudResource) -> bool:
        """Terminate AWS resource."""
        try:
            if resource.type == ResourceType.COMPUTE:
                ec2 = self.aws_client.client('ec2', region_name=resource.region)
                ec2.terminate_instances(InstanceIds=[resource.id])
            elif resource.type == ResourceType.DATABASE:
                rds = self.aws_client.client('rds', region_name=resource.region)
                rds.delete_db_instance(
                    DBInstanceIdentifier=resource.id,
                    SkipFinalSnapshot=True
                )
            
            self.logger.info(f"Successfully terminated AWS resource {resource.id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to terminate AWS resource {resource.id}: {e}")
            return False
    
    async def _terminate_azure_resource(self, resource: CloudResource) -> bool:
        """Terminate Azure resource."""
        self.logger.info(f"Azure resource termination not implemented for {resource.id}")
        return False
    
    async def _terminate_gcp_resource(self, resource: CloudResource) -> bool:
        """Terminate GCP resource."""
        self.logger.info(f"GCP resource termination not implemented for {resource.id}")
        return False
    
    async def _convert_to_spot_instance(self, resource: CloudResource) -> bool:
        """Convert instance to spot instance."""
        self.logger.info(f"Converting to spot instance: {resource.id}")
        # Implementation would create new spot instance and migrate workload
        return True
    
    async def _purchase_reserved_instance(self, resource: CloudResource) -> bool:
        """Purchase reserved instance."""
        self.logger.info(f"Purchasing reserved instance for: {resource.id}")
        # Implementation would purchase reserved instance through cloud provider API
        return True
    
    def generate_optimization_report(self) -> Dict:
        """Generate comprehensive optimization report."""
        recommendations = list(self.recommendations.values())
        
        total_potential_savings = sum(rec.estimated_savings_monthly for rec in recommendations)
        
        recommendations_by_action = {}
        for rec in recommendations:
            action = rec.action.value
            if action not in recommendations_by_action:
                recommendations_by_action[action] = []
            recommendations_by_action[action].append(rec)
        
        high_impact_recommendations = [
            rec for rec in recommendations 
            if rec.estimated_savings_monthly > 100 and rec.confidence_score > 0.8
        ]
        
        return {
            "generated_at": datetime.now().isoformat(),
            "total_resources": len(self.resources),
            "total_recommendations": len(recommendations),
            "total_potential_savings_monthly": total_potential_savings,
            "recommendations_by_action": {
                action: len(recs) for action, recs in recommendations_by_action.items()
            },
            "high_impact_recommendations": len(high_impact_recommendations),
            "top_recommendations": [
                {
                    "resource_id": rec.resource_id,
                    "action": rec.action.value,
                    "savings_monthly": rec.estimated_savings_monthly,
                    "confidence": rec.confidence_score,
                    "reason": rec.reason
                }
                for rec in sorted(recommendations, key=lambda r: r.estimated_savings_monthly, reverse=True)[:10]
            ]
        }


async def main():
    """Example usage of Cloud Resource Optimizer."""
    optimizer = CloudResourceOptimizer()
    
    try:
        # Initialize cloud clients
        await optimizer.initialize_cloud_clients()
        
        # Discover resources
        print("🔍 Discovering cloud resources...")
        resources = await optimizer.discover_resources()
        
        total_resources = sum(len(r) for r in resources.values())
        print(f"✅ Discovered {total_resources} resources")
        
        # Analyze costs
        print("💰 Analyzing costs...")
        cost_analysis = await optimizer.analyze_costs()
        print(f"📊 Total monthly cost: ${cost_analysis.total_monthly_cost:.2f}")
        
        # Generate recommendations
        print("🎯 Generating optimization recommendations...")
        recommendations = await optimizer.generate_recommendations()
        print(f"💡 Generated {len(recommendations)} recommendations")
        
        # Show top recommendations
        print("\n🏆 Top 5 Recommendations:")
        for i, rec in enumerate(recommendations[:5], 1):
            print(f"  {i}. {rec.action.value.title()} {rec.resource_id}")
            print(f"     💵 Savings: ${rec.estimated_savings_monthly:.2f}/month ({rec.savings_percent:.1f}%)")
            print(f"     🎯 Confidence: {rec.confidence_score:.1%}")
            print(f"     📝 Reason: {rec.reason}")
            print()
        
        # Generate report
        report = optimizer.generate_optimization_report()
        print(f"📈 Optimization Report Generated")
        print(f"   Total Potential Savings: ${report['total_potential_savings_monthly']:.2f}/month")
        
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())