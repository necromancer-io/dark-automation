#!/usr/bin/env python3
"""
Edge Computing Orchestrator - Distributed Edge Infrastructure Management
Advanced edge computing platform for distributed workload orchestration.

Use of this code is at your own risk.
Author bears no responsibility for any damages caused by the code.
"""

import os
import sys
import json
import time
import asyncio
import logging
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
import hashlib
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
import socket
import subprocess

class EdgeNodeType(Enum):
    """Edge node types."""
    GATEWAY = "gateway"
    COMPUTE = "compute"
    STORAGE = "storage"
    SENSOR_HUB = "sensor_hub"
    AI_ACCELERATOR = "ai_accelerator"
    NETWORK_FUNCTION = "network_function"

class WorkloadType(Enum):
    """Edge workload types."""
    CONTAINER = "container"
    FUNCTION = "function"
    AI_MODEL = "ai_model"
    DATA_PROCESSING = "data_processing"
    STREAM_PROCESSING = "stream_processing"
    MICROSERVICE = "microservice"

class NodeStatus(Enum):
    """Edge node status."""
    ONLINE = "online"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"
    OVERLOADED = "overloaded"
    ERROR = "error"

@dataclass
class EdgeNode:
    """Edge computing node."""
    node_id: str
    name: str
    node_type: EdgeNodeType
    location: str
    ip_address: str
    port: int
    status: NodeStatus
    cpu_cores: int
    memory_gb: float
    storage_gb: float
    gpu_count: int = 0
    network_bandwidth_mbps: float = 1000.0
    capabilities: List[str] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)
    last_heartbeat: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.now)

@dataclass
class ResourceUsage:
    """Resource usage metrics."""
    node_id: str
    cpu_percent: float
    memory_percent: float
    storage_percent: float
    network_rx_mbps: float
    network_tx_mbps: float
    gpu_percent: float = 0.0
    temperature_celsius: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)

@dataclass
class EdgeWorkload:
    """Edge workload definition."""
    workload_id: str
    name: str
    workload_type: WorkloadType
    image: str
    resource_requirements: Dict[str, Any]
    environment_variables: Dict[str, str] = field(default_factory=dict)
    ports: List[int] = field(default_factory=list)
    volumes: List[str] = field(default_factory=list)
    placement_constraints: Dict[str, Any] = field(default_factory=dict)
    scaling_policy: Dict[str, Any] = field(default_factory=dict)
    health_check: Optional[Dict[str, Any]] = None
    created_at: datetime = field(default_factory=datetime.now)

@dataclass
class WorkloadDeployment:
    """Workload deployment instance."""
    deployment_id: str
    workload_id: str
    node_id: str
    status: str
    container_id: Optional[str] = None
    started_at: Optional[datetime] = None
    stopped_at: Optional[datetime] = None
    restart_count: int = 0
    logs: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)

class EdgeNodeManager:
    """Edge node management and monitoring."""
    
    def __init__(self):
        self.nodes = {}
        self.resource_usage = {}
        self.logger = logging.getLogger('EdgeNodeManager')
        
    def register_node(self, node: EdgeNode) -> bool:
        """Register edge node."""
        try:
            self.nodes[node.node_id] = node
            self.resource_usage[node.node_id] = []
            
            self.logger.info(f"Registered edge node: {node.name} ({node.node_id})")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to register node {node.node_id}: {e}")
            return False
    
    def unregister_node(self, node_id: str) -> bool:
        """Unregister edge node."""
        try:
            if node_id in self.nodes:
                node = self.nodes[node_id]
                del self.nodes[node_id]
                
                if node_id in self.resource_usage:
                    del self.resource_usage[node_id]
                
                self.logger.info(f"Unregistered edge node: {node_id}")
                return True
            else:
                self.logger.warning(f"Node not found: {node_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to unregister node {node_id}: {e}")
            return False
    
    def update_node_status(self, node_id: str, status: NodeStatus, 
                          resource_usage: ResourceUsage = None):
        """Update node status and resource usage."""
        if node_id not in self.nodes:
            return False
        
        node = self.nodes[node_id]
        node.status = status
        node.last_heartbeat = datetime.now()
        
        if resource_usage:
            if node_id not in self.resource_usage:
                self.resource_usage[node_id] = []
            
            self.resource_usage[node_id].append(resource_usage)
            
            # Keep only recent metrics (last 1000 entries)
            if len(self.resource_usage[node_id]) > 1000:
                self.resource_usage[node_id] = self.resource_usage[node_id][-1000:]
        
        return True
    
    def get_node_metrics(self, node_id: str, hours: int = 1) -> List[ResourceUsage]:
        """Get node resource usage metrics."""
        if node_id not in self.resource_usage:
            return []
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        return [
            usage for usage in self.resource_usage[node_id]
            if usage.timestamp >= cutoff_time
        ]
    
    def find_suitable_nodes(self, requirements: Dict[str, Any], 
                           count: int = 1) -> List[EdgeNode]:
        """Find nodes that meet resource requirements."""
        suitable_nodes = []
        
        required_cpu = requirements.get('cpu_cores', 1)
        required_memory = requirements.get('memory_gb', 1.0)
        required_storage = requirements.get('storage_gb', 1.0)
        required_capabilities = requirements.get('capabilities', [])
        location_preference = requirements.get('location')
        
        for node in self.nodes.values():
            if node.status != NodeStatus.ONLINE:
                continue
            
            # Check resource availability
            current_usage = self._get_current_usage(node.node_id)
            
            available_cpu = node.cpu_cores * (1 - current_usage.get('cpu_percent', 0) / 100)
            available_memory = node.memory_gb * (1 - current_usage.get('memory_percent', 0) / 100)
            available_storage = node.storage_gb * (1 - current_usage.get('storage_percent', 0) / 100)
            
            if (available_cpu >= required_cpu and
                available_memory >= required_memory and
                available_storage >= required_storage):
                
                # Check capabilities
                if all(cap in node.capabilities for cap in required_capabilities):
                    # Check location preference
                    if not location_preference or node.location == location_preference:
                        suitable_nodes.append(node)
        
        # Sort by resource availability (prefer less loaded nodes)
        suitable_nodes.sort(key=lambda n: self._calculate_node_load(n.node_id))
        
        return suitable_nodes[:count]
    
    def _get_current_usage(self, node_id: str) -> Dict[str, float]:
        """Get current resource usage for node."""
        if node_id not in self.resource_usage or not self.resource_usage[node_id]:
            return {'cpu_percent': 0, 'memory_percent': 0, 'storage_percent': 0}
        
        latest_usage = self.resource_usage[node_id][-1]
        
        return {
            'cpu_percent': latest_usage.cpu_percent,
            'memory_percent': latest_usage.memory_percent,
            'storage_percent': latest_usage.storage_percent
        }
    
    def _calculate_node_load(self, node_id: str) -> float:
        """Calculate overall node load score."""
        usage = self._get_current_usage(node_id)
        
        # Weighted average of resource usage
        load_score = (
            usage['cpu_percent'] * 0.4 +
            usage['memory_percent'] * 0.4 +
            usage['storage_percent'] * 0.2
        )
        
        return load_score

class WorkloadScheduler:
    """Edge workload scheduler."""
    
    def __init__(self, node_manager: EdgeNodeManager):
        self.node_manager = node_manager
        self.workloads = {}
        self.deployments = {}
        self.logger = logging.getLogger('WorkloadScheduler')
        
    def register_workload(self, workload: EdgeWorkload) -> bool:
        """Register workload definition."""
        try:
            self.workloads[workload.workload_id] = workload
            self.logger.info(f"Registered workload: {workload.name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to register workload {workload.workload_id}: {e}")
            return False
    
    def schedule_workload(self, workload_id: str, replicas: int = 1) -> List[str]:
        """Schedule workload on suitable edge nodes."""
        if workload_id not in self.workloads:
            raise ValueError(f"Workload {workload_id} not found")
        
        workload = self.workloads[workload_id]
        deployment_ids = []
        
        # Find suitable nodes
        suitable_nodes = self.node_manager.find_suitable_nodes(
            workload.resource_requirements, 
            count=replicas
        )
        
        if len(suitable_nodes) < replicas:
            self.logger.warning(f"Only {len(suitable_nodes)} suitable nodes found for {replicas} replicas")
        
        # Deploy to selected nodes
        for i, node in enumerate(suitable_nodes):
            deployment_id = self._deploy_to_node(workload, node)
            if deployment_id:
                deployment_ids.append(deployment_id)
        
        self.logger.info(f"Scheduled workload {workload_id} on {len(deployment_ids)} nodes")
        
        return deployment_ids
    
    def _deploy_to_node(self, workload: EdgeWorkload, node: EdgeNode) -> Optional[str]:
        """Deploy workload to specific node."""
        try:
            deployment_id = str(uuid.uuid4())
            
            deployment = WorkloadDeployment(
                deployment_id=deployment_id,
                workload_id=workload.workload_id,
                node_id=node.node_id,
                status="deploying"
            )
            
            self.deployments[deployment_id] = deployment
            
            # Simulate deployment process
            if workload.workload_type == WorkloadType.CONTAINER:
                success = self._deploy_container(workload, node, deployment)
            elif workload.workload_type == WorkloadType.FUNCTION:
                success = self._deploy_function(workload, node, deployment)
            elif workload.workload_type == WorkloadType.AI_MODEL:
                success = self._deploy_ai_model(workload, node, deployment)
            else:
                success = self._deploy_generic(workload, node, deployment)
            
            if success:
                deployment.status = "running"
                deployment.started_at = datetime.now()
                self.logger.info(f"Deployed workload {workload.workload_id} to node {node.node_id}")
                return deployment_id
            else:
                deployment.status = "failed"
                self.logger.error(f"Failed to deploy workload {workload.workload_id} to node {node.node_id}")
                return None
                
        except Exception as e:
            self.logger.error(f"Deployment error: {e}")
            return None
    
    def _deploy_container(self, workload: EdgeWorkload, node: EdgeNode, 
                         deployment: WorkloadDeployment) -> bool:
        """Deploy container workload."""
        try:
            # Simulate container deployment
            container_id = f"container_{deployment.deployment_id[:8]}"
            deployment.container_id = container_id
            
            # In real implementation, would use Docker API or containerd
            self.logger.info(f"Starting container {workload.image} on node {node.node_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Container deployment failed: {e}")
            return False
    
    def _deploy_function(self, workload: EdgeWorkload, node: EdgeNode,
                        deployment: WorkloadDeployment) -> bool:
        """Deploy serverless function."""
        try:
            # Simulate function deployment
            self.logger.info(f"Deploying function {workload.name} on node {node.node_id}")
            
            # In real implementation, would integrate with FaaS platform
            return True
            
        except Exception as e:
            self.logger.error(f"Function deployment failed: {e}")
            return False
    
    def _deploy_ai_model(self, workload: EdgeWorkload, node: EdgeNode,
                        deployment: WorkloadDeployment) -> bool:
        """Deploy AI model workload."""
        try:
            # Check if node has AI acceleration capabilities
            if "ai_accelerator" not in node.capabilities and node.gpu_count == 0:
                self.logger.warning(f"Node {node.node_id} lacks AI acceleration for model {workload.name}")
            
            self.logger.info(f"Deploying AI model {workload.name} on node {node.node_id}")
            
            # In real implementation, would use TensorFlow Serving, TorchServe, etc.
            return True
            
        except Exception as e:
            self.logger.error(f"AI model deployment failed: {e}")
            return False
    
    def _deploy_generic(self, workload: EdgeWorkload, node: EdgeNode,
                       deployment: WorkloadDeployment) -> bool:
        """Deploy generic workload."""
        try:
            self.logger.info(f"Deploying {workload.workload_type.value} {workload.name} on node {node.node_id}")
            return True
        except Exception as e:
            self.logger.error(f"Generic deployment failed: {e}")
            return False
    
    def stop_deployment(self, deployment_id: str) -> bool:
        """Stop workload deployment."""
        if deployment_id not in self.deployments:
            return False
        
        deployment = self.deployments[deployment_id]
        
        try:
            # Simulate stopping workload
            if deployment.container_id:
                self.logger.info(f"Stopping container {deployment.container_id}")
            
            deployment.status = "stopped"
            deployment.stopped_at = datetime.now()
            
            self.logger.info(f"Stopped deployment {deployment_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to stop deployment {deployment_id}: {e}")
            return False
    
    def scale_workload(self, workload_id: str, target_replicas: int) -> List[str]:
        """Scale workload to target number of replicas."""
        current_deployments = [
            d for d in self.deployments.values()
            if d.workload_id == workload_id and d.status == "running"
        ]
        
        current_replicas = len(current_deployments)
        
        if target_replicas > current_replicas:
            # Scale up
            additional_replicas = target_replicas - current_replicas
            new_deployments = self.schedule_workload(workload_id, additional_replicas)
            
            self.logger.info(f"Scaled up workload {workload_id} by {additional_replicas} replicas")
            return new_deployments
        
        elif target_replicas < current_replicas:
            # Scale down
            excess_replicas = current_replicas - target_replicas
            deployments_to_stop = current_deployments[:excess_replicas]
            
            for deployment in deployments_to_stop:
                self.stop_deployment(deployment.deployment_id)
            
            self.logger.info(f"Scaled down workload {workload_id} by {excess_replicas} replicas")
            return [d.deployment_id for d in deployments_to_stop]
        
        else:
            self.logger.info(f"Workload {workload_id} already at target scale ({target_replicas})")
            return []
    
    def get_workload_status(self, workload_id: str) -> Dict[str, Any]:
        """Get workload deployment status."""
        deployments = [
            d for d in self.deployments.values()
            if d.workload_id == workload_id
        ]
        
        status_counts = {}
        for deployment in deployments:
            status = deployment.status
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            'workload_id': workload_id,
            'total_deployments': len(deployments),
            'status_counts': status_counts,
            'deployments': [
                {
                    'deployment_id': d.deployment_id,
                    'node_id': d.node_id,
                    'status': d.status,
                    'started_at': d.started_at.isoformat() if d.started_at else None
                }
                for d in deployments
            ]
        }

class EdgeOrchestrator:
    """Main edge computing orchestrator."""
    
    def __init__(self):
        self.node_manager = EdgeNodeManager()
        self.scheduler = WorkloadScheduler(self.node_manager)
        self.logger = self._setup_logging()
        self.monitoring_active = False
        self.executor = ThreadPoolExecutor(max_workers=10)
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger('EdgeOrchestrator')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def register_edge_node(self, node: EdgeNode) -> bool:
        """Register new edge node."""
        return self.node_manager.register_node(node)
    
    def deploy_workload(self, workload: EdgeWorkload, replicas: int = 1) -> List[str]:
        """Deploy workload to edge infrastructure."""
        # Register workload
        self.scheduler.register_workload(workload)
        
        # Schedule deployment
        deployment_ids = self.scheduler.schedule_workload(workload.workload_id, replicas)
        
        self.logger.info(f"Deployed workload {workload.name} with {len(deployment_ids)} replicas")
        
        return deployment_ids
    
    def start_monitoring(self):
        """Start edge infrastructure monitoring."""
        if self.monitoring_active:
            return
        
        self.monitoring_active = True
        
        # Start monitoring thread
        monitoring_thread = threading.Thread(target=self._monitoring_loop)
        monitoring_thread.daemon = True
        monitoring_thread.start()
        
        self.logger.info("Started edge infrastructure monitoring")
    
    def stop_monitoring(self):
        """Stop edge infrastructure monitoring."""
        self.monitoring_active = False
        self.logger.info("Stopped edge infrastructure monitoring")
    
    def _monitoring_loop(self):
        """Main monitoring loop."""
        while self.monitoring_active:
            try:
                # Monitor all registered nodes
                for node_id, node in self.node_manager.nodes.items():
                    self._monitor_node(node)
                
                time.sleep(30)  # Monitor every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Monitoring loop error: {e}")
                time.sleep(10)
    
    def _monitor_node(self, node: EdgeNode):
        """Monitor individual edge node."""
        try:
            # Simulate getting resource usage
            # In real implementation, would query node agent or use SSH/API
            
            resource_usage = ResourceUsage(
                node_id=node.node_id,
                cpu_percent=self._get_simulated_cpu_usage(),
                memory_percent=self._get_simulated_memory_usage(),
                storage_percent=self._get_simulated_storage_usage(),
                network_rx_mbps=self._get_simulated_network_usage(),
                network_tx_mbps=self._get_simulated_network_usage(),
                gpu_percent=self._get_simulated_gpu_usage() if node.gpu_count > 0 else 0.0,
                temperature_celsius=self._get_simulated_temperature()
            )
            
            # Update node status based on resource usage
            if resource_usage.cpu_percent > 90 or resource_usage.memory_percent > 90:
                status = NodeStatus.OVERLOADED
            elif resource_usage.temperature_celsius > 80:
                status = NodeStatus.ERROR
            else:
                status = NodeStatus.ONLINE
            
            self.node_manager.update_node_status(node.node_id, status, resource_usage)
            
        except Exception as e:
            self.logger.error(f"Failed to monitor node {node.node_id}: {e}")
            self.node_manager.update_node_status(node.node_id, NodeStatus.ERROR)
    
    def _get_simulated_cpu_usage(self) -> float:
        """Get simulated CPU usage."""
        import random
        return random.uniform(10, 80)
    
    def _get_simulated_memory_usage(self) -> float:
        """Get simulated memory usage."""
        import random
        return random.uniform(20, 70)
    
    def _get_simulated_storage_usage(self) -> float:
        """Get simulated storage usage."""
        import random
        return random.uniform(30, 60)
    
    def _get_simulated_network_usage(self) -> float:
        """Get simulated network usage."""
        import random
        return random.uniform(1, 100)
    
    def _get_simulated_gpu_usage(self) -> float:
        """Get simulated GPU usage."""
        import random
        return random.uniform(0, 95)
    
    def _get_simulated_temperature(self) -> float:
        """Get simulated temperature."""
        import random
        return random.uniform(35, 75)
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get overall edge cluster status."""
        nodes = list(self.node_manager.nodes.values())
        deployments = list(self.scheduler.deployments.values())
        
        node_status_counts = {}
        for node in nodes:
            status = node.status.value
            node_status_counts[status] = node_status_counts.get(status, 0) + 1
        
        deployment_status_counts = {}
        for deployment in deployments:
            status = deployment.status
            deployment_status_counts[status] = deployment_status_counts.get(status, 0) + 1
        
        # Calculate total resources
        total_cpu = sum(node.cpu_cores for node in nodes)
        total_memory = sum(node.memory_gb for node in nodes)
        total_storage = sum(node.storage_gb for node in nodes)
        total_gpu = sum(node.gpu_count for node in nodes)
        
        return {
            'cluster_summary': {
                'total_nodes': len(nodes),
                'online_nodes': node_status_counts.get('online', 0),
                'total_deployments': len(deployments),
                'running_deployments': deployment_status_counts.get('running', 0)
            },
            'node_status': node_status_counts,
            'deployment_status': deployment_status_counts,
            'total_resources': {
                'cpu_cores': total_cpu,
                'memory_gb': total_memory,
                'storage_gb': total_storage,
                'gpu_count': total_gpu
            },
            'monitoring_active': self.monitoring_active
        }
    
    def list_nodes(self) -> List[Dict[str, Any]]:
        """List all edge nodes."""
        return [
            {
                'node_id': node.node_id,
                'name': node.name,
                'type': node.node_type.value,
                'location': node.location,
                'status': node.status.value,
                'ip_address': node.ip_address,
                'resources': {
                    'cpu_cores': node.cpu_cores,
                    'memory_gb': node.memory_gb,
                    'storage_gb': node.storage_gb,
                    'gpu_count': node.gpu_count
                },
                'last_heartbeat': node.last_heartbeat.isoformat() if node.last_heartbeat else None
            }
            for node in self.node_manager.nodes.values()
        ]
    
    def list_workloads(self) -> List[Dict[str, Any]]:
        """List all workloads."""
        workload_status = {}
        
        for workload_id in self.scheduler.workloads.keys():
            status = self.scheduler.get_workload_status(workload_id)
            workload_status[workload_id] = status
        
        return [
            {
                'workload_id': workload.workload_id,
                'name': workload.name,
                'type': workload.workload_type.value,
                'image': workload.image,
                'deployments': workload_status.get(workload.workload_id, {}).get('total_deployments', 0),
                'running': workload_status.get(workload.workload_id, {}).get('status_counts', {}).get('running', 0),
                'created_at': workload.created_at.isoformat()
            }
            for workload in self.scheduler.workloads.values()
        ]


def main():
    """Example usage of Edge Computing Orchestrator."""
    orchestrator = EdgeOrchestrator()
    
    try:
        print("🌐 Edge Computing Orchestrator")
        
        # Register edge nodes
        print("\n📡 Registering edge nodes...")
        
        # Gateway node
        gateway_node = EdgeNode(
            node_id="gateway_001",
            name="Main Gateway",
            node_type=EdgeNodeType.GATEWAY,
            location="Building A",
            ip_address="192.168.1.10",
            port=8080,
            status=NodeStatus.ONLINE,
            cpu_cores=4,
            memory_gb=8.0,
            storage_gb=100.0,
            capabilities=["networking", "routing", "firewall"]
        )
        
        # Compute node with GPU
        compute_node = EdgeNode(
            node_id="compute_001",
            name="AI Compute Node",
            node_type=EdgeNodeType.AI_ACCELERATOR,
            location="Building A",
            ip_address="192.168.1.11",
            port=8080,
            status=NodeStatus.ONLINE,
            cpu_cores=16,
            memory_gb=64.0,
            storage_gb=500.0,
            gpu_count=2,
            capabilities=["ai_accelerator", "cuda", "tensorflow", "pytorch"]
        )
        
        # Sensor hub
        sensor_hub = EdgeNode(
            node_id="sensor_hub_001",
            name="IoT Sensor Hub",
            node_type=EdgeNodeType.SENSOR_HUB,
            location="Building B",
            ip_address="192.168.1.12",
            port=8080,
            status=NodeStatus.ONLINE,
            cpu_cores=2,
            memory_gb=4.0,
            storage_gb=50.0,
            capabilities=["iot", "mqtt", "sensors"]
        )
        
        # Register nodes
        orchestrator.register_edge_node(gateway_node)
        orchestrator.register_edge_node(compute_node)
        orchestrator.register_edge_node(sensor_hub)
        
        print("✅ Edge nodes registered")
        
        # Start monitoring
        orchestrator.start_monitoring()
        print("📊 Started infrastructure monitoring")
        
        # Create and deploy workloads
        print("\n🚀 Deploying workloads...")
        
        # AI model workload
        ai_workload = EdgeWorkload(
            workload_id="ai_model_001",
            name="Object Detection Model",
            workload_type=WorkloadType.AI_MODEL,
            image="tensorflow/serving:latest",
            resource_requirements={
                'cpu_cores': 4,
                'memory_gb': 8.0,
                'storage_gb': 10.0,
                'capabilities': ['ai_accelerator']
            },
            environment_variables={
                'MODEL_NAME': 'object_detection',
                'MODEL_BASE_PATH': '/models'
            },
            ports=[8501, 8500]
        )
        
        # Data processing workload
        data_workload = EdgeWorkload(
            workload_id="data_processor_001",
            name="Stream Data Processor",
            workload_type=WorkloadType.DATA_PROCESSING,
            image="apache/kafka:latest",
            resource_requirements={
                'cpu_cores': 2,
                'memory_gb': 4.0,
                'storage_gb': 20.0,
                'capabilities': ['networking']
            },
            ports=[9092]
        )
        
        # Deploy workloads
        ai_deployments = orchestrator.deploy_workload(ai_workload, replicas=1)
        data_deployments = orchestrator.deploy_workload(data_workload, replicas=2)
        
        print(f"✅ Deployed AI model: {len(ai_deployments)} replicas")
        print(f"✅ Deployed data processor: {len(data_deployments)} replicas")
        
        # Wait for deployments to stabilize
        time.sleep(2)
        
        # Show cluster status
        print("\n📈 Cluster Status:")
        status = orchestrator.get_cluster_status()
        
        print(f"   Total Nodes: {status['cluster_summary']['total_nodes']}")
        print(f"   Online Nodes: {status['cluster_summary']['online_nodes']}")
        print(f"   Total Deployments: {status['cluster_summary']['total_deployments']}")
        print(f"   Running Deployments: {status['cluster_summary']['running_deployments']}")
        
        print(f"\n   Total Resources:")
        resources = status['total_resources']
        print(f"     CPU Cores: {resources['cpu_cores']}")
        print(f"     Memory: {resources['memory_gb']} GB")
        print(f"     Storage: {resources['storage_gb']} GB")
        print(f"     GPUs: {resources['gpu_count']}")
        
        # List nodes
        print("\n🖥️  Edge Nodes:")
        nodes = orchestrator.list_nodes()
        
        for node in nodes:
            print(f"   📡 {node['name']}")
            print(f"      Type: {node['type']}")
            print(f"      Status: {node['status']}")
            print(f"      Location: {node['location']}")
            print(f"      Resources: {node['resources']['cpu_cores']} CPU, {node['resources']['memory_gb']} GB RAM")
        
        # List workloads
        print("\n⚙️  Deployed Workloads:")
        workloads = orchestrator.list_workloads()
        
        for workload in workloads:
            print(f"   🔧 {workload['name']}")
            print(f"      Type: {workload['type']}")
            print(f"      Deployments: {workload['running']}/{workload['deployments']}")
        
        # Test scaling
        print("\n📈 Testing workload scaling...")
        
        # Scale up data processor
        new_deployments = orchestrator.scheduler.scale_workload("data_processor_001", 3)
        print(f"✅ Scaled data processor to 3 replicas")
        
        # Wait a moment
        time.sleep(1)
        
        # Scale down
        stopped_deployments = orchestrator.scheduler.scale_workload("data_processor_001", 1)
        print(f"✅ Scaled data processor down to 1 replica")
        
        print("\n✅ Edge Computing Orchestrator demo completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        # Cleanup
        orchestrator.stop_monitoring()


if __name__ == "__main__":
    main()