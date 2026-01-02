#!/usr/bin/env python3
"""
Kubernetes Orchestrator - Enterprise Container Management
Advanced Kubernetes cluster management and application orchestration.

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
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, asdict, field
from enum import Enum
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
import base64
import hashlib

class DeploymentStrategy(Enum):
    """Kubernetes deployment strategies."""
    ROLLING_UPDATE = "RollingUpdate"
    RECREATE = "Recreate"
    BLUE_GREEN = "BlueGreen"
    CANARY = "Canary"

class ResourceType(Enum):
    """Kubernetes resource types."""
    DEPLOYMENT = "Deployment"
    SERVICE = "Service"
    INGRESS = "Ingress"
    CONFIGMAP = "ConfigMap"
    SECRET = "Secret"
    PERSISTENT_VOLUME = "PersistentVolume"
    PERSISTENT_VOLUME_CLAIM = "PersistentVolumeClaim"
    HORIZONTAL_POD_AUTOSCALER = "HorizontalPodAutoscaler"
    NETWORK_POLICY = "NetworkPolicy"
    SERVICE_ACCOUNT = "ServiceAccount"

@dataclass
class KubernetesCluster:
    """Kubernetes cluster configuration."""
    name: str
    endpoint: str
    region: str
    version: str
    node_count: int
    node_type: str
    kubeconfig_path: str
    monitoring_enabled: bool = True
    logging_enabled: bool = True
    network_policy_enabled: bool = True

@dataclass
class Application:
    """Application deployment configuration."""
    name: str
    namespace: str
    image: str
    tag: str
    replicas: int
    resources: Dict[str, Any]
    environment_variables: Dict[str, str] = field(default_factory=dict)
    config_maps: List[str] = field(default_factory=list)
    secrets: List[str] = field(default_factory=list)
    health_check: Optional[Dict[str, Any]] = None
    service_config: Optional[Dict[str, Any]] = None

@dataclass
class DeploymentResult:
    """Deployment operation result."""
    success: bool
    deployment_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    resources_created: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    rollback_available: bool = False

class KubernetesOrchestrator:
    """
    Enterprise Kubernetes orchestrator for container management.
    Provides advanced deployment strategies and cluster operations.
    """
    
    def __init__(self, cluster_config: KubernetesCluster):
        self.cluster = cluster_config
        self.logger = self._setup_logging()
        self.kubectl_path = self._find_kubectl()
        self.deployments = {}
        self.executor = ThreadPoolExecutor(max_workers=10)
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for Kubernetes orchestrator."""
        logger = logging.getLogger('KubernetesOrchestrator')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def _find_kubectl(self) -> str:
        """Find kubectl binary path."""
        kubectl_paths = ['/usr/local/bin/kubectl', '/usr/bin/kubectl', 'kubectl']
        
        for path in kubectl_paths:
            try:
                result = subprocess.run([path, 'version', '--client'], 
                                      capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    return path
            except (subprocess.TimeoutExpired, FileNotFoundError):
                continue
        
        raise RuntimeError("kubectl not found in system PATH")
    
    async def connect_cluster(self) -> bool:
        """
        Connect to Kubernetes cluster and verify connectivity.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.logger.info(f"Connecting to cluster: {self.cluster.name}")
            
            # Set kubeconfig
            os.environ['KUBECONFIG'] = self.cluster.kubeconfig_path
            
            # Test cluster connectivity
            result = await self._run_kubectl_command(['cluster-info'])
            
            if result['success']:
                self.logger.info("Successfully connected to Kubernetes cluster")
                
                # Get cluster version
                version_result = await self._run_kubectl_command(['version', '--output=json'])
                if version_result['success']:
                    version_info = json.loads(version_result['output'])
                    server_version = version_info.get('serverVersion', {}).get('gitVersion', 'unknown')
                    self.logger.info(f"Cluster version: {server_version}")
                
                return True
            else:
                self.logger.error(f"Failed to connect to cluster: {result['error']}")
                return False
                
        except Exception as e:
            self.logger.error(f"Cluster connection error: {e}")
            return False
    
    async def _run_kubectl_command(self, args: List[str], 
                                  namespace: str = None) -> Dict[str, Any]:
        """
        Run kubectl command asynchronously.
        
        Args:
            args: kubectl command arguments
            namespace: Kubernetes namespace
            
        Returns:
            Dictionary with command result
        """
        cmd = [self.kubectl_path] + args
        
        if namespace:
            cmd.extend(['--namespace', namespace])
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=os.environ.copy()
            )
            
            stdout, stderr = await process.communicate()
            
            return {
                'success': process.returncode == 0,
                'output': stdout.decode('utf-8'),
                'error': stderr.decode('utf-8'),
                'return_code': process.returncode
            }
            
        except Exception as e:
            return {
                'success': False,
                'output': '',
                'error': str(e),
                'return_code': -1
            }
    
    async def create_namespace(self, namespace: str, 
                             labels: Dict[str, str] = None) -> bool:
        """
        Create Kubernetes namespace.
        
        Args:
            namespace: Namespace name
            labels: Optional labels for namespace
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Creating namespace: {namespace}")
        
        namespace_manifest = {
            'apiVersion': 'v1',
            'kind': 'Namespace',
            'metadata': {
                'name': namespace,
                'labels': labels or {}
            }
        }
        
        return await self._apply_manifest(namespace_manifest)
    
    async def deploy_application(self, app: Application, 
                               strategy: DeploymentStrategy = DeploymentStrategy.ROLLING_UPDATE) -> DeploymentResult:
        """
        Deploy application to Kubernetes cluster.
        
        Args:
            app: Application configuration
            strategy: Deployment strategy
            
        Returns:
            DeploymentResult with operation details
        """
        deployment_id = f"deploy_{app.name}_{int(time.time())}"
        result = DeploymentResult(
            success=False,
            deployment_id=deployment_id,
            start_time=datetime.now()
        )
        
        try:
            self.logger.info(f"Deploying application: {app.name} using {strategy.value}")
            
            # Create namespace if it doesn't exist
            await self.create_namespace(app.namespace)
            
            # Generate manifests
            manifests = self._generate_application_manifests(app, strategy)
            
            # Apply manifests
            for manifest in manifests:
                apply_result = await self._apply_manifest(manifest, app.namespace)
                if not apply_result:
                    raise Exception(f"Failed to apply manifest for {manifest['kind']}")
                
                result.resources_created.append(f"{manifest['kind']}/{manifest['metadata']['name']}")
            
            # Wait for deployment to be ready
            if strategy in [DeploymentStrategy.ROLLING_UPDATE, DeploymentStrategy.RECREATE]:
                await self._wait_for_deployment_ready(app.name, app.namespace)
            
            result.success = True
            result.rollback_available = True
            
            self.logger.info(f"Successfully deployed application: {app.name}")
            
        except Exception as e:
            result.error_message = str(e)
            self.logger.error(f"Deployment failed for {app.name}: {e}")
        
        finally:
            result.end_time = datetime.now()
            self.deployments[deployment_id] = result
        
        return result
    
    def _generate_application_manifests(self, app: Application, 
                                      strategy: DeploymentStrategy) -> List[Dict[str, Any]]:
        """Generate Kubernetes manifests for application."""
        manifests = []
        
        # Deployment manifest
        deployment_manifest = {
            'apiVersion': 'apps/v1',
            'kind': 'Deployment',
            'metadata': {
                'name': app.name,
                'namespace': app.namespace,
                'labels': {
                    'app': app.name,
                    'version': app.tag,
                    'managed-by': 'kubernetes-orchestrator'
                }
            },
            'spec': {
                'replicas': app.replicas,
                'strategy': {
                    'type': strategy.value
                },
                'selector': {
                    'matchLabels': {
                        'app': app.name
                    }
                },
                'template': {
                    'metadata': {
                        'labels': {
                            'app': app.name,
                            'version': app.tag
                        }
                    },
                    'spec': {
                        'containers': [{
                            'name': app.name,
                            'image': f"{app.image}:{app.tag}",
                            'resources': app.resources,
                            'env': [
                                {'name': k, 'value': v} 
                                for k, v in app.environment_variables.items()
                            ]
                        }]
                    }
                }
            }
        }
        
        # Add health checks if configured
        if app.health_check:
            container = deployment_manifest['spec']['template']['spec']['containers'][0]
            if 'liveness' in app.health_check:
                container['livenessProbe'] = app.health_check['liveness']
            if 'readiness' in app.health_check:
                container['readinessProbe'] = app.health_check['readiness']
        
        # Add rolling update configuration
        if strategy == DeploymentStrategy.ROLLING_UPDATE:
            deployment_manifest['spec']['strategy']['rollingUpdate'] = {
                'maxUnavailable': '25%',
                'maxSurge': '25%'
            }
        
        manifests.append(deployment_manifest)
        
        # Service manifest
        if app.service_config:
            service_manifest = {
                'apiVersion': 'v1',
                'kind': 'Service',
                'metadata': {
                    'name': f"{app.name}-service",
                    'namespace': app.namespace,
                    'labels': {
                        'app': app.name
                    }
                },
                'spec': {
                    'selector': {
                        'app': app.name
                    },
                    'ports': app.service_config.get('ports', []),
                    'type': app.service_config.get('type', 'ClusterIP')
                }
            }
            manifests.append(service_manifest)
        
        # ConfigMap manifests
        for config_map_name in app.config_maps:
            config_map_manifest = {
                'apiVersion': 'v1',
                'kind': 'ConfigMap',
                'metadata': {
                    'name': config_map_name,
                    'namespace': app.namespace
                },
                'data': {
                    'config.yaml': 'placeholder: value'  # Would be populated from actual config
                }
            }
            manifests.append(config_map_manifest)
        
        return manifests
    
    async def _apply_manifest(self, manifest: Dict[str, Any], 
                            namespace: str = None) -> bool:
        """Apply Kubernetes manifest."""
        try:
            # Convert manifest to YAML
            manifest_yaml = yaml.dump(manifest, default_flow_style=False)
            
            # Write to temporary file
            temp_file = f"/tmp/manifest_{int(time.time())}.yaml"
            with open(temp_file, 'w') as f:
                f.write(manifest_yaml)
            
            # Apply manifest
            args = ['apply', '-f', temp_file]
            result = await self._run_kubectl_command(args, namespace)
            
            # Cleanup temp file
            os.remove(temp_file)
            
            return result['success']
            
        except Exception as e:
            self.logger.error(f"Failed to apply manifest: {e}")
            return False
    
    async def _wait_for_deployment_ready(self, deployment_name: str, 
                                       namespace: str, timeout: int = 300) -> bool:
        """
        Wait for deployment to be ready.
        
        Args:
            deployment_name: Name of deployment
            namespace: Kubernetes namespace
            timeout: Timeout in seconds
            
        Returns:
            True if deployment is ready, False if timeout
        """
        self.logger.info(f"Waiting for deployment {deployment_name} to be ready")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            result = await self._run_kubectl_command([
                'rollout', 'status', 'deployment', deployment_name
            ], namespace)
            
            if result['success'] and 'successfully rolled out' in result['output']:
                self.logger.info(f"Deployment {deployment_name} is ready")
                return True
            
            await asyncio.sleep(5)
        
        self.logger.warning(f"Timeout waiting for deployment {deployment_name}")
        return False
    
    async def scale_deployment(self, deployment_name: str, namespace: str, 
                             replicas: int) -> bool:
        """
        Scale deployment to specified number of replicas.
        
        Args:
            deployment_name: Name of deployment
            namespace: Kubernetes namespace
            replicas: Target number of replicas
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Scaling deployment {deployment_name} to {replicas} replicas")
        
        result = await self._run_kubectl_command([
            'scale', 'deployment', deployment_name, f'--replicas={replicas}'
        ], namespace)
        
        if result['success']:
            # Wait for scaling to complete
            await self._wait_for_deployment_ready(deployment_name, namespace)
            return True
        
        return False
    
    async def rollback_deployment(self, deployment_name: str, 
                                namespace: str, revision: int = None) -> bool:
        """
        Rollback deployment to previous or specific revision.
        
        Args:
            deployment_name: Name of deployment
            namespace: Kubernetes namespace
            revision: Specific revision to rollback to
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Rolling back deployment {deployment_name}")
        
        args = ['rollout', 'undo', 'deployment', deployment_name]
        if revision:
            args.extend([f'--to-revision={revision}'])
        
        result = await self._run_kubectl_command(args, namespace)
        
        if result['success']:
            await self._wait_for_deployment_ready(deployment_name, namespace)
            return True
        
        return False
    
    async def get_pod_logs(self, pod_name: str, namespace: str, 
                          lines: int = 100) -> str:
        """
        Get logs from specific pod.
        
        Args:
            pod_name: Name of pod
            namespace: Kubernetes namespace
            lines: Number of log lines to retrieve
            
        Returns:
            Pod logs as string
        """
        result = await self._run_kubectl_command([
            'logs', pod_name, f'--tail={lines}'
        ], namespace)
        
        return result['output'] if result['success'] else result['error']
    
    async def execute_in_pod(self, pod_name: str, namespace: str, 
                           command: List[str]) -> Dict[str, Any]:
        """
        Execute command in pod.
        
        Args:
            pod_name: Name of pod
            namespace: Kubernetes namespace
            command: Command to execute
            
        Returns:
            Execution result
        """
        args = ['exec', pod_name, '--'] + command
        return await self._run_kubectl_command(args, namespace)
    
    async def get_cluster_resources(self) -> Dict[str, Any]:
        """Get cluster resource utilization."""
        self.logger.info("Getting cluster resource utilization")
        
        # Get nodes
        nodes_result = await self._run_kubectl_command(['get', 'nodes', '-o', 'json'])
        
        # Get pods
        pods_result = await self._run_kubectl_command(['get', 'pods', '--all-namespaces', '-o', 'json'])
        
        resources = {
            'timestamp': datetime.now().isoformat(),
            'nodes': {'count': 0, 'ready': 0},
            'pods': {'total': 0, 'running': 0, 'pending': 0, 'failed': 0},
            'namespaces': []
        }
        
        if nodes_result['success']:
            nodes_data = json.loads(nodes_result['output'])
            resources['nodes']['count'] = len(nodes_data.get('items', []))
            
            for node in nodes_data.get('items', []):
                conditions = node.get('status', {}).get('conditions', [])
                for condition in conditions:
                    if condition.get('type') == 'Ready' and condition.get('status') == 'True':
                        resources['nodes']['ready'] += 1
                        break
        
        if pods_result['success']:
            pods_data = json.loads(pods_result['output'])
            resources['pods']['total'] = len(pods_data.get('items', []))
            
            for pod in pods_data.get('items', []):
                phase = pod.get('status', {}).get('phase', 'Unknown')
                if phase == 'Running':
                    resources['pods']['running'] += 1
                elif phase == 'Pending':
                    resources['pods']['pending'] += 1
                elif phase == 'Failed':
                    resources['pods']['failed'] += 1
        
        return resources
    
    async def create_horizontal_pod_autoscaler(self, deployment_name: str, 
                                             namespace: str, min_replicas: int = 2,
                                             max_replicas: int = 10, 
                                             cpu_threshold: int = 70) -> bool:
        """
        Create Horizontal Pod Autoscaler for deployment.
        
        Args:
            deployment_name: Name of deployment
            namespace: Kubernetes namespace
            min_replicas: Minimum number of replicas
            max_replicas: Maximum number of replicas
            cpu_threshold: CPU utilization threshold percentage
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Creating HPA for deployment {deployment_name}")
        
        hpa_manifest = {
            'apiVersion': 'autoscaling/v2',
            'kind': 'HorizontalPodAutoscaler',
            'metadata': {
                'name': f"{deployment_name}-hpa",
                'namespace': namespace
            },
            'spec': {
                'scaleTargetRef': {
                    'apiVersion': 'apps/v1',
                    'kind': 'Deployment',
                    'name': deployment_name
                },
                'minReplicas': min_replicas,
                'maxReplicas': max_replicas,
                'metrics': [{
                    'type': 'Resource',
                    'resource': {
                        'name': 'cpu',
                        'target': {
                            'type': 'Utilization',
                            'averageUtilization': cpu_threshold
                        }
                    }
                }]
            }
        }
        
        return await self._apply_manifest(hpa_manifest, namespace)
    
    async def cleanup_deployment(self, deployment_name: str, namespace: str) -> bool:
        """
        Clean up all resources for a deployment.
        
        Args:
            deployment_name: Name of deployment
            namespace: Kubernetes namespace
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Cleaning up deployment {deployment_name}")
        
        resources_to_delete = [
            ('deployment', deployment_name),
            ('service', f"{deployment_name}-service"),
            ('hpa', f"{deployment_name}-hpa")
        ]
        
        success = True
        for resource_type, resource_name in resources_to_delete:
            result = await self._run_kubectl_command([
                'delete', resource_type, resource_name, '--ignore-not-found=true'
            ], namespace)
            
            if not result['success']:
                self.logger.warning(f"Failed to delete {resource_type}/{resource_name}")
                success = False
        
        return success


async def main():
    """Example usage of Kubernetes orchestrator."""
    # Cluster configuration
    cluster = KubernetesCluster(
        name="production-cluster",
        endpoint="https://k8s-api.example.com",
        region="us-east-1",
        version="1.24",
        node_count=5,
        node_type="t3.large",
        kubeconfig_path="~/.kube/config"
    )
    
    # Initialize orchestrator
    orchestrator = KubernetesOrchestrator(cluster)
    
    try:
        # Connect to cluster
        if not await orchestrator.connect_cluster():
            print("Failed to connect to cluster")
            return
        
        # Application configuration
        app = Application(
            name="web-app",
            namespace="production",
            image="nginx",
            tag="1.21",
            replicas=3,
            resources={
                'requests': {'cpu': '100m', 'memory': '128Mi'},
                'limits': {'cpu': '500m', 'memory': '512Mi'}
            },
            environment_variables={
                'ENV': 'production',
                'LOG_LEVEL': 'info'
            },
            health_check={
                'liveness': {
                    'httpGet': {'path': '/health', 'port': 80},
                    'initialDelaySeconds': 30,
                    'periodSeconds': 10
                },
                'readiness': {
                    'httpGet': {'path': '/ready', 'port': 80},
                    'initialDelaySeconds': 5,
                    'periodSeconds': 5
                }
            },
            service_config={
                'type': 'LoadBalancer',
                'ports': [{'port': 80, 'targetPort': 80}]
            }
        )
        
        # Deploy application
        result = await orchestrator.deploy_application(app, DeploymentStrategy.ROLLING_UPDATE)
        
        if result.success:
            print(f"✅ Deployment successful: {result.deployment_id}")
            print(f"Resources created: {', '.join(result.resources_created)}")
            
            # Create autoscaler
            await orchestrator.create_horizontal_pod_autoscaler(
                app.name, app.namespace, min_replicas=2, max_replicas=10
            )
            
            # Get cluster resources
            resources = await orchestrator.get_cluster_resources()
            print(f"Cluster status: {resources['nodes']['ready']}/{resources['nodes']['count']} nodes ready")
            
        else:
            print(f"❌ Deployment failed: {result.error_message}")
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())