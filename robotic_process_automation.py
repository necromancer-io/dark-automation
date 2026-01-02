#!/usr/bin/env python3
"""
Robotic Process Automation (RPA) - Enterprise Automation Platform
Advanced RPA system for business process automation and workflow orchestration.

Use of this code is at your own risk.
Author bears no responsibility for any damages caused by the code.
"""

import os
import sys
import json
import time
import asyncio
import logging
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple, Callable
from dataclasses import dataclass, asdict, field
from enum import Enum
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
import pyautogui
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class ActionType(Enum):
    """RPA action types."""
    CLICK = "click"
    TYPE = "type"
    WAIT = "wait"
    SCREENSHOT = "screenshot"
    READ_TEXT = "read_text"
    NAVIGATE = "navigate"
    UPLOAD_FILE = "upload_file"
    DOWNLOAD_FILE = "download_file"
    API_CALL = "api_call"
    DATABASE_QUERY = "database_query"
    EMAIL_SEND = "email_send"
    FILE_OPERATION = "file_operation"
    EXCEL_OPERATION = "excel_operation"
    PDF_OPERATION = "pdf_operation"

class ExecutionStatus(Enum):
    """Execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRY = "retry"

@dataclass
class RPAAction:
    """RPA action definition."""
    action_id: str
    action_type: ActionType
    parameters: Dict[str, Any]
    timeout: int = 30
    retry_count: int = 3
    description: str = ""
    conditions: List[Dict[str, Any]] = field(default_factory=list)

@dataclass
class RPAWorkflow:
    """RPA workflow definition."""
    workflow_id: str
    name: str
    description: str
    actions: List[RPAAction]
    variables: Dict[str, Any] = field(default_factory=dict)
    schedule: Optional[str] = None
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.now)

@dataclass
class ExecutionResult:
    """Action execution result."""
    action_id: str
    status: ExecutionStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    result_data: Any = None
    error_message: Optional[str] = None
    screenshot_path: Optional[str] = None
    retry_attempt: int = 0

@dataclass
class WorkflowExecution:
    """Workflow execution instance."""
    execution_id: str
    workflow_id: str
    status: ExecutionStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    action_results: List[ExecutionResult] = field(default_factory=list)
    variables: Dict[str, Any] = field(default_factory=dict)
    logs: List[str] = field(default_factory=list)

class WebAutomation:
    """Web browser automation engine."""
    
    def __init__(self, headless: bool = False):
        self.driver = None
        self.headless = headless
        self.logger = logging.getLogger('WebAutomation')
        
    def start_browser(self, browser_type: str = "chrome") -> bool:
        """Start web browser."""
        try:
            if browser_type.lower() == "chrome":
                options = webdriver.ChromeOptions()
                if self.headless:
                    options.add_argument("--headless")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                
                self.driver = webdriver.Chrome(options=options)
            
            elif browser_type.lower() == "firefox":
                options = webdriver.FirefoxOptions()
                if self.headless:
                    options.add_argument("--headless")
                
                self.driver = webdriver.Firefox(options=options)
            
            else:
                raise ValueError(f"Unsupported browser: {browser_type}")
            
            self.driver.implicitly_wait(10)
            self.logger.info(f"Started {browser_type} browser")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start browser: {e}")
            return False
    
    def stop_browser(self):
        """Stop web browser."""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
                self.logger.info("Browser stopped")
            except Exception as e:
                self.logger.error(f"Error stopping browser: {e}")
    
    def navigate_to_url(self, url: str) -> bool:
        """Navigate to URL."""
        try:
            self.driver.get(url)
            self.logger.info(f"Navigated to: {url}")
            return True
        except Exception as e:
            self.logger.error(f"Navigation failed: {e}")
            return False
    
    def click_element(self, selector: str, selector_type: str = "css") -> bool:
        """Click web element."""
        try:
            if selector_type == "css":
                element = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
            elif selector_type == "xpath":
                element = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, selector))
                )
            elif selector_type == "id":
                element = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, selector))
                )
            else:
                raise ValueError(f"Unsupported selector type: {selector_type}")
            
            element.click()
            self.logger.info(f"Clicked element: {selector}")
            return True
            
        except Exception as e:
            self.logger.error(f"Click failed: {e}")
            return False
    
    def type_text(self, selector: str, text: str, selector_type: str = "css") -> bool:
        """Type text into element."""
        try:
            if selector_type == "css":
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
            elif selector_type == "xpath":
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
            elif selector_type == "id":
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, selector))
                )
            else:
                raise ValueError(f"Unsupported selector type: {selector_type}")
            
            element.clear()
            element.send_keys(text)
            self.logger.info(f"Typed text into element: {selector}")
            return True
            
        except Exception as e:
            self.logger.error(f"Type text failed: {e}")
            return False
    
    def read_text(self, selector: str, selector_type: str = "css") -> Optional[str]:
        """Read text from element."""
        try:
            if selector_type == "css":
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
            elif selector_type == "xpath":
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
            elif selector_type == "id":
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, selector))
                )
            else:
                raise ValueError(f"Unsupported selector type: {selector_type}")
            
            text = element.text
            self.logger.info(f"Read text from element: {selector}")
            return text
            
        except Exception as e:
            self.logger.error(f"Read text failed: {e}")
            return None
    
    def take_screenshot(self, file_path: str) -> bool:
        """Take screenshot."""
        try:
            self.driver.save_screenshot(file_path)
            self.logger.info(f"Screenshot saved: {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Screenshot failed: {e}")
            return False
    
    def wait_for_element(self, selector: str, selector_type: str = "css", timeout: int = 10) -> bool:
        """Wait for element to appear."""
        try:
            if selector_type == "css":
                WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
            elif selector_type == "xpath":
                WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
            elif selector_type == "id":
                WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.ID, selector))
                )
            
            self.logger.info(f"Element found: {selector}")
            return True
            
        except Exception as e:
            self.logger.error(f"Wait for element failed: {e}")
            return False

class DesktopAutomation:
    """Desktop application automation engine."""
    
    def __init__(self):
        self.logger = logging.getLogger('DesktopAutomation')
        
        # Configure pyautogui
        pyautogui.FAILSAFE = True
        pyautogui.PAUSE = 0.5
    
    def click_coordinates(self, x: int, y: int) -> bool:
        """Click at specific coordinates."""
        try:
            pyautogui.click(x, y)
            self.logger.info(f"Clicked at coordinates: ({x}, {y})")
            return True
        except Exception as e:
            self.logger.error(f"Click coordinates failed: {e}")
            return False
    
    def click_image(self, image_path: str, confidence: float = 0.8) -> bool:
        """Click on image match."""
        try:
            location = pyautogui.locateOnScreen(image_path, confidence=confidence)
            if location:
                center = pyautogui.center(location)
                pyautogui.click(center)
                self.logger.info(f"Clicked on image: {image_path}")
                return True
            else:
                self.logger.warning(f"Image not found: {image_path}")
                return False
        except Exception as e:
            self.logger.error(f"Click image failed: {e}")
            return False
    
    def type_text(self, text: str, interval: float = 0.1) -> bool:
        """Type text."""
        try:
            pyautogui.typewrite(text, interval=interval)
            self.logger.info(f"Typed text: {text[:50]}...")
            return True
        except Exception as e:
            self.logger.error(f"Type text failed: {e}")
            return False
    
    def press_key(self, key: str) -> bool:
        """Press keyboard key."""
        try:
            pyautogui.press(key)
            self.logger.info(f"Pressed key: {key}")
            return True
        except Exception as e:
            self.logger.error(f"Press key failed: {e}")
            return False
    
    def press_hotkey(self, *keys) -> bool:
        """Press hotkey combination."""
        try:
            pyautogui.hotkey(*keys)
            self.logger.info(f"Pressed hotkey: {'+'.join(keys)}")
            return True
        except Exception as e:
            self.logger.error(f"Press hotkey failed: {e}")
            return False
    
    def take_screenshot(self, file_path: str, region: Tuple[int, int, int, int] = None) -> bool:
        """Take screenshot."""
        try:
            if region:
                screenshot = pyautogui.screenshot(region=region)
            else:
                screenshot = pyautogui.screenshot()
            
            screenshot.save(file_path)
            self.logger.info(f"Screenshot saved: {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Screenshot failed: {e}")
            return False
    
    def find_image(self, image_path: str, confidence: float = 0.8) -> Optional[Tuple[int, int, int, int]]:
        """Find image on screen."""
        try:
            location = pyautogui.locateOnScreen(image_path, confidence=confidence)
            if location:
                self.logger.info(f"Image found: {image_path}")
                return location
            else:
                self.logger.warning(f"Image not found: {image_path}")
                return None
        except Exception as e:
            self.logger.error(f"Find image failed: {e}")
            return None

class FileOperations:
    """File and document operations."""
    
    def __init__(self):
        self.logger = logging.getLogger('FileOperations')
    
    def read_file(self, file_path: str, encoding: str = 'utf-8') -> Optional[str]:
        """Read text file."""
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            
            self.logger.info(f"Read file: {file_path}")
            return content
        except Exception as e:
            self.logger.error(f"Read file failed: {e}")
            return None
    
    def write_file(self, file_path: str, content: str, encoding: str = 'utf-8') -> bool:
        """Write text file."""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with open(file_path, 'w', encoding=encoding) as f:
                f.write(content)
            
            self.logger.info(f"Wrote file: {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Write file failed: {e}")
            return False
    
    def copy_file(self, source_path: str, destination_path: str) -> bool:
        """Copy file."""
        try:
            import shutil
            
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            shutil.copy2(source_path, destination_path)
            
            self.logger.info(f"Copied file: {source_path} -> {destination_path}")
            return True
        except Exception as e:
            self.logger.error(f"Copy file failed: {e}")
            return False
    
    def move_file(self, source_path: str, destination_path: str) -> bool:
        """Move file."""
        try:
            import shutil
            
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            shutil.move(source_path, destination_path)
            
            self.logger.info(f"Moved file: {source_path} -> {destination_path}")
            return True
        except Exception as e:
            self.logger.error(f"Move file failed: {e}")
            return False
    
    def delete_file(self, file_path: str) -> bool:
        """Delete file."""
        try:
            os.remove(file_path)
            self.logger.info(f"Deleted file: {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Delete file failed: {e}")
            return False
    
    def list_files(self, directory_path: str, pattern: str = "*") -> List[str]:
        """List files in directory."""
        try:
            import glob
            
            search_pattern = os.path.join(directory_path, pattern)
            files = glob.glob(search_pattern)
            
            self.logger.info(f"Listed {len(files)} files in: {directory_path}")
            return files
        except Exception as e:
            self.logger.error(f"List files failed: {e}")
            return []

class RPAEngine:
    """Main RPA execution engine."""
    
    def __init__(self):
        self.web_automation = WebAutomation()
        self.desktop_automation = DesktopAutomation()
        self.file_operations = FileOperations()
        self.workflows = {}
        self.executions = {}
        self.logger = self._setup_logging()
        self.executor = ThreadPoolExecutor(max_workers=5)
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger('RPAEngine')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def register_workflow(self, workflow: RPAWorkflow) -> bool:
        """Register RPA workflow."""
        try:
            self.workflows[workflow.workflow_id] = workflow
            self.logger.info(f"Registered workflow: {workflow.name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to register workflow: {e}")
            return False
    
    def execute_workflow(self, workflow_id: str, variables: Dict[str, Any] = None) -> str:
        """Execute RPA workflow."""
        if workflow_id not in self.workflows:
            raise ValueError(f"Workflow not found: {workflow_id}")
        
        execution_id = str(uuid.uuid4())
        workflow = self.workflows[workflow_id]
        
        execution = WorkflowExecution(
            execution_id=execution_id,
            workflow_id=workflow_id,
            status=ExecutionStatus.RUNNING,
            start_time=datetime.now(),
            variables=variables or {}
        )
        
        self.executions[execution_id] = execution
        
        # Execute workflow in background
        future = self.executor.submit(self._execute_workflow_actions, workflow, execution)
        
        self.logger.info(f"Started workflow execution: {workflow.name} ({execution_id})")
        
        return execution_id
    
    def _execute_workflow_actions(self, workflow: RPAWorkflow, execution: WorkflowExecution):
        """Execute workflow actions sequentially."""
        try:
            execution.logs.append(f"Starting workflow: {workflow.name}")
            
            # Merge workflow variables with execution variables
            merged_variables = {**workflow.variables, **execution.variables}
            
            for action in workflow.actions:
                if execution.status == ExecutionStatus.CANCELLED:
                    break
                
                execution.logs.append(f"Executing action: {action.action_type.value}")
                
                # Execute action with retry logic
                result = self._execute_action_with_retry(action, merged_variables)
                execution.action_results.append(result)
                
                if result.status == ExecutionStatus.FAILED:
                    execution.status = ExecutionStatus.FAILED
                    execution.logs.append(f"Workflow failed at action: {action.action_id}")
                    break
                
                # Update variables with action results
                if result.result_data and isinstance(result.result_data, dict):
                    merged_variables.update(result.result_data)
            
            if execution.status == ExecutionStatus.RUNNING:
                execution.status = ExecutionStatus.COMPLETED
                execution.logs.append("Workflow completed successfully")
            
        except Exception as e:
            execution.status = ExecutionStatus.FAILED
            execution.logs.append(f"Workflow execution error: {str(e)}")
            self.logger.error(f"Workflow execution failed: {e}")
        
        finally:
            execution.end_time = datetime.now()
            
            # Cleanup resources
            self.web_automation.stop_browser()
    
    def _execute_action_with_retry(self, action: RPAAction, variables: Dict[str, Any]) -> ExecutionResult:
        """Execute action with retry logic."""
        result = ExecutionResult(
            action_id=action.action_id,
            status=ExecutionStatus.RUNNING,
            start_time=datetime.now()
        )
        
        for attempt in range(action.retry_count + 1):
            result.retry_attempt = attempt
            
            try:
                success, data = self._execute_single_action(action, variables)
                
                if success:
                    result.status = ExecutionStatus.COMPLETED
                    result.result_data = data
                    break
                else:
                    if attempt < action.retry_count:
                        self.logger.warning(f"Action failed, retrying ({attempt + 1}/{action.retry_count})")
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        result.status = ExecutionStatus.FAILED
                        result.error_message = "Action failed after all retries"
                
            except Exception as e:
                if attempt < action.retry_count:
                    self.logger.warning(f"Action error, retrying: {e}")
                    time.sleep(2 ** attempt)
                else:
                    result.status = ExecutionStatus.FAILED
                    result.error_message = str(e)
        
        result.end_time = datetime.now()
        return result
    
    def _execute_single_action(self, action: RPAAction, variables: Dict[str, Any]) -> Tuple[bool, Any]:
        """Execute single RPA action."""
        params = self._substitute_variables(action.parameters, variables)
        
        if action.action_type == ActionType.NAVIGATE:
            if not self.web_automation.driver:
                self.web_automation.start_browser()
            
            url = params.get('url')
            return self.web_automation.navigate_to_url(url), None
        
        elif action.action_type == ActionType.CLICK:
            if 'selector' in params:
                # Web click
                selector = params.get('selector')
                selector_type = params.get('selector_type', 'css')
                return self.web_automation.click_element(selector, selector_type), None
            else:
                # Desktop click
                x, y = params.get('x'), params.get('y')
                return self.desktop_automation.click_coordinates(x, y), None
        
        elif action.action_type == ActionType.TYPE:
            text = params.get('text', '')
            
            if 'selector' in params:
                # Web type
                selector = params.get('selector')
                selector_type = params.get('selector_type', 'css')
                return self.web_automation.type_text(selector, text, selector_type), None
            else:
                # Desktop type
                return self.desktop_automation.type_text(text), None
        
        elif action.action_type == ActionType.READ_TEXT:
            selector = params.get('selector')
            selector_type = params.get('selector_type', 'css')
            text = self.web_automation.read_text(selector, selector_type)
            
            # Store result in variable if specified
            variable_name = params.get('variable_name')
            if variable_name and text:
                return True, {variable_name: text}
            
            return text is not None, text
        
        elif action.action_type == ActionType.WAIT:
            duration = params.get('duration', 1)
            time.sleep(duration)
            return True, None
        
        elif action.action_type == ActionType.SCREENSHOT:
            file_path = params.get('file_path', f'/tmp/screenshot_{int(time.time())}.png')
            
            if 'selector' in params:
                return self.web_automation.take_screenshot(file_path), file_path
            else:
                return self.desktop_automation.take_screenshot(file_path), file_path
        
        elif action.action_type == ActionType.API_CALL:
            return self._execute_api_call(params)
        
        elif action.action_type == ActionType.FILE_OPERATION:
            return self._execute_file_operation(params)
        
        else:
            raise NotImplementedError(f"Action type not implemented: {action.action_type}")
    
    def _substitute_variables(self, parameters: Dict[str, Any], variables: Dict[str, Any]) -> Dict[str, Any]:
        """Substitute variables in parameters."""
        substituted = {}
        
        for key, value in parameters.items():
            if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                # Variable substitution
                var_name = value[2:-1]
                substituted[key] = variables.get(var_name, value)
            else:
                substituted[key] = value
        
        return substituted
    
    def _execute_api_call(self, params: Dict[str, Any]) -> Tuple[bool, Any]:
        """Execute API call."""
        try:
            method = params.get('method', 'GET').upper()
            url = params.get('url')
            headers = params.get('headers', {})
            data = params.get('data')
            
            if method == 'GET':
                response = requests.get(url, headers=headers, timeout=30)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=data, timeout=30)
            elif method == 'PUT':
                response = requests.put(url, headers=headers, json=data, timeout=30)
            elif method == 'DELETE':
                response = requests.delete(url, headers=headers, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            
            # Try to parse JSON response
            try:
                result_data = response.json()
            except:
                result_data = response.text
            
            return True, result_data
            
        except Exception as e:
            self.logger.error(f"API call failed: {e}")
            return False, str(e)
    
    def _execute_file_operation(self, params: Dict[str, Any]) -> Tuple[bool, Any]:
        """Execute file operation."""
        operation = params.get('operation')
        
        if operation == 'read':
            file_path = params.get('file_path')
            content = self.file_operations.read_file(file_path)
            return content is not None, content
        
        elif operation == 'write':
            file_path = params.get('file_path')
            content = params.get('content', '')
            return self.file_operations.write_file(file_path, content), None
        
        elif operation == 'copy':
            source = params.get('source_path')
            destination = params.get('destination_path')
            return self.file_operations.copy_file(source, destination), None
        
        elif operation == 'move':
            source = params.get('source_path')
            destination = params.get('destination_path')
            return self.file_operations.move_file(source, destination), None
        
        elif operation == 'delete':
            file_path = params.get('file_path')
            return self.file_operations.delete_file(file_path), None
        
        elif operation == 'list':
            directory = params.get('directory_path')
            pattern = params.get('pattern', '*')
            files = self.file_operations.list_files(directory, pattern)
            return True, files
        
        else:
            raise ValueError(f"Unsupported file operation: {operation}")
    
    def get_execution_status(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get workflow execution status."""
        if execution_id not in self.executions:
            return None
        
        execution = self.executions[execution_id]
        
        return {
            'execution_id': execution.execution_id,
            'workflow_id': execution.workflow_id,
            'status': execution.status.value,
            'start_time': execution.start_time.isoformat(),
            'end_time': execution.end_time.isoformat() if execution.end_time else None,
            'action_count': len(execution.action_results),
            'completed_actions': len([r for r in execution.action_results if r.status == ExecutionStatus.COMPLETED]),
            'failed_actions': len([r for r in execution.action_results if r.status == ExecutionStatus.FAILED]),
            'logs': execution.logs[-10:]  # Last 10 log entries
        }
    
    def cancel_execution(self, execution_id: str) -> bool:
        """Cancel workflow execution."""
        if execution_id not in self.executions:
            return False
        
        execution = self.executions[execution_id]
        execution.status = ExecutionStatus.CANCELLED
        execution.logs.append("Execution cancelled by user")
        
        self.logger.info(f"Cancelled execution: {execution_id}")
        return True
    
    def list_workflows(self) -> List[Dict[str, Any]]:
        """List all registered workflows."""
        return [
            {
                'workflow_id': workflow.workflow_id,
                'name': workflow.name,
                'description': workflow.description,
                'action_count': len(workflow.actions),
                'enabled': workflow.enabled,
                'created_at': workflow.created_at.isoformat()
            }
            for workflow in self.workflows.values()
        ]
    
    def get_platform_statistics(self) -> Dict[str, Any]:
        """Get RPA platform statistics."""
        total_executions = len(self.executions)
        completed_executions = len([e for e in self.executions.values() if e.status == ExecutionStatus.COMPLETED])
        failed_executions = len([e for e in self.executions.values() if e.status == ExecutionStatus.FAILED])
        running_executions = len([e for e in self.executions.values() if e.status == ExecutionStatus.RUNNING])
        
        return {
            'total_workflows': len(self.workflows),
            'enabled_workflows': len([w for w in self.workflows.values() if w.enabled]),
            'total_executions': total_executions,
            'completed_executions': completed_executions,
            'failed_executions': failed_executions,
            'running_executions': running_executions,
            'success_rate': (completed_executions / total_executions * 100) if total_executions > 0 else 0
        }


def main():
    """Example usage of RPA Engine."""
    rpa_engine = RPAEngine()
    
    try:
        print("🤖 Robotic Process Automation Platform")
        
        # Create sample workflow
        print("\n⚙️  Creating RPA workflow...")
        
        # Define workflow actions
        actions = [
            RPAAction(
                action_id="navigate_to_site",
                action_type=ActionType.NAVIGATE,
                parameters={'url': 'https://httpbin.org/forms/post'},
                description="Navigate to test form"
            ),
            RPAAction(
                action_id="fill_customer_name",
                action_type=ActionType.TYPE,
                parameters={
                    'selector': 'input[name="custname"]',
                    'text': '${customer_name}'
                },
                description="Fill customer name"
            ),
            RPAAction(
                action_id="fill_telephone",
                action_type=ActionType.TYPE,
                parameters={
                    'selector': 'input[name="custtel"]',
                    'text': '${phone_number}'
                },
                description="Fill telephone number"
            ),
            RPAAction(
                action_id="fill_email",
                action_type=ActionType.TYPE,
                parameters={
                    'selector': 'input[name="custemail"]',
                    'text': '${email_address}'
                },
                description="Fill email address"
            ),
            RPAAction(
                action_id="take_screenshot",
                action_type=ActionType.SCREENSHOT,
                parameters={'file_path': '/tmp/form_filled.png'},
                description="Take screenshot of filled form"
            ),
            RPAAction(
                action_id="submit_form",
                action_type=ActionType.CLICK,
                parameters={'selector': 'input[type="submit"]'},
                description="Submit the form"
            ),
            RPAAction(
                action_id="wait_for_response",
                action_type=ActionType.WAIT,
                parameters={'duration': 2},
                description="Wait for response"
            )
        ]
        
        # Create workflow
        workflow = RPAWorkflow(
            workflow_id="customer_registration_001",
            name="Customer Registration Automation",
            description="Automate customer registration form submission",
            actions=actions,
            variables={
                'customer_name': 'John Doe',
                'phone_number': '+1-555-0123',
                'email_address': 'john.doe@example.com'
            }
        )
        
        # Register workflow
        rpa_engine.register_workflow(workflow)
        print("✅ RPA workflow created and registered")
        
        # Execute workflow
        print("\n🚀 Executing RPA workflow...")
        
        execution_variables = {
            'customer_name': 'Jane Smith',
            'phone_number': '+1-555-0456',
            'email_address': 'jane.smith@example.com'
        }
        
        execution_id = rpa_engine.execute_workflow(
            "customer_registration_001",
            execution_variables
        )
        
        print(f"✅ Workflow execution started: {execution_id}")
        
        # Monitor execution
        print("\n📊 Monitoring execution...")
        
        for _ in range(30):  # Monitor for up to 30 seconds
            status = rpa_engine.get_execution_status(execution_id)
            
            if status:
                print(f"\r   Status: {status['status']} | "
                      f"Actions: {status['completed_actions']}/{status['action_count']} | "
                      f"Failed: {status['failed_actions']}", end="")
                
                if status['status'] in ['completed', 'failed', 'cancelled']:
                    break
            
            time.sleep(1)
        
        print()  # New line after monitoring
        
        # Show final status
        final_status = rpa_engine.get_execution_status(execution_id)
        if final_status:
            print(f"\n📋 Final Status: {final_status['status'].upper()}")
            
            if final_status['logs']:
                print("📝 Recent Logs:")
                for log in final_status['logs'][-5:]:
                    print(f"   • {log}")
        
        # Create file automation workflow
        print("\n📁 Creating file automation workflow...")
        
        file_actions = [
            RPAAction(
                action_id="create_report_file",
                action_type=ActionType.FILE_OPERATION,
                parameters={
                    'operation': 'write',
                    'file_path': '/tmp/automation_report.txt',
                    'content': 'RPA Automation Report\n\nGenerated at: ${timestamp}\nCustomer: ${customer_name}\nStatus: Completed\n'
                },
                description="Create automation report"
            ),
            RPAAction(
                action_id="list_temp_files",
                action_type=ActionType.FILE_OPERATION,
                parameters={
                    'operation': 'list',
                    'directory_path': '/tmp',
                    'pattern': '*.txt'
                },
                description="List temporary files"
            ),
            RPAAction(
                action_id="api_notification",
                action_type=ActionType.API_CALL,
                parameters={
                    'method': 'POST',
                    'url': 'https://httpbin.org/post',
                    'headers': {'Content-Type': 'application/json'},
                    'data': {
                        'message': 'RPA workflow completed',
                        'customer': '${customer_name}',
                        'timestamp': '${timestamp}'
                    }
                },
                description="Send completion notification"
            )
        ]
        
        file_workflow = RPAWorkflow(
            workflow_id="file_automation_001",
            name="File Processing Automation",
            description="Automate file operations and notifications",
            actions=file_actions,
            variables={
                'timestamp': datetime.now().isoformat(),
                'customer_name': 'Test Customer'
            }
        )
        
        rpa_engine.register_workflow(file_workflow)
        
        # Execute file workflow
        file_execution_id = rpa_engine.execute_workflow("file_automation_001")
        print(f"✅ File automation started: {file_execution_id}")
        
        # Wait for completion
        time.sleep(3)
        
        # Show platform statistics
        print("\n📈 Platform Statistics:")
        stats = rpa_engine.get_platform_statistics()
        
        print(f"   Total Workflows: {stats['total_workflows']}")
        print(f"   Enabled Workflows: {stats['enabled_workflows']}")
        print(f"   Total Executions: {stats['total_executions']}")
        print(f"   Success Rate: {stats['success_rate']:.1f}%")
        
        # List workflows
        print("\n⚙️  Registered Workflows:")
        workflows = rpa_engine.list_workflows()
        
        for workflow in workflows:
            print(f"   🔧 {workflow['name']}")
            print(f"      Actions: {workflow['action_count']}")
            print(f"      Status: {'Enabled' if workflow['enabled'] else 'Disabled'}")
        
        print("\n✅ RPA Platform demo completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()