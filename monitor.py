#!/usr/bin/env python3
"""
Dark Automation - Real-time Monitoring Dashboard
Advanced system monitoring and alerting interface
"""

import time
import json
import psutil
import threading
from datetime import datetime
from collections import deque

class MonitoringDashboard:
    """Real-time system monitoring dashboard"""
    
    def __init__(self):
        self.version = "1.0.0"
        self.running = False
        self.metrics_history = {
            'cpu': deque(maxlen=100),
            'memory': deque(maxlen=100),
            'disk': deque(maxlen=100),
            'network': deque(maxlen=100)
        }
        
    def display_header(self):
        """Display dashboard header"""
        header = """
        ╔══════════════════════════════════════════════════════════════╗
        ║                    DARK MONITORING DASHBOARD                 ║
        ║                     Real-time System Metrics                ║
        ╚══════════════════════════════════════════════════════════════╝
        """
        print(header)
    
    def get_system_metrics(self):
        """Collect current system metrics"""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            network = psutil.net_io_counters()
            
            metrics = {
                'timestamp': datetime.now().isoformat(),
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_used_gb': round(memory.used / (1024**3), 2),
                'memory_total_gb': round(memory.total / (1024**3), 2),
                'disk_percent': round((disk.used / disk.total) * 100, 2),
                'disk_used_gb': round(disk.used / (1024**3), 2),
                'disk_total_gb': round(disk.total / (1024**3), 2),
                'network_bytes_sent': network.bytes_sent,
                'network_bytes_recv': network.bytes_recv
            }
            
            return metrics
            
        except Exception as e:
            print(f"Error collecting metrics: {e}")
            return None
    
    def update_metrics_history(self, metrics):
        """Update metrics history for trending"""
        if metrics:
            self.metrics_history['cpu'].append(metrics['cpu_percent'])
            self.metrics_history['memory'].append(metrics['memory_percent'])
            self.metrics_history['disk'].append(metrics['disk_percent'])
            self.metrics_history['network'].append(metrics['network_bytes_sent'])
    
    def display_metrics(self, metrics):
        """Display current system metrics"""
        if not metrics:
            return
            
        print(f"\n⏰ Last Update: {metrics['timestamp']}")
        print("=" * 60)
        
        # CPU Information
        cpu_bar = self.create_progress_bar(metrics['cpu_percent'])
        print(f"🖥️  CPU Usage:    {metrics['cpu_percent']:6.1f}% {cpu_bar}")
        
        # Memory Information
        memory_bar = self.create_progress_bar(metrics['memory_percent'])
        print(f"🧠 Memory Usage: {metrics['memory_percent']:6.1f}% {memory_bar}")
        print(f"   Used: {metrics['memory_used_gb']}GB / {metrics['memory_total_gb']}GB")
        
        # Disk Information
        disk_bar = self.create_progress_bar(metrics['disk_percent'])
        print(f"💾 Disk Usage:   {metrics['disk_percent']:6.1f}% {disk_bar}")
        print(f"   Used: {metrics['disk_used_gb']}GB / {metrics['disk_total_gb']}GB")
        
        # Network Information
        print(f"🌐 Network:")
        print(f"   Sent: {self.format_bytes(metrics['network_bytes_sent'])}")
        print(f"   Recv: {self.format_bytes(metrics['network_bytes_recv'])}")
        
        print("=" * 60)
    
    def create_progress_bar(self, percentage, width=20):
        """Create ASCII progress bar"""
        filled = int(width * percentage / 100)
        bar = '█' * filled + '░' * (width - filled)
        
        if percentage > 90:
            color = '\033[91m'  # Red
        elif percentage > 70:
            color = '\033[93m'  # Yellow
        else:
            color = '\033[92m'  # Green
            
        return f"{color}[{bar}]\033[0m"
    
    def format_bytes(self, bytes_value):
        """Format bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"
    
    def display_alerts(self, metrics):
        """Display system alerts"""
        alerts = []
        
        if metrics['cpu_percent'] > 80:
            alerts.append(f"🚨 HIGH CPU USAGE: {metrics['cpu_percent']:.1f}%")
        
        if metrics['memory_percent'] > 85:
            alerts.append(f"🚨 HIGH MEMORY USAGE: {metrics['memory_percent']:.1f}%")
            
        if metrics['disk_percent'] > 90:
            alerts.append(f"🚨 HIGH DISK USAGE: {metrics['disk_percent']:.1f}%")
        
        if alerts:
            print("\n⚠️  SYSTEM ALERTS:")
            for alert in alerts:
                print(f"   {alert}")
            print()
    
    def save_metrics_log(self, metrics):
        """Save metrics to log file"""
        try:
            with open('monitoring.log', 'a') as f:
                f.write(f"{json.dumps(metrics)}\n")
        except Exception as e:
            print(f"Error saving metrics: {e}")
    
    def start_monitoring(self, interval=5):
        """Start real-time monitoring"""
        self.running = True
        print("🚀 Starting Dark Monitoring Dashboard...")
        print("Press Ctrl+C to stop monitoring\n")
        
        try:
            while self.running:
                # Clear screen (works on most terminals)
                print('\033[2J\033[H')
                
                self.display_header()
                
                # Get and display current metrics
                metrics = self.get_system_metrics()
                if metrics:
                    self.update_metrics_history(metrics)
                    self.display_metrics(metrics)
                    self.display_alerts(metrics)
                    self.save_metrics_log(metrics)
                
                # Wait for next update
                time.sleep(interval)
                
        except KeyboardInterrupt:
            self.stop_monitoring()
    
    def stop_monitoring(self):
        """Stop monitoring"""
        self.running = False
        print("\n\n🛑 Monitoring stopped. Dashboard closed.")

def main():
    """Main entry point"""
    dashboard = MonitoringDashboard()
    
    try:
        dashboard.start_monitoring(interval=3)
    except Exception as e:
        print(f"Dashboard error: {e}")

if __name__ == "__main__":
    main()