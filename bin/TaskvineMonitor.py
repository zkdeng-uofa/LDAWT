#!/usr/bin/env python3

import argparse
import time
import json
import os
import psutil
import signal
import sys
from datetime import datetime

def parse_args():
    parser = argparse.ArgumentParser(description="Monitor TaskVine performance and resource usage")
    parser.add_argument("--port", type=int, default=9124, help="TaskVine manager port")
    parser.add_argument("--log_file", type=str, default="taskvine_monitor.log", help="Log file for monitoring output")
    parser.add_argument("--interval", type=int, default=10, help="Monitoring interval in seconds")
    parser.add_argument("--disk_path", type=str, default=None, help="Path to monitor for disk usage (default: current working directory)")
    return parser.parse_args()

class TaskVineMonitor:
    def __init__(self, port, log_file, interval, disk_path=None):
        self.port = port
        self.log_file = log_file
        self.interval = interval
        self.disk_path = disk_path or os.getcwd()  # Use current working directory if not specified
        self.running = True
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def signal_handler(self, sig, frame):
        print("\nShutting down monitor...")
        self.running = False
        
    def log_message(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{timestamp}] {message}"
        print(log_msg)
        
        with open(self.log_file, 'a') as f:
            f.write(log_msg + "\n")
            
    def get_system_stats(self):
        """Get system resource usage statistics"""
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        
        # Monitor disk usage for the specified path (mounted drive)
        try:
            disk = psutil.disk_usage(self.disk_path)
        except Exception as e:
            self.log_message(f"Error accessing disk path {self.disk_path}: {e}")
            # Fallback to root if path is inaccessible
            disk = psutil.disk_usage('/')
        
        return {
            'cpu_percent': cpu_percent,
            'memory_percent': memory.percent,
            'memory_available_gb': memory.available / (1024**3),
            'disk_percent': disk.percent,
            'disk_free_gb': disk.free / (1024**3),
            'disk_total_gb': disk.total / (1024**3),
            'disk_used_gb': disk.used / (1024**3),
            'disk_path': self.disk_path
        }
        
    def check_port_status(self):
        """Check if TaskVine manager port is accessible"""
        import socket
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('localhost', self.port))
            sock.close()
            return result == 0
        except Exception:
            return False
            
    def get_network_stats(self):
        """Get network I/O statistics"""
        net_io = psutil.net_io_counters()
        return {
            'bytes_sent': net_io.bytes_sent,
            'bytes_recv': net_io.bytes_recv,
            'packets_sent': net_io.packets_sent,
            'packets_recv': net_io.packets_recv
        }
        
    def check_file_descriptors(self):
        """Check file descriptor usage"""
        try:
            current_process = psutil.Process()
            fd_count = current_process.num_fds()
            return fd_count
        except Exception:
            return -1
            
    def monitor(self):
        """Main monitoring loop"""
        self.log_message(f"Starting TaskVine monitoring on port {self.port}")
        self.log_message(f"Monitoring interval: {self.interval} seconds")
        self.log_message(f"Monitoring disk usage for path: {self.disk_path}")
        
        initial_net_stats = self.get_network_stats()
        
        while self.running:
            try:
                # System stats
                sys_stats = self.get_system_stats()
                
                # Port status
                port_active = self.check_port_status()
                
                # Network stats
                current_net_stats = self.get_network_stats()
                
                # File descriptors
                fd_count = self.check_file_descriptors()
                
                # Calculate network delta since start
                net_sent_mb = (current_net_stats['bytes_sent'] - initial_net_stats['bytes_sent']) / (1024**2)
                net_recv_mb = (current_net_stats['bytes_recv'] - initial_net_stats['bytes_recv']) / (1024**2)
                
                status_msg = (
                    f"TaskVine Status - Port {self.port}: {'ACTIVE' if port_active else 'INACTIVE'} | "
                    f"CPU: {sys_stats['cpu_percent']:.1f}% | "
                    f"Memory: {sys_stats['memory_percent']:.1f}% ({sys_stats['memory_available_gb']:.1f}GB free) | "
                    f"Disk [{sys_stats['disk_path']}]: {sys_stats['disk_percent']:.1f}% "
                    f"({sys_stats['disk_used_gb']:.1f}GB/{sys_stats['disk_total_gb']:.1f}GB, {sys_stats['disk_free_gb']:.1f}GB free) | "
                    f"Network: ↑{net_sent_mb:.1f}MB ↓{net_recv_mb:.1f}MB | "
                    f"FDs: {fd_count}"
                )
                
                self.log_message(status_msg)
                
                # Warning conditions
                if sys_stats['memory_percent'] > 90:
                    self.log_message("WARNING: High memory usage detected!")
                    
                if sys_stats['disk_percent'] > 90:
                    self.log_message("WARNING: High disk usage detected!")
                    
                if fd_count > 1000:
                    self.log_message("WARNING: High file descriptor usage detected!")
                    
                if not port_active:
                    self.log_message("WARNING: TaskVine manager port is not accessible!")
                
                time.sleep(self.interval)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.log_message(f"Error during monitoring: {e}")
                time.sleep(self.interval)
                
        self.log_message("Monitoring stopped")

def main():
    args = parse_args()
    monitor = TaskVineMonitor(args.port, args.log_file, args.interval, args.disk_path)
    monitor.monitor()

if __name__ == '__main__':
    main() 