#!/usr/bin/env python3

import argparse
import subprocess
import os
import sys
import psutil
import time

def parse_args():
    parser = argparse.ArgumentParser(description="Start TaskVine worker with optimized settings")
    parser.add_argument("--manager_host", type=str, required=True, help="TaskVine manager host/IP")
    parser.add_argument("--manager_port", type=int, default=9124, help="TaskVine manager port")
    parser.add_argument("--worker_name", type=str, help="Worker name (auto-generated if not provided)")
    parser.add_argument("--cores", type=int, help="Number of cores to use (auto-detected if not provided)")
    parser.add_argument("--memory", type=int, help="Memory in MB to use (auto-detected if not provided)")
    parser.add_argument("--disk", type=int, help="Disk space in MB to use (auto-detected if not provided)")
    parser.add_argument("--timeout", type=int, default=3600, help="Worker timeout in seconds")
    parser.add_argument("--log_file", type=str, help="Log file for worker output")
    return parser.parse_args()

def get_system_resources():
    """Auto-detect optimal system resources"""
    # CPU cores (leave 1 core for system)
    total_cores = psutil.cpu_count(logical=False)
    worker_cores = max(1, total_cores - 1)
    
    # Memory (use 80% of available memory)
    memory = psutil.virtual_memory()
    worker_memory_mb = int((memory.total * 0.8) / (1024 * 1024))
    
    # Disk space (use 70% of available space)
    disk = psutil.disk_usage('/')
    worker_disk_mb = int((disk.free * 0.7) / (1024 * 1024))
    
    return worker_cores, worker_memory_mb, worker_disk_mb

def check_dependencies():
    """Check if required dependencies are available"""
    try:
        import ndcctools.taskvine
        return True
    except ImportError:
        print("Error: ndcctools.taskvine not found. Please install TaskVine.")
        print("Install with: pip install ndcctools")
        return False

def start_worker(args):
    """Start the TaskVine worker with optimized settings"""
    
    if not check_dependencies():
        sys.exit(1)
    
    # Auto-detect resources if not provided
    if not args.cores or not args.memory or not args.disk:
        auto_cores, auto_memory, auto_disk = get_system_resources()
        
        cores = args.cores or auto_cores
        memory = args.memory or auto_memory
        disk = args.disk or auto_disk
    else:
        cores = args.cores
        memory = args.memory
        disk = args.disk
    
    # Generate worker name if not provided
    if not args.worker_name:
        import socket
        hostname = socket.gethostname()
        worker_name = f"worker-{hostname}-{int(time.time())}"
    else:
        worker_name = args.worker_name
    
    # Build the vine_worker command
    cmd = [
        "vine_worker",
        "--manager-name", f"{args.manager_host}:{args.manager_port}",
        "--cores", str(cores),
        "--memory", str(memory),
        "--disk", str(disk),
        "--timeout", str(args.timeout),
        "--workdir", f"/tmp/{worker_name}",
        "--name", worker_name
    ]
    
    # Add logging if specified
    if args.log_file:
        cmd.extend(["--debug", "all", "--debug-file", args.log_file])
    
    print(f"Starting TaskVine worker:")
    print(f"  Manager: {args.manager_host}:{args.manager_port}")
    print(f"  Worker Name: {worker_name}")
    print(f"  Resources: {cores} cores, {memory} MB memory, {disk} MB disk")
    print(f"  Timeout: {args.timeout} seconds")
    if args.log_file:
        print(f"  Log file: {args.log_file}")
    print(f"  Command: {' '.join(cmd)}")
    print()
    
    # Create work directory
    workdir = f"/tmp/{worker_name}"
    os.makedirs(workdir, exist_ok=True)
    
    try:
        # Start the worker
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        print("Worker started successfully!")
        print("Press Ctrl+C to stop the worker")
        print("-" * 50)
        
        # Monitor worker output
        for line in iter(process.stdout.readline, ''):
            if line:
                print(f"[Worker] {line.strip()}")
                
        process.wait()
        
    except KeyboardInterrupt:
        print("\nShutting down worker...")
        process.terminate()
        process.wait()
        
    except Exception as e:
        print(f"Error starting worker: {e}")
        sys.exit(1)
    
    finally:
        # Cleanup work directory
        try:
            import shutil
            shutil.rmtree(workdir)
        except Exception:
            pass

def main():
    args = parse_args()
    start_worker(args)

if __name__ == '__main__':
    main() 