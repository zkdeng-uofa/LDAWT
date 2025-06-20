import ndcctools.taskvine as vine
import json
import os
import argparse
import time
import sys

def parse_args():
    parser = argparse.ArgumentParser(description="Config File")
    parser.add_argument(
        "--config_file", 
        type=str,
        default="files/config_files/taskvineCloud.json",
        help="Path to the configuration JSON file.")
    return parser.parse_args()

def parse_json_config(file_path):
    """
    Load and parse the JSON configuration file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    
    with open(file_path, 'r') as config_file:
        config = json.load(config_file)
    
    # Set defaults for missing keys
    config.setdefault('class_col', 'species_name')
    config.setdefault('url_col', 'photo_url')
    config.setdefault('timeout_minutes', 30)
    config.setdefault('max_retries', 3)
    
    return config

def declare_parquet_files(manager, directory):
    """
    Declare input parquet files to TaskVine manager.
    """
    if not os.path.exists(directory):
        print(f"Error: Directory {directory} does not exist")
        return {}
        
    parquet_files = [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith(".parquet")]
    
    if not parquet_files:
        print(f"Warning: No parquet files found in {directory}")
        return {}
        
    declared_files = {}

    # Loop over each parquet file and declare it
    for parquet_file in parquet_files:
        # Extract the file name without the directory and extension
        file_name = os.path.basename(parquet_file)
        
        # Declare the file with the manager
        try:
            declared_file = manager.declare_file(parquet_file)
            # Store the declared file in the dictionary with the file name as the key
            declared_files[file_name] = declared_file
            print(f"Declared input file: {file_name}")
        except Exception as e:
            print(f"Error declaring file {parquet_file}: {e}")
    
    return declared_files

def declare_output_files(manager, directory):
    """
    Declare output tar.gz files for TaskVine manager.
    """
    if not os.path.exists(directory):
        print(f"Error: Directory {directory} does not exist")
        return {}
        
    parquet_files = [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith(".parquet")]
    declared_files = {}

    # Loop over each parquet file and declare it
    for parquet_file in parquet_files:
        # Extract the file name without the directory and extension
        file_name = os.path.basename(parquet_file)
        
        # Create output tar.gz filename
        output_file = file_name.replace(".parquet", ".tar.gz")
        
        # Declare the output tar.gz file with the manager
        try:
            declared_file = manager.declare_file(output_file)
            # Store the declared file in the dictionary with the file name as the key
            declared_files[file_name] = declared_file
            print(f"Declared output file: {output_file}")
        except Exception as e:
            print(f"Error declaring output file {output_file}: {e}")
    
    return declared_files

def submit_tasks_cloud(manager, download_script, parquet_files, output_files, config):
    """
    Submit download tasks to TaskVine with proper error handling and monitoring.
    """
    download_script_vine = manager.declare_file(download_script)
    
    class_col = config.get('class_col', 'species_name')
    url_col = config.get('url_col', 'photo_url')
    timeout_minutes = config.get('timeout_minutes', 30)
    max_retries = config.get('max_retries', 3)

    submitted_tasks = 0
    for file_name, declared_file in parquet_files.items():
        try:
            # Create the TaskVine task
            download_task = vine.Task(
                f'python {download_script} --input_path {file_name} --output_tar {file_name.replace(".parquet", ".tar.gz")} --url_name {url_col} --class_name {class_col} && ' +
                f'gocmd put {file_name.replace(".parquet", ".tar.gz")} AIIRA_New_Insects'
            )
            
            # Set basic task properties
            download_task.set_retries(max_retries)
            
            # Set resource requirements (adjust based on your worker capabilities)
            download_task.set_cores(8)
            # download_task.set_memory(16000)  # 16GB memory limit
            # download_task.set_disk(50000)    # 50GB disk space
            
            inputs = declared_file
            outputs = output_files[file_name]
            
            # Add inputs and outputs to the task
            download_task.add_input(download_script_vine, download_script)
            download_task.add_input(inputs, file_name)
            download_task.add_output(outputs, f'{file_name.replace(".parquet", ".tar.gz")}')

            # Submit the task to the manager
            task_id = manager.submit(download_task)
            submitted_tasks += 1
            print(f"Submitted task {task_id} for {file_name}")
            
        except Exception as e:
            print(f"Error submitting task for {file_name}: {e}")

    print(f"Successfully submitted {submitted_tasks} tasks")
    return submitted_tasks

def monitor_tasks(manager, total_tasks):
    """
    Monitor task completion with detailed status reporting.
    """
    print("Waiting for tasks to complete...")
    completed_tasks = 0
    failed_tasks = 0
    start_time = time.time()
    
    while not manager.empty():
        task = manager.wait(5)
        if task:
            completed_tasks += 1
            elapsed_time = time.time() - start_time
            
            if task.successful():
                print(f"✓ Task {task.id} completed successfully ({completed_tasks}/{total_tasks}) - Elapsed: {elapsed_time:.1f}s")
                if task.output:
                    print(f"  Output: {task.output.strip()}")
            else:
                failed_tasks += 1
                print(f"✗ Task {task.id} FAILED ({completed_tasks}/{total_tasks}) - Elapsed: {elapsed_time:.1f}s")
                print(f"  Exit code: {task.exit_code}")
                if task.output:
                    print(f"  Output: {task.output.strip()}")
                if hasattr(task, 'result') and task.result:
                    print(f"  Result: {task.result}")
        else:
            # Print status every 30 seconds when no task completes
            if int(time.time()) % 30 == 0:
                elapsed_time = time.time() - start_time
                print(f"Status: {completed_tasks}/{total_tasks} completed, {manager.stats.tasks_running} running, {manager.stats.tasks_waiting} waiting - Elapsed: {elapsed_time:.1f}s")

    print(f"\nAll tasks completed! Success: {completed_tasks - failed_tasks}, Failed: {failed_tasks}")
    return completed_tasks - failed_tasks, failed_tasks

def main():
    # Parse command-line arguments
    args = parse_args()

    # Load the configuration file specified by the user
    try:
        configs = parse_json_config(args.config_file)
        print(f"Loaded configuration from {args.config_file}")
    except Exception as e:
        print(f"Error loading configuration: {e}")
        sys.exit(1)

    # Initialize the TaskVine manager
    try:
        manager = vine.Manager(configs['port_number'])
        print(f'TaskVine Manager listening on port {manager.port}')
        
        # Set manager properties for better performance
        manager.tune("worker-retrievals", 5)
        manager.tune("transfer-temps-recovery", 1)
        
    except Exception as e:
        print(f"Error initializing TaskVine manager: {e}")
        sys.exit(1)

    # Declare the input and output files to TaskVine
    download_script = "bin/ImgDownloadBW.py"
    
    if not os.path.exists(download_script):
        print(f"Error: Download script not found: {download_script}")
        sys.exit(1)
        
    directory = configs['parquets_directory']
    
    parquet_files = declare_parquet_files(manager, directory)
    if not parquet_files:
        print("No parquet files to process. Exiting.")
        sys.exit(1)
        
    output_files = declare_output_files(manager, directory)

    # Submit the tasks
    total_tasks = submit_tasks_cloud(manager, download_script, parquet_files, output_files, configs)
    
    if total_tasks == 0:
        print("No tasks were submitted. Exiting.")
        sys.exit(1)

    # Monitor task completion
    successful_tasks, failed_tasks = monitor_tasks(manager, total_tasks)
    
    if failed_tasks > 0:
        print(f"Warning: {failed_tasks} tasks failed out of {total_tasks}")
        sys.exit(1)
    else:
        print("All tasks completed successfully!")

if __name__ == '__main__':
    main()
