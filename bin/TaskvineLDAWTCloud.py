import ndcctools.taskvine as vine
import json
import os
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="Config File")
    parser.add_argument(
        "--config_file", 
        type=str,
        default="data/json/taskvine.json",
        help="Path to the configuration JSON file.")
    return parser.parse_args()

def parse_json_config(file_path):
    """
    Load and parse the JSON configuration file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    
    with open(file_path, 'r') as config_file:
        return json.load(config_file)

def declare_parquet_files(manager, directory):
    """
    Declare input parquet files to TaskVine manager.
    """
    parquet_files = [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith(".parquet")]
    declared_files = {}

    # Loop over each parquet file and declare it
    for parquet_file in parquet_files:
        # Extract the file name without the directory and extension
        file_name = os.path.basename(parquet_file)
        
        # Declare the file with the manager
        declared_file = manager.declare_file(parquet_file)
        
        # Store the declared file in the dictionary with the file name as the key
        declared_files[file_name] = declared_file
    
    return declared_files

def declare_output_files(manager, directory):
    """
    Declare output tar.gz files for TaskVine manager.
    """
    parquet_files = [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith(".parquet")]
    declared_files = {}

    # Loop over each parquet file and declare it
    for parquet_file in parquet_files:
        # Extract the file name without the directory and extension
        file_name = os.path.basename(parquet_file)
        
        # Declare the output tar.gz file with the manager
        declared_file = manager.declare_file(parquet_file.replace(".parquet", ".tar.gz"))
        
        # Store the declared file in the dictionary with the file name as the key
        declared_files[file_name] = declared_file
    
    return declared_files

def submit_tasks(manager, download_script, parquet_files, output_files):
    """
    Submit download tasks to TaskVine.
    """
    download_script_vine = manager.declare_file(download_script)

    for file_name, declared_file in parquet_files.items():
        # Create the TaskVine task
        download_task = vine.Task(
            f'python {download_script} --input_path {file_name} --output_tar {file_name.replace(".parquet", ".tar.gz")} --class_name name',
        )
        inputs = declared_file
        outputs = output_files[file_name]
        
        # Add inputs and outputs to the task
        download_task.add_input(download_script_vine, download_script)
        download_task.add_input(inputs, file_name)
        download_task.add_output(outputs, file_name.replace(".parquet", ".tar.gz"))

        # Submit the task to the manager
        manager.submit(download_task)

def submit_tasks_cloud(manager, download_script, parquet_files, output_files):
    """
    Submit download tasks to TaskVine.
    """
    download_script_vine = manager.declare_file(download_script)

    for file_name, declared_file in parquet_files.items():
        # Create the TaskVine task
        download_task = vine.Task(
            f'python {download_script} --input_path {file_name} --output_tar {file_name.replace(".parquet", ".tar.gz")} --class_name name && ' +
            f'gocmd put -f {file_name.replace(".parquet", ".tar.gz")}',
        )
        inputs = declared_file
        outputs = output_files[file_name]
        
        # Add inputs and outputs to the task
        download_task.add_input(download_script_vine, download_script)
        download_task.add_input(inputs, file_name)
        #download_task.add_output(outputs, file_name.replace(".parquet", ".tar.gz"))


        # Submit the task to the manager
        manager.submit(download_task)

def main():
    # Parse command-line arguments
    args = parse_args()

    # Load the configuration file specified by the user
    configs = parse_json_config(args.config_file)

    # Initialize the TaskVine manager
    manager = vine.Manager(configs['port_number'])
    print(f'Listening on port {manager.port}')

    # Declare the input and output files to TaskVine
    download_script = "bin/ImgDownload.py"
    directory = configs['parquets_directory']
    
    parquet_files = declare_parquet_files(manager, directory)
    output_files = declare_output_files(manager, directory)

    # Submit the tasks
    submit_tasks_cloud(manager, download_script, parquet_files, output_files)

    # Wait for tasks to complete
    print("Waiting for tasks to complete...")
    while not manager.empty():
        task = manager.wait(5)
        if task:
            print(f"Task {task.id} completed with result {task.output}")

    print("All tasks done.")

if __name__ == '__main__':
    main()
