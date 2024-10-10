import ndcctools.taskvine as vine
import json
import os

def parse_json_config(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    
    with open(file_path, 'r') as config_file:
        return json.load(config_file)

def declare_parquet_files(manager, directory):

    parquet_files = []
    for filename in os.listdir(directory):
        if filename.endswith(".parquet"):
            parquet_files.append(os.path.join(directory, filename))
        else:
            continue
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
    parquet_files = []
    for filename in os.listdir(directory):
        if filename.endswith(".parquet"):
            parquet_files.append(os.path.join(directory, filename))
        else:
            continue
        declared_files = {}

    # Loop over each parquet file and declare it

    for parquet_file in parquet_files:
        # Extract the file name without the directory and extension
        file_name = os.path.basename(parquet_file)
        
        # Declare the file with the manager
        declared_file = manager.declare_file(parquet_file.replace(".parquet", ".tar.gz"))
        
        # Store the declared file in the dictionary with the file name as the key
        declared_files[file_name] = declared_file
    
    return declared_files

configs = parse_json_config("data/json/taskvine.json")

m = vine.Manager(configs['port_number'])
print(f'Listening on port {m.port}')

downloadScript = "bin/ImgDownload.py"
directory = configs['parquets_directory']
downloadScriptVine = m.declare_file(downloadScript)
parquetFiles = declare_parquet_files(m, directory)
outputFiles = declare_output_files(m, directory)

for file_name, declared_file in parquetFiles.items():
    #print(file_name, declared_file, outputFiles[file_name])
    downloadTask = vine.Task(
        f'python {downloadScript} --input_path {file_name} --output_tar {file_name.replace(".parquet", ".tar.gz")} --class_name name',
    )
    inputs = declared_file
    outputs = outputFiles[file_name]
    downloadTask.add_input(downloadScriptVine, downloadScript)
    downloadTask.add_input(inputs, file_name)
    downloadTask.add_output(outputs, file_name.replace(".parquet", ".tar.gz"))
    m.submit(downloadTask)

print("Waiting for tasks to complete...")
while not m.empty():
    task = m.wait(5)
    if task:
        print(f"Task {task.id} completed with result {task.output}")

print("All tasks done.")
