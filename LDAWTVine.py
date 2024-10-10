import ndcctools.taskvine as vine

import json
import os

def parse_json_config(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    
    with open(file_path, 'r') as config_file:
        return json.load(config_file)


m = vine.Manager(9123)
print(f"Listening on port {m.port}")


downloadScript = "ImgDownload.py"
urlList = "group_10.parquet"

downloadScriptM = m.declare_file(downloadScript)
urlListM = m.declare_file(urlList)
outputM = m.declare_temp()

downloadTask = vine.Task(f"python {downloadScript} --input_path {urlList} --output_folder output")
downloadTask.add_input(downloadScriptM, downloadScript)
downloadTask.add_input(urlListM, urlList)
downloadTask.add_output(outputM, "output")
m.submit(downloadTask)

print("Waiting for tasks to complete...")
while not m.empty():
    task = m.wait(5)
    if task:
        print(f"Task {task.id} completed with result {task.output}")

print("All tasks done.")