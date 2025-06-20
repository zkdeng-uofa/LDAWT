#!/usr/bin/env python3

import subprocess
import threading
import queue
import time
from pathlib import Path

def upload_file(filename, result_queue):
    """
    Upload a single file to Cyverse using gocmd put
    """
    try:
        cmd = f"gocmd put --progress {filename} AIIRA_New_Insects"
        print(f"Starting upload: {filename}")
        
        start_time = time.time()
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        end_time = time.time()
        
        if result.returncode == 0:
            print(f"✓ Successfully uploaded {filename} ({end_time - start_time:.1f}s)")
            result_queue.put(("success", filename, end_time - start_time))
        else:
            print(f"✗ Failed to upload {filename}: {result.stderr}")
            result_queue.put(("error", filename, result.stderr))
    except Exception as e:
        print(f"✗ Exception uploading {filename}: {str(e)}")
        result_queue.put(("exception", filename, str(e)))

def upload_files_parallel():
    """
    Read new_insects_not_local.txt and upload files in parallel (3 at a time)
    """
    # Check if the file exists
    input_file = "../new_insects_not_local.txt"
    if not Path(input_file).exists():
        print(f"Error: {input_file} not found!")
        return
    
    # Read all filenames
    print(f"Reading filenames from {input_file}...")
    with open(input_file, 'r') as f:
        filenames = [line.strip() for line in f if line.strip()]
    
    print(f"Found {len(filenames)} files to upload")
    
    if not filenames:
        print("No files to upload!")
        return
    
    # Check if files exist locally
    existing_files = []
    missing_files = []
    for filename in filenames:
        if Path(filename).exists():
            existing_files.append(filename)
        else:
            missing_files.append(filename)
    
    if missing_files:
        print(f"\nWarning: {len(missing_files)} files not found locally:")
        for missing in missing_files[:10]:  # Show first 10
            print(f"  - {missing}")
        if len(missing_files) > 10:
            print(f"  ... and {len(missing_files) - 10} more")
        print()
    
    print(f"Uploading {len(existing_files)} existing files...")
    
    # Set up parallel processing
    max_concurrent = 25
    result_queue = queue.Queue()
    active_threads = []
    file_index = 0
    
    successful_uploads = 0
    failed_uploads = 0
    total_files = len(existing_files)
    
    print(f"Starting parallel upload with {max_concurrent} concurrent processes...\n")
    
    while file_index < total_files or active_threads:
        # Start new threads if we have capacity and files left
        while len(active_threads) < max_concurrent and file_index < total_files:
            filename = existing_files[file_index]
            thread = threading.Thread(target=upload_file, args=(filename, result_queue))
            thread.start()
            active_threads.append(thread)
            file_index += 1
        
        # Check for completed threads
        active_threads = [t for t in active_threads if t.is_alive()]
        
        # Process results
        try:
            while True:
                status, filename, info = result_queue.get_nowait()
                if status == "success":
                    successful_uploads += 1
                else:
                    failed_uploads += 1
                    
                progress = successful_uploads + failed_uploads
                print(f"Progress: {progress}/{total_files} files processed")
                
        except queue.Empty:
            pass
        
        # Small delay to prevent busy waiting
        time.sleep(0.1)
    
    # Final summary
    print(f"\n" + "="*50)
    print(f"Upload Summary:")
    print(f"Total files to upload: {total_files}")
    print(f"Successful uploads: {successful_uploads}")
    print(f"Failed uploads: {failed_uploads}")
    if missing_files:
        print(f"Files not found locally: {len(missing_files)}")
    print(f"="*50)

if __name__ == "__main__":
    upload_files_parallel() 