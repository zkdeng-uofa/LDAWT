#!/usr/bin/env python3

def compare_files():
    """
    Compare new_insects_cloud.txt and new_insects_local.txt
    Find entries that are in cloud but not in local -> save to new_insects_not_cloud.txt
    Find entries that are in local but not in cloud -> save to new_insects_not_local.txt
    
    Cloud file has format:
    /iplant/home/zkdeng/AIIRA_New_Insects:
      group_100.tar.gz
      group_1002.tar.gz
    
    Local file has format:
    group_1.tar.gz
    group_10.tar.gz
    """
    
    # Read the local file
    print("Reading new_insects_local.txt...")
    with open('new_insects_local.txt', 'r') as f:
        local_entries = set(line.strip() for line in f if line.strip())
    
    # Read the cloud file and process it properly
    print("Reading new_insects_cloud.txt...")
    cloud_entries = set()
    with open('new_insects_cloud.txt', 'r') as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and header lines (lines that don't end with .tar.gz)
            if line and line.endswith('.tar.gz'):
                # Remove any leading spaces and add to set
                filename = line.strip()
                cloud_entries.add(filename)
    
    # Find entries in cloud but not in local
    print("Comparing files...")
    cloud_not_in_local = cloud_entries - local_entries
    local_not_in_cloud = local_entries - cloud_entries
    
    # Sort for consistent output
    cloud_not_in_local_sorted = sorted(cloud_not_in_local)
    local_not_in_cloud_sorted = sorted(local_not_in_cloud)
    
    # Write results to files
    print(f"Writing {len(cloud_not_in_local_sorted)} entries to new_insects_not_cloud.txt...")
    with open('new_insects_not_cloud.txt', 'w') as f:
        for entry in cloud_not_in_local_sorted:
            f.write(entry + '\n')
    
    print(f"Writing {len(local_not_in_cloud_sorted)} entries to new_insects_not_local.txt...")
    with open('new_insects_not_local.txt', 'w') as f:
        for entry in local_not_in_cloud_sorted:
            f.write(entry + '\n')
    
    # Print summary
    print(f"\nSummary:")
    print(f"Local entries: {len(local_entries)}")
    print(f"Cloud entries: {len(cloud_entries)}")
    print(f"Entries in cloud but not in local: {len(cloud_not_in_local_sorted)}")
    print(f"Entries in local but not in cloud: {len(local_not_in_cloud_sorted)}")
    
    if cloud_not_in_local_sorted:
        print(f"\nFirst 10 entries in cloud but not in local:")
        for entry in cloud_not_in_local_sorted[:10]:
            print(f"  {entry}")
    else:
        print("\nAll cloud entries are present in local file.")
    
    if local_not_in_cloud_sorted:
        print(f"\nFirst 10 entries in local but not in cloud:")
        for entry in local_not_in_cloud_sorted[:10]:
            print(f"  {entry}")
    else:
        print("\nAll local entries are present in cloud file.")

if __name__ == "__main__":
    compare_files() 