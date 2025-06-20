import subprocess

def get_local_ip():
    try:
        # Run the 'hostname -I' command
        result = subprocess.run(['hostname', '-I'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        # Get the output, strip any extra spaces or newlines
        ip_addresses = result.stdout.strip()
        return ip_addresses
    except subprocess.CalledProcessError as e:
        return f"Error: {e}"

# Get and print the local IP addresses

def main():
    local_ips = get_local_ip()
    print(f"My local IP addresses are: {local_ips}")
    print(local_ips.split()[0])
    managerIp = local_ips.split()[0]

    slurm_script = """#!/bin/bash
    #SBATCH --job-name=taskvine           
    #SBATCH --output=result.out           
    #SBATCH --account=nirav
    #SBATCH --partition=standard           
    #SBATCH --nodes=1
    #SBATCH --ntasks=2                    
    #SBATCH --time=01:00:00               

    # Load any necessary modules
    module load python/3.9
    source ~/.bashrc
    conda init bash
    conda init
    conda activate taskvine

    # Execute the Python script or command
    vine_submit_workers -T slurm -p "--cpus-per-task=4 --time=1:00:00" -t 01:00:00 """+managerIp+""" 9124 2

    """

    slurm_filename = "taskvine.slurm"
    with open(slurm_filename, "w") as slurm_file:
        slurm_file.write(slurm_script)

    print(f"SLURM script saved as {slurm_filename}")

if __name__ == "__main__":
    main()