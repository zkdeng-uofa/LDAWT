#!/bin/bash

# Get the script's directory
script_dir="$(dirname "$0")"

# File name to be copied
file_to_copy="LDAWT1.slurm"
#output="download2.slurm"

string_start="LDAWT"
extension=".slurm"

number="$1"

#read -p "Number: " number
destination_file="${string_start}${number}${extension}"

# Source file and destination file paths
source_file="$script_dir/$file_to_copy"

#read -p "Destination: " destination_file

# Copy the file
cp "$source_file" "$destination_file"

title_line="#SBATCH --job-name=LDAWT$number"
sed -i "7s#.*#${title_line}#" "$destination_file" 

# Modify Lines
iget_line="iget -rfPT AIIRA/group_$number.parquet"
sed -i "35s#.*#${iget_line}#" "$destination_file"

fixRow_line="python fixRows.py --input group_$number.parquet"
sed -i "36s#.*#${fixRow_line}#" "$destination_file"

download_line="time python ImgDownload.py --input_path group_$number.parquet --output_folder group_$number"
sed -i "37s#.*#${download_line}#" "$destination_file"

tar_line="time tar -cf group_$number.tar group_$number"
sed -i "38s#.*#${tar_line}#" "$destination_file"

iput_line="time iput -fPT group_$number.tar foundation_imgs/group_$number.tar"
sed -i "40s#.*#${iput_line}#" "$destination_file"

rm_tar_line = "rm -r group_$number.tar"
sed -i "42s#.*#{rm_tar_line}" ""

rm_tar_line = "rm -r group_$number"
sed -i "43s#.*#{rm_tar_line}" ""

echo "File copied successfully!"
