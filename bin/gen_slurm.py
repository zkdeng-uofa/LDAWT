#!/usr/bin/env python
import argparse
import os

def parse_args() -> argparse.Namespace:
    """
    Parse user arguments from command line
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--copies",
        type = str
    )
    parser.add_argument(
        "--slurm_path",
        type = str
    )
    parser.add_argument(
        "--parquet_name",
        type = str
    )
    args = parser.parse_args()
    
    return args


def main():
    inputs = parse_args()
    search_string = inputs.parquet_name
    # Create a file for each number of copies requested
    for i in range(1, int(inputs.copies)+1):
        output_file = inputs.slurm_path.split("/")[-1].replace("1", str(i))
        replace_string = search_string.replace("1", str(i))

        # Open original file and write new contents to a new file
        with open(inputs.slurm_path, "r") as infile, open(output_file, "w") as outfile:
            for line in infile:
                # Replace parquet_name with new name
                modified_line = line.replace(search_string, replace_string)

                # Write contents into new file
                outfile.write(modified_line)

if __name__ == "__main__":
    main()