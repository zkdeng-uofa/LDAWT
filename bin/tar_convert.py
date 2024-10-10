import tarfile
import os
import argparse
from io import BytesIO
from tqdm import tqdm  # Import tqdm

def parse_args() -> argparse.Namespace:
    """
    Parse user inputs from arguments from the command line.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--tar_path", type=str, help="Path to input tar")
    parser.add_argument("--output_tar", type=str, help="Path to output tar")
    return parser.parse_args()

def main():
    inputs = parse_args()
    with tarfile.open(inputs.tar_path, 'r') as input_tar, tarfile.open(inputs.output_tar, 'w') as output_tar:
        members = input_tar.getmembers()
        for member in tqdm(members, desc="Extracting"):
            if member.isfile():
                file_obj = input_tar.extractfile(member)
                data = file_obj.read()

                new_member = tarfile.TarInfo(name=os.path.basename(member.name))
                new_member.size = len(data)

                output_tar.addfile(new_member, BytesIO(data))

if __name__ == "__main__":
    main()
