#!/usr/bin/env python3

import pandas as pd
import os
import tarfile
from pathlib import Path
from tqdm import tqdm
from PIL import Image
import io
import argparse

def parse_args():
    """
    Parse user inputs from arguments using argparse.
    """
    parser = argparse.ArgumentParser(description="Extract and save images from a Parquet file containing byte data.")
    
    parser.add_argument("--input_path", type=str, required=True, help="Path to the input Parquet file.")
    parser.add_argument("--output_tar", type=str, required=True, help="Path to the output tar file (e.g., 'images.tar.gz').")
    parser.add_argument("--image_col", type=str, default="image", help="Column name containing the image data.")
    parser.add_argument("--label_col", type=str, default="label", help="Column name containing the class labels.")
    parser.add_argument("--image_format", type=str, default="png", help="Image format to save (e.g., 'png', 'jpg').")

    return parser.parse_args()

def extract_and_save_images(df, image_col, label_col, output_folder, image_format):
    """
    Extract images from byte data and save to disk.
    """
    os.makedirs(output_folder, exist_ok=True)
    errors = 0

    for _, row in tqdm(df.iterrows(), total=len(df)):
        try:
            # Extract bytes from the nested structure
            image_data = row[image_col]  # Access the dictionary
            if "bytes" not in image_data:
                raise ValueError(f"Expected 'bytes' key in {image_data}")
            
            image_bytes = image_data["bytes"]  # Extract the bytes dictionary
            byte_sequence = bytes(image_bytes)  # Convert to a byte array
            
            # Convert the byte array to an image
            image = Image.open(io.BytesIO(byte_sequence))

            # Get label and create class-specific folder
            label = str(row[label_col])
            class_folder = os.path.join(output_folder, label)
            os.makedirs(class_folder, exist_ok=True)

            # Save image
            image_id = f"{_}.{image_format}"  # Use row index for unique naming
            image_path = os.path.join(class_folder, image_id)
            image.save(image_path, format=image_format.upper())
        except Exception as e:
            errors += 1
            print(f"Error processing row {_}: {e}")

    print(f"Extraction completed with {errors} errors.")

def main():
    args = parse_args()

    input_path = args.input_path
    output_tar_path = args.output_tar
    image_col = args.image_col
    label_col = args.label_col
    image_format = args.image_format

    # Extract the base name of the tar file (without extension) for the output folder name
    output_folder = os.path.splitext(os.path.basename(output_tar_path))[0]
    if output_tar_path.endswith(".tar.gz"):
        output_folder = os.path.splitext(output_folder)[0]

    # Read the Parquet file into a DataFrame
    if input_path.endswith(".parquet"):
        df = pd.read_parquet(input_path)
    else:
        print("Unsupported file format. Please provide a Parquet file.")
        sys.exit(1)

    # Extract and save images
    extract_and_save_images(df, image_col, label_col, output_folder, image_format)

    # Tar the output folder
    with tarfile.open(output_tar_path, "w:gz") as tar:
        tar.add(output_folder, arcname=os.path.basename(output_folder))

    full_path = Path(output_tar_path).resolve()
    print(f"Tared output folder into: {full_path}")

if __name__ == '__main__':
    main()
