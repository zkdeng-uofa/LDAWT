#!/usr/bin/env python3

import pandas as pd
import os
import sys
import aiohttp
import asyncio
import tarfile
import shutil
from pathlib import Path
from tqdm.asyncio import tqdm
import argparse
import mimetypes

def parse_args():
    """
    Parse user inputs from arguments using argparse.
    """
    parser = argparse.ArgumentParser(description="Download images asynchronously and tar the output folder.")
    
    parser.add_argument("--input_path", type=str, required=True, help="Path to the input CSV or Parquet file.")
    parser.add_argument("--output_tar", type=str, required=True, help="Path to the output tar file (e.g., 'images.tar.gz').")
    parser.add_argument("--url_name", type=str, default="photo_url", help="Column name containing the image URLs.")
    parser.add_argument("--class_name", type=str, default="taxon_name", help="Column name containing the class names.")

    return parser.parse_args()

async def download_image_with_extensions(session, semaphore, row, output_folder, url_col, class_col):
    """Download an image asynchronously with retries for different file extensions"""
    # Extensions to try if the original extension fails
    fallback_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.pdf']
    async with semaphore:
        key, image_url = row.name, row[url_col]
        class_name = row[class_col].replace("'", "").replace(" ", "_")
        
        # Get the base URL and the original extension
        base_url, original_ext = os.path.splitext(image_url)
        
        # Check if the URL has an extension
        if not original_ext:
            # If no extension, download the content and determine the MIME type
            try:
                async with session.get(image_url) as response:
                    if response.status == 200:
                        content = await response.read()
                        mime_type = response.headers.get('Content-Type')
                        ext = mimetypes.guess_extension(mime_type)
                        if ext:
                            file_name = f"{base_url.split('/')[-1]}{ext}"
                            file_path = os.path.join(output_folder, class_name, file_name)
                            os.makedirs(os.path.dirname(file_path), exist_ok=True)
                            with open(file_path, 'wb') as f:
                                f.write(content)
                            return key, file_name, class_name, None
                        else:
                            print(f"Failed to determine extension for MIME type: {mime_type}")
                    else:
                        print(f"Failed to download {image_url}: HTTP {response.status}")
            except Exception as err:
                print(f"Error with URL {image_url}: {err}")
                return key, None, class_name, str(err)
        else:
            # If URL has an extension, proceed with the original functionality
            file_name = f"{base_url.split('/')[-2]}{original_ext}"
            file_path = os.path.join(output_folder, class_name, file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # Try downloading the image with the original extension first
            try:
                async with session.get(image_url) as response:
                    if response.status == 200:
                        content = await response.read()
                        with open(file_path, 'wb') as f:
                            f.write(content)
                        return key, file_name, class_name, None
                    else:
                        print(f"Failed to download {file_name}: HTTP {response.status}")
            except Exception as err:
                print(f"Error with original URL {image_url}: {err}")

            # If original extension fails, try the fallback extensions
            for ext in fallback_extensions:
                if ext == original_ext:  # Skip if the fallback extension is the same as the original
                    continue
                new_url = f"{base_url}{ext}"
                file_name = f"{base_url.split('/')[-2]}{ext}"
                file_path = os.path.join(output_folder, class_name, file_name)

                try:
                    async with session.get(new_url) as response:
                        if response.status == 200:
                            content = await response.read()
                            with open(file_path, 'wb') as f:
                                f.write(content)
                            return key, file_name, class_name, None
                        else:
                            print(f"Failed to download {file_name}: HTTP {response.status}")
                except Exception as err:
                    print(f"Error with {new_url}: {err}")
                    continue  # Try the next extension

        # If all extensions fail
        return key, None, class_name, "All extensions failed."

async def main():
    args = parse_args()

    input_path = args.input_path
    output_tar_path = args.output_tar
    url_col = args.url_name
    class_col = args.class_name
    concurrent_downloads = 1000  # Fixed number of concurrent downloads

    # Extract the base name of the tar file (without extension) for the output folder name
    output_folder = os.path.splitext(os.path.basename(output_tar_path))[0]
    if output_tar_path.endswith(".tar.gz"):
        output_folder = os.path.splitext(output_folder)[0]

    # Read the specified file into a dataframe
    if input_path.endswith(".parquet"):
        df = pd.read_parquet(input_path)
    elif input_path.endswith(".csv"):
        df = pd.read_csv(input_path)
    else:
        print("Unsupported file format. Please provide a CSV or Parquet file.")
        sys.exit(1)

    semaphore = asyncio.Semaphore(concurrent_downloads)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for _, row in df.iterrows():
            task = download_image_with_extensions(session, semaphore, row, output_folder, url_col, class_col)
            tasks.append(asyncio.create_task(task))

        errors = 0
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            key, file_name, class_name, error = await future
            if error:
                errors += 1
                # Uncomment the following line to print errors
                # print(f"Error downloading image {file_name}: {error}")
        print(f"Completed with {errors} errors.")

    # Tar the output folder
    with tarfile.open(output_tar_path, "w:gz") as tar:
        tar.add(output_folder, arcname=os.path.basename(output_folder))

    full_path = Path(output_tar_path).resolve()
    print(f"Tared output folder into: {full_path}")


if __name__ == '__main__':
    asyncio.run(main())