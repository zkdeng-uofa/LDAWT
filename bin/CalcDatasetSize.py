import os
import argparse
import asyncio
import aiohttp
import pandas as pd
from urllib.parse import urlsplit, urlunsplit

def parse_args():
    parser = argparse.ArgumentParser(description='Calculate the total size of images from URLs in parquet files')
    parser.add_argument(
        '--directory', 
        type=str, 
        help='Input directory containing parquet files',
        default='data/parquet/10split'
    )
    parser.add_argument(
        '--url_column', 
        type=str, 
        help='url column',
        default='url'
    )
    return parser.parse_args()

async def estimate_image_size(session, url):
    try:
        # Send a HEAD request to get headers only
        async with session.head(url, allow_redirects=True) as response:
            if response.status == 200 and 'Content-Length' in response.headers:
                size_in_bytes = int(response.headers['Content-Length'])
                size_in_kb = size_in_bytes / 1024
                return size_in_kb
            # If HEAD request fails, try a GET request with range header
            async with session.get(url, headers={'Range': 'bytes=0-1023'}, allow_redirects=True) as response:
                if response.status == 200 and 'Content-Range' in response.headers:
                    content_range = response.headers['Content-Range']
                    size_in_bytes = int(content_range.split('/')[-1])
                    size_in_kb = size_in_bytes / 1024
                    return size_in_kb
            return None
    except Exception:
        return None

async def handle_url_without_extension(session, url):
    try:
        async with session.head(url, allow_redirects=True) as response:
            if response.status == 200:
                content_type = response.headers.get('Content-Type', '')
                if 'image' in content_type and 'Content-Length' in response.headers:
                    size_in_bytes = int(response.headers['Content-Length'])
                    size_in_kb = size_in_bytes / 1024
                    return size_in_kb
            # If HEAD request fails, try a GET request with range header
            async with session.get(url, headers={'Range': 'bytes=0-1023'}, allow_redirects=True) as response:
                if response.status == 200 and 'Content-Range' in response.headers:
                    content_range = response.headers['Content-Range']
                    size_in_bytes = int(content_range.split('/')[-1])
                    size_in_kb = size_in_bytes / 1024
                    return size_in_kb
            return None
    except Exception:
        return None

async def try_different_extensions(session, url, unresolved_urls):
    size_kb = await estimate_image_size(session, url)
    if size_kb is not None:
        return size_kb

    # Try different extensions if the original URL does not work
    extensions = ['.JPG', '.jpeg', '.JPEG', '.jpg', '.png', '.gif', '.pdf']
    url_parts = urlsplit(url)
    
    for ext in extensions:
        if '.' in url_parts.path:
            base, current_ext = url_parts.path.rsplit('.', 1)
        else:
            base = url_parts.path
            current_ext = ''
        new_url = urlunsplit((url_parts.scheme, url_parts.netloc, f"{base}{ext}", url_parts.query, url_parts.fragment))
        #print(f"Retrying with {new_url}...")
        
        size_kb = await estimate_image_size(session, new_url)
        if size_kb is not None:
            return size_kb

    # If no extension works, try handling URL without extension
    size_kb = await handle_url_without_extension(session, url)
    if size_kb is not None:
        return size_kb

    unresolved_urls.append(url)
    print(f"Could not determine size for {url}")
    return 0

async def get_total_size(urls):
    unresolved_urls = []
    async with aiohttp.ClientSession() as session:
        tasks = [try_different_extensions(session, url, unresolved_urls) for url in urls]
        sizes_kb = await asyncio.gather(*tasks)

    total_size_kb = sum(sizes_kb)
    total_size_mb = total_size_kb / 1024
    total_size_gb = total_size_mb / 1024
    return total_size_kb, total_size_mb, total_size_gb, len(unresolved_urls)

async def run(urls):
    total_size_kb, total_size_mb, total_size_gb, unresolved_count = await get_total_size(urls)
    print(f"Total Size: {total_size_mb:.2f} MB ({total_size_gb:.2f} GB)")
    print(f"Total unresolved URLs: {unresolved_count}")

async def main():
    args = parse_args()
    directory = args.directory

    df = pd.DataFrame()  # Initialize an empty DataFrame

    # Load all parquet files in the directory
    for parquet in os.listdir(directory):
        if parquet.endswith(".parquet"):
            parquet_path = os.path.join(directory, parquet)
            try:
                temp_df = pd.read_parquet(parquet_path)
                df = pd.concat([df, temp_df], ignore_index=True)
            except Exception as e:
                print(f"Error reading {parquet_path}: {e}")
    
    # Ensure the dataframe is not empty and has the 'url' column
    if df.empty or args.url_column not in df.columns:
        print("No valid parquet files or 'url' column missing")
        return

    df = df.reset_index(drop=True)
    await run(df[args.url_column])

if __name__ == '__main__':
    asyncio.run(main())