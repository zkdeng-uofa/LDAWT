#!/usr/bin/env python3

import pandas as pd
import os
import sys
import aiohttp
import asyncio
import tarfile
import time
import mimetypes
import argparse
import json
from pathlib import Path
from tqdm.asyncio import tqdm
import signal
import shutil

class TokenBucket:
    """
    A token bucket implementation for rate limiting.
    """
    def __init__(self, rate, capacity):
        """
        Initialize the token bucket.
        :param rate: Number of tokens added per second.
        :param capacity: Maximum number of tokens the bucket can hold.
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_refill = time.time()

    async def acquire(self):
        """
        Wait until a token is available and consume it.
        """
        while True:
            self._refill()
            if self.tokens >= 1:
                self.tokens -= 1
                return
            await asyncio.sleep(0.01)  # Wait a short time before checking again

    def _refill(self):
        """
        Refill the bucket with tokens based on the elapsed time.
        """
        now = time.time()
        elapsed = now - self.last_refill
        new_tokens = elapsed * self.rate
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_refill = now

    def adjust_rate(self, new_rate):
        new_rate = max(1, new_rate)  # Minimum 1 request per second
        if abs(new_rate - self.rate) > 0.1:  # Only adjust if significant change
            print(f"[TokenBucket] Adjusting rate: {self.rate:.2f} -> {new_rate:.2f} tokens/sec")
            self.rate = new_rate

    def get_rate(self):
        return self.rate

async def gradually_increase_rate(token_bucket, max_rate, interval=10):
    """
    Gradually increase the rate limit over time to recover from rate limiting.
    """
    while True:
        await asyncio.sleep(interval)
        current_rate = token_bucket.get_rate()
        if current_rate < max_rate:
            new_rate = min(max_rate, current_rate * 1.2)  # Increase by 10%
            token_bucket.adjust_rate(new_rate)

def parse_args():
    """
    Parse user inputs from arguments using argparse.
    """
    parser = argparse.ArgumentParser(description="Download images asynchronously, track bandwidth, and tar the output folder.")
    
    parser.add_argument("--input", type=str, required=True, help="Path to the input CSV or Parquet file.")
    parser.add_argument("--output", type=str, required=True, help="Path to the output tar file (e.g., 'images.tar.gz').")
    parser.add_argument("--url", type=str, default="photo_url", help="Column name containing the image URLs.")
    parser.add_argument("--label", type=str, default="taxon_name", help="Column name containing the class names.")
    parser.add_argument("--concurrent_downloads", type=int, default=1000, help="Number of concurrent downloads (default: 1000).")
    parser.add_argument("--timeout", type=int, default=30, help="Download timeout in seconds (default: 30).")
    parser.add_argument("--max_file_size", type=int, default=500*1024*1024, help="Maximum file size in bytes (default: 500MB).")
    parser.add_argument("--rate_limit", type=float, default=100.0, help="Initial rate limit in requests per second (default: 100.0).")
    parser.add_argument("--rate_capacity", type=int, default=200, help="Token bucket capacity (default: 200).")
    parser.add_argument("--enable_rate_limiting", action="store_true", help="Enable token bucket rate limiting.")

    return parser.parse_args()

# Global flag for graceful shutdown
shutdown_flag = False

def signal_handler(sig, frame):
    global shutdown_flag
    print("\nReceived interrupt signal. Shutting down gracefully...")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def save_and_track(content, file_path, max_file_size, total_bytes):
    """Helper function to write content to file and track size"""
    try:
        # Check file size before saving
        if len(content) > max_file_size:
            return False, "File too large"
            
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'wb') as f:
            f.write(content)
        file_size = os.path.getsize(file_path)  # Get actual stored size
        total_bytes.append(file_size)  # Track real disk size
        return True, None
    except Exception as e:
        return False, str(e)

async def download_image_no_extensions(
        key,
        image_url, 
        class_name, 
        session, 
        timeout, 
        base_url, 
        output_folder,
        max_file_size,
        total_bytes,
        token_bucket=None
    ):
    try:
        # Wait for token if rate limiting is enabled
        if token_bucket:
            await token_bucket.acquire()
            
        async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status == 200:
                content = await response.read()
                mime_type = response.headers.get('Content-Type')
                ext = mimetypes.guess_extension(mime_type) or ".jpg"
                file_name = f"{base_url.split('/')[-2]}{ext}"
                file_path = os.path.join(output_folder, class_name, file_name)
                success, error = save_and_track(content, file_path, max_file_size, total_bytes)
                
                if success:
                    return key, file_name, class_name, None, response.status
                else:
                    return key, None, class_name, error, response.status
            else:
                return key, None, class_name, f"HTTP {response.status}", response.status
    except asyncio.TimeoutError:
        return key, None, class_name, "Timeout", 0
    except Exception as err:
        return key, None, class_name, str(err), 0

async def download_image_with_extensions(
        key,
        image_url,
        original_ext,
        fallback_extensions,
        class_name,
        session,
        timeout,
        base_url,
        output_folder,
        max_file_size,
        total_bytes,
        token_bucket=None
    ):
    file_name = f"{base_url.split('/')[-2]}{original_ext}"
    file_path = os.path.join(output_folder, class_name, file_name)
    try:
        # Wait for token if rate limiting is enabled
        if token_bucket:
            await token_bucket.acquire()
            
        async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status == 200:
                content = await response.read()
                success, error = save_and_track(content, file_path, max_file_size, total_bytes)
                if success:
                    return key, file_name, class_name, None, response.status
                else:
                    return key, None, class_name, error, response.status
            else:
                return key, None, class_name, f"HTTP {response.status}", response.status
    except asyncio.TimeoutError:
        return key, None, class_name, "Timeout", 0
    except Exception as err:
        pass
    
    # Try fallback extensions only if original failed due to connection issues
    for ext in fallback_extensions:
        if ext == original_ext or shutdown_flag:
            continue
        new_url = f"{base_url}{ext}"
        file_name = f"{base_url.split('/')[-2]}{ext}"
        file_path = os.path.join(output_folder, class_name, file_name)
        
        try:
            # Wait for token if rate limiting is enabled
            if token_bucket:
                await token_bucket.acquire()
                
            async with session.get(new_url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                if response.status == 200:
                    content = await response.read()
                    success, error = save_and_track(content, file_path, max_file_size, total_bytes)
                    if success:
                        return key, file_name, class_name, None, response.status
                    else:
                        return key, None, class_name, error, response.status
                else:
                    return key, None, class_name, f"HTTP {response.status}", response.status
        except asyncio.TimeoutError:
            continue
        except Exception:
            continue  # Try the next extension

    return key, None, class_name, "All download attempts failed", 0

async def download_image(
        session, 
        semaphore, 
        row, 
        output_folder, 
        url_col, 
        class_col, 
        total_bytes, 
        timeout, 
        max_file_size,
        token_bucket=None
    ):
    """Download an image asynchronously with retries for different file extensions, tracking actual stored size."""
    
    global shutdown_flag
    if shutdown_flag:
        return row.name, None, None, "Shutdown requested"
    
    fallback_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.pdf']

    async with semaphore:
        key, image_url = row.name, row[url_col]
        
        # Validate URL
        if pd.isna(image_url) or not str(image_url).strip():
            return key, None, None, "Empty or invalid URL"
        
        # Clean class name
        class_name = str(row[class_col]).replace("'", "").replace(" ", "_").replace("/", "_")
        base_url, original_ext = os.path.splitext(str(image_url))
        
        # If no extension, determine it dynamically
        if not original_ext:
            return await download_image_no_extensions(
                key,
                image_url,
                class_name,
                session,
                timeout,
                base_url,
                output_folder,
                max_file_size,
                total_bytes,
                token_bucket
            )

        else:
            # Try downloading with original extension
            return await download_image_with_extensions(
                key,
                image_url,
                original_ext,
                fallback_extensions,
                class_name,
                session,
                timeout,
                base_url,
                output_folder,
                max_file_size,
                total_bytes,
                token_bucket
            )

def validate_and_clean(
        input, 
        output_folder, 
        url_col, 
        class_col
        ):
    if not os.path.exists(input):
        print(f"Error: Input file {input} not found")
        sys.exit(1)
    
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    
    try:
        if input.endswith(".parquet"):
            df = pd.read_parquet(input)
        elif input.endswith(".csv"):
            df = pd.read_csv(input)
        else:
            print("Unsupported file format. Please provide a CSV or Parquet file.")
            sys.exit(1)
    except Exception as e:
        print(f"Error reading input file: {e}")
        sys.exit(1)
    
    if url_col not in df.columns:
        print(f"Error: Column '{url_col}' not found in input file")
        print(f"Available columns: {list(df.columns)}")
        sys.exit(1)
    
    if class_col not in df.columns:
        print(f"Error: Column '{class_col}' not found in input file")
        print(f"Available columns: {list(df.columns)}")
        sys.exit(1)

    initial_count = len(df)
    df = df.dropna(subset=[url_col])
    filtered_count = len(df)
    
    if filtered_count < initial_count:
        print(f"Filtered out {initial_count - filtered_count} rows with missing URLs")
    
    if filtered_count == 0:
        print("No valid URLs found in input file")
        sys.exit(1)
    
    return df, filtered_count

def create_json_overview(
        input, 
        output_path, 
        url_col, 
        class_col, 
        concurrent_downloads, 
        timeout, 
        max_file_size,
        error_details,
        successful_downloads,
        total_bytes,
        total_time,
        total_errors,
        total_downloaded,
        filtered_count,
        token_bucket=None,
        enable_rate_limiting=False
    ):

    overview_data = {
        "script_inputs": {
            "input_file": input,
            "output_file": output_path,
            "url_column": url_col,
            "label_column": class_col,
            "concurrent_downloads": concurrent_downloads,
            "timeout": timeout,
            "max_file_size": max_file_size,
            "rate_limiting_enabled": enable_rate_limiting,
            "final_rate_limit": token_bucket.get_rate() if token_bucket else None
        },
        "download_summary": {
            "total_records_processed": filtered_count,
            "successful_downloads": successful_downloads,
            "failed_downloads": total_errors,
            "success_rate_percent": round((successful_downloads/(successful_downloads+total_errors)*100), 2) if (successful_downloads + total_errors) > 0 else 0,
            "total_data_mb": round(total_downloaded / 1e6, 2) if total_downloaded > 0 else 0,
            "total_time_seconds": round(total_time, 2),
            "average_speed_mbps": round((total_downloaded / total_time) / 1e6, 2) if total_time > 0 and total_downloaded > 0 else 0
        },
        "error_breakdown": {},
        "execution_info": {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "shutdown_requested": shutdown_flag
        }
    }

    if total_errors > 0:
        error_counts = {}
        for error_info in error_details:
            error_type = error_info['error']
            if error_type in error_counts:
                error_counts[error_type] += 1
            else:
                error_counts[error_type] = 1
        overview_data["error_breakdown"] = error_counts

    json_filename = os.path.splitext(output_path)[0] + "_overview.json"
    try:
        with open(json_filename, 'w') as json_file:
            json.dump(overview_data, json_file, indent=2)
        print(f"Created overview file: {json_filename}")
    except Exception as e:
        print(f"Warning: Could not create overview JSON file: {e}")

    return None

def create_tar_archive(
        output_path,
        output_folder,
        successful_downloads,
        total_errors,
    ):
    if successful_downloads > 0 and not shutdown_flag and os.path.exists(output_folder):
        try:
            print(f"\nCreating tar archive: {output_path}")
            with tarfile.open(output_path, "w") as tar:
                tar.add(output_folder, arcname=os.path.basename(output_folder))
                
            full_path = Path(output_path).resolve()
            tar_size = os.path.getsize(output_path)
            print(f"Created tar archive: {full_path} ({tar_size / 1e6:.2f} MB)")
            
            # Clean up the temporary folder
            #shutil.rmtree(output_folder)
            
        except Exception as e:
            print(f"Error creating tar archive: {e}")
            sys.exit(1)
    else:
        if shutdown_flag:
            print("Shutdown was requested, skipping tar creation")
        elif successful_downloads == 0:
            print("No successful downloads, skipping tar creation")
        sys.exit(1 if total_errors > 0 else 0)
    
    return None

async def main():
    global shutdown_flag
    
    args = parse_args()

    input = args.input
    output_path = args.output
    url_col = args.url
    class_col = args.label
    concurrent_downloads = args.concurrent_downloads
    timeout = args.timeout
    max_file_size = args.max_file_size
    rate_limit = args.rate_limit
    rate_capacity = args.rate_capacity
    enable_rate_limiting = args.enable_rate_limiting
    output_folder = os.path.splitext(os.path.basename(output_path))[0]

    # Validate inputs
    df, filtered_count = validate_and_clean(input, output_folder, url_col, class_col)
    
    # Initialize token bucket if rate limiting is enabled
    token_bucket = None
    if enable_rate_limiting:
        token_bucket = TokenBucket(rate=rate_limit, capacity=rate_capacity)
        print(f"Processing {filtered_count} images with {concurrent_downloads} concurrent downloads and {rate_limit:.1f} req/s rate limit")
    else:
        print(f"Processing {filtered_count} images with {concurrent_downloads} concurrent downloads (no rate limiting)")

    semaphore = asyncio.Semaphore(concurrent_downloads)
    total_bytes = []  # List to track total bytes downloaded

    start_time = time.monotonic()  # Start timer

    # Configure session with connection pooling and limits
    connector = aiohttp.TCPConnector(
        limit=concurrent_downloads * 2,  # Total connection pool size
        limit_per_host=20,  # Max connections per host
        ttl_dns_cache=300,  # DNS cache TTL
        use_dns_cache=True,
    )

    # Start rate recovery task if rate limiting is enabled
    recovery_task = None
    if enable_rate_limiting and token_bucket:
        recovery_task = asyncio.create_task(
            gradually_increase_rate(token_bucket, rate_limit, interval=5)
        )
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=timeout*2),  # Overall session timeout
        headers={'User-Agent': 'LDAWT-ImageDownloader/1.0'}
    ) as session:
        tasks = [
            download_image(
                session, semaphore, row, output_folder, url_col, class_col, 
                total_bytes, timeout, max_file_size, token_bucket
            ) 
            for _, row in df.iterrows()
        ]
        
        error_details = []  # List to track detailed error information
        successful_downloads = 0
        
        try:
            for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Downloading"):
                if shutdown_flag:
                    print("Shutdown requested, cancelling remaining downloads...")
                    break
                    
                key, file_name, class_name, error, status_code = await future
                if error:
                    error_details.append({
                        'key': key,
                        'file_name': file_name,
                        'class': class_name,
                        'error': error,
                        'status_code': status_code
                    })
                    
                    # Adaptive rate control based on error type
                    if token_bucket and enable_rate_limiting:
                        if status_code == 429 or "429" in str(error):  # Rate limited
                            new_rate = token_bucket.get_rate() * 0.5  # Reduce rate by 50%
                            token_bucket.adjust_rate(new_rate)
                        elif status_code in [503, 502, 504]:  # Server errors
                            new_rate = token_bucket.get_rate() * 0.75  # Reduce rate by 25%
                            token_bucket.adjust_rate(new_rate)
                    
                    # Print error in real-time
                    print(f"\n[ERROR] Key: {key}, Error: {error}, Status Code: {status_code}")
                else:
                    successful_downloads += 1
        except KeyboardInterrupt:
            print("Download interrupted by user")
            shutdown_flag = True
    
    # Cancel recovery task if it was started
    if recovery_task:
        recovery_task.cancel()

    total_time = time.monotonic() - start_time  # Total time taken
    total_downloaded = sum(total_bytes)  # Total bytes downloaded
    total_errors = len(error_details)

    print(f"\nDownload Summary:")
    print(f"  - Successful downloads: {successful_downloads}")
    print(f"  - Failed downloads: {total_errors}")
    if successful_downloads + total_errors > 0:
        print(f"  - Success rate: {(successful_downloads/(successful_downloads+total_errors)*100):.1f}%")
    
    # Display detailed error breakdown
    if total_errors > 0:
        print(f"\nError Breakdown:")
        error_counts = {}
        for error_info in error_details:
            error_type = error_info['error']
            if error_type in error_counts:
                error_counts[error_type] += 1
            else:
                error_counts[error_type] = 1
        
        for error_type, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {error_type}: {count} occurrences")

    create_json_overview(
        input, 
        output_path, 
        url_col, 
        class_col, 
        concurrent_downloads, 
        timeout, 
        max_file_size,
        error_details,
        successful_downloads,
        total_bytes,
        total_time,
        total_errors,
        total_downloaded,
        filtered_count,
        token_bucket,
        enable_rate_limiting
    )

    if total_time > 0 and total_downloaded > 0:
        avg_speed = total_downloaded / total_time  # Bytes per second
        print(f"  - Total Data: {total_downloaded / 1e6:.2f} MB")
        print(f"  - Time Taken: {total_time:.2f} sec")
        print(f"  - Avg Speed: {avg_speed / 1e6:.2f} MB/s")
    else:
        print("  - No successful downloads to compute bandwidth statistics.")

    # Only create tar if we have successful downloads and no shutdown was requested
    create_tar_archive(
        output_path,
        output_folder,
        successful_downloads,
        total_errors
    )

if __name__ == '__main__':
    asyncio.run(main())
