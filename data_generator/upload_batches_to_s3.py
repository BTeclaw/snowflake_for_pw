#!/usr/bin/env python3
"""
Script to upload CDC batch files to S3.
Usage: python upload_batches_to_s3.py --start-batch 0001 --end-batch 0005 --s3-bucket my-bucket --s3-prefix cdc-batches/transactions/ [--output-dir ./output]
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Tuple

import boto3
from botocore.exceptions import ClientError, NoCredentialsError


def find_batch_files(output_dir: str, batch_num: str) -> List[Path]:
    """
    Find all batch files matching the batch number pattern.
    
    Args:
        output_dir: Directory to search in
        batch_num: Batch number (4 digits, e.g., "0001")
    
    Returns:
        List of matching file paths
    """
    output_path = Path(output_dir)
    pattern = f"batch_{batch_num}_*.csv"
    return sorted(output_path.glob(pattern))


def upload_file_to_s3(
    local_file: Path, s3_bucket: str, s3_prefix: str, s3_client
) -> Tuple[bool, str]:
    """
    Upload a file to S3.
    
    Args:
        local_file: Local file path
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix/path
        s3_client: Boto3 S3 client
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    filename = local_file.name
    s3_key = f"{s3_prefix}{filename}" if s3_prefix else filename
    
    try:
        s3_client.upload_file(str(local_file), s3_bucket, s3_key)
        return True, f"s3://{s3_bucket}/{s3_key}"
    except ClientError as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)


def validate_batch_number(batch_num: str) -> bool:
    """Validate that batch number is 4 digits."""
    return batch_num.isdigit() and len(batch_num) == 4


def main():
    parser = argparse.ArgumentParser(
        description="Upload CDC batch files to S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python upload_batches_to_s3.py --start-batch 0001 --end-batch 0005 --s3-bucket my-bucket --s3-prefix cdc-batches/transactions/
  python upload_batches_to_s3.py --start-batch 0001 --end-batch 0005 --s3-bucket my-bucket --s3-prefix cdc-batches/transactions/ --output-dir ./output
        """,
    )
    
    parser.add_argument(
        "--start-batch",
        type=str,
        required=True,
        help="Starting batch number (e.g., 0001)",
        dest="start_batch",
    )
    parser.add_argument(
        "--end-batch",
        type=str,
        required=True,
        help="Ending batch number (e.g., 0005)",
        dest="end_batch",
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        required=True,
        help="S3 bucket name (e.g., my-bucket)",
        dest="s3_bucket",
    )
    parser.add_argument(
        "--s3-prefix",
        type=str,
        required=True,
        help="S3 prefix/path (e.g., cdc-batches/transactions/)",
        dest="s3_prefix",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./output",
        help="Directory containing batch files (default: ./output)",
        dest="output_dir",
    )
    
    args = parser.parse_args()
    
    # Validate batch numbers
    if not validate_batch_number(args.start_batch):
        print(f"Error: Start batch '{args.start_batch}' must be 4 digits (e.g., 0001)", file=sys.stderr)
        sys.exit(1)
    
    if not validate_batch_number(args.end_batch):
        print(f"Error: End batch '{args.end_batch}' must be 4 digits (e.g., 0005)", file=sys.stderr)
        sys.exit(1)
    
    # Convert to integers for comparison
    start_num = int(args.start_batch)
    end_num = int(args.end_batch)
    
    if start_num > end_num:
        print(
            f"Error: Start batch ({args.start_batch}) must be less than or equal to end batch ({args.end_batch})",
            file=sys.stderr,
        )
        sys.exit(1)
    
    # Validate output directory
    output_path = Path(args.output_dir)
    if not output_path.exists():
        print(f"Error: Output directory '{args.output_dir}' does not exist", file=sys.stderr)
        sys.exit(1)
    
    if not output_path.is_dir():
        print(f"Error: '{args.output_dir}' is not a directory", file=sys.stderr)
        sys.exit(1)
    
    # Normalize S3 prefix
    s3_prefix = args.s3_prefix.rstrip("/")
    if s3_prefix:
        s3_prefix = f"{s3_prefix}/"
    
    # Initialize S3 client
    try:
        s3_client = boto3.client("s3")
    except NoCredentialsError:
        print("Error: AWS credentials not found. Please configure your AWS credentials.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to initialize S3 client: {e}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Uploading batches {args.start_batch} to {args.end_batch} to s3://{args.s3_bucket}/{s3_prefix}")
    print()
    
    # Counters
    success_count = 0
    failed_count = 0
    not_found_count = 0
    
    # Process each batch in range
    for batch_num_int in range(start_num, end_num + 1):
        batch_num = f"{batch_num_int:04d}"
        batch_files = find_batch_files(str(output_path), batch_num)
        
        if not batch_files:
            print(f"Warning: No batch file found for batch {batch_num}")
            not_found_count += 1
            continue
        
        # Upload each matching file
        for batch_file in batch_files:
            print(f"Uploading {batch_file.name}... ", end="", flush=True)
            success, message = upload_file_to_s3(batch_file, args.s3_bucket, s3_prefix, s3_client)
            
            if success:
                print(f"✓")
                success_count += 1
            else:
                print(f"✗ Failed: {message}")
                failed_count += 1
    
    # Print summary
    print()
    print("Upload Summary:")
    print(f"  Successful: {success_count}")
    if failed_count > 0:
        print(f"  Failed: {failed_count}")
    if not_found_count > 0:
        print(f"  Not Found: {not_found_count}")
    
    # Exit with appropriate code
    if failed_count == 0 and not_found_count == 0:
        print("All batches uploaded successfully!")
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()

