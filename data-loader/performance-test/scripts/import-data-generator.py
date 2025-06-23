#!/usr/bin/env python3

import argparse
import binascii
import datetime
import json
import multiprocessing
import os
import random
import string
import sys
from datetime import timedelta
from typing import Dict, List, Any

# --- Configuration ---
DEFAULT_OUTPUT_FILE = "output.csv"
RANDOM_STRING_LENGTH = 12
RANDOM_INT_MIN = -2 ** 31
RANDOM_INT_MAX = 2 ** 31 - 1
RANDOM_FLOAT_MAX = 10000.0
RANDOM_FLOAT_PRECISION = 6
RANDOM_DATE_MAX_DAYS_AGO = 3650  # Approx 10 years
RANDOM_BLOB_LENGTH = 16


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Generate random CSV data based on a ScalarDB JSON schema using multiple cores.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('schema_file', help='Path to the ScalarDB JSON schema file')
    parser.add_argument('table_name', help='Name of the table within the schema (e.g., "namespace.tablename")')

    # Create a mutually exclusive group for num_rows or target_size
    size_group = parser.add_mutually_exclusive_group(required=True)
    size_group.add_argument('-n', '--num-rows', type=int, help='Number of data rows to generate')
    size_group.add_argument('-s', '--target-size', type=str,
                            help='Target file size (e.g., "10MB", "1GB"). Supports B, KB, MB, GB suffixes.')

    parser.add_argument('-o', '--output-file', default=DEFAULT_OUTPUT_FILE,
                        help=f'Name of the output CSV file (default: {DEFAULT_OUTPUT_FILE})')

    args = parser.parse_args()

    # Basic validation
    if not os.path.isfile(args.schema_file):
        parser.error(f"Schema file not found: {args.schema_file}")

    if args.num_rows is not None and args.num_rows <= 0:
        parser.error("Number of rows must be a positive integer")

    # Parse target size if provided
    if args.target_size:
        try:
            args.target_size_bytes = parse_size(args.target_size)
        except ValueError as e:
            parser.error(str(e))

    return args


def parse_size(size_str):
    """Parse a size string like '10MB' and return the size in bytes."""
    size_str = size_str.upper().strip()

    # Define size multipliers
    multipliers = {
        'KB': 1024,
        'MB': 1024 * 1024,
        'GB': 1024 * 1024 * 1024,
    }

    # Try to match with a suffix
    for suffix, multiplier in multipliers.items():
        if size_str.endswith(suffix):
            try:
                # Extract the numeric part without the suffix
                value = float(size_str[:-len(suffix)])
                if value <= 0:
                    raise ValueError("Size value must be positive")
                return int(value * multiplier)
            except ValueError as e:
                print(e)
                raise ValueError(f"Invalid size format: {size_str}")

    # Try parsing as a plain number (bytes)
    try:
        value = float(size_str)
        if value <= 0:
            raise ValueError("Size value must be positive")
        return int(value)
    except ValueError:
        raise ValueError(f"Invalid size format: {size_str}. Use formats like '10MB', '1.5GB', etc.")


def generate_random_data(data_type: str) -> str:
    """Generate random data based on ScalarDB type."""
    # Convert type to uppercase for case-insensitive matching
    upper_type = data_type.upper()

    if upper_type in ("INT", "BIGINT"):
        return str(random.randint(RANDOM_INT_MIN, RANDOM_INT_MAX))

    elif upper_type == "TEXT":
        return ''.join(random.choices(string.ascii_letters + string.digits, k=RANDOM_STRING_LENGTH))

    elif upper_type in ("FLOAT", "DOUBLE"):
        return f"{random.uniform(0, RANDOM_FLOAT_MAX):.{RANDOM_FLOAT_PRECISION}f}"

    elif upper_type == "DATE":
        random_days_ago = random.randint(0, RANDOM_DATE_MAX_DAYS_AGO)
        return (datetime.datetime.now() - timedelta(days=random_days_ago)).strftime('%Y-%m-%d')

    elif upper_type == "TIME":
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        millisecond = random.randint(0, 999)
        return f"{hour:02d}:{minute:02d}:{second:02d}.{millisecond:03d}"

    elif upper_type == "TIMESTAMP":
        random_days_ago = random.randint(0, RANDOM_DATE_MAX_DAYS_AGO)
        random_seconds_ago = random.randint(0, 86400)  # Seconds in a day
        timestamp = datetime.datetime.now() - timedelta(days=random_days_ago, seconds=random_seconds_ago)
        return timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]  # Trim microseconds to milliseconds

    elif upper_type == "TIMESTAMPTZ":
        random_days_ago = random.randint(0, RANDOM_DATE_MAX_DAYS_AGO)
        random_seconds_ago = random.randint(0, 86400)  # Seconds in a day
        timestamp = datetime.datetime.now() - timedelta(days=random_days_ago, seconds=random_seconds_ago)
        return timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'  # Add Z for UTC

    elif upper_type == "BOOLEAN":
        return random.choice(["true", "false"])

    elif upper_type == "BLOB":
        random_bytes = os.urandom(RANDOM_BLOB_LENGTH)
        return binascii.hexlify(random_bytes).decode('ascii')

    else:
        return f"UNSUPPORTED_TYPE({data_type})"


def generate_row(column_types: List[str]) -> str:
    """Generate a single CSV row based on the schema."""
    row_values = [generate_random_data(col_type) for col_type in column_types]
    return ','.join(row_values)


def estimate_row_size(column_types: List[str], sample_size: int = 10) -> int:
    """Estimate the average size of a row in bytes by generating sample rows."""
    total_size = 0
    for _ in range(sample_size):
        row = generate_row(column_types)
        total_size += len(row.encode('utf-8')) + 1  # +1 for newline character
    return total_size // sample_size


def worker_task(args):
    """Worker function for multiprocessing."""
    _, column_types = args
    return generate_row(column_types)


def format_size(size_bytes):
    """Format size in bytes to a human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes}B"
    elif size_bytes < pow(1024, 2):
        return f"{round(size_bytes / 1024, 2)}KB"
    elif size_bytes < pow(1024, 3):
        return f"{round(size_bytes / (pow(1024, 2)), 2)}MB"
    elif size_bytes < pow(1024, 4):
        return f"{round(size_bytes / (pow(1024, 3)), 2)}GB"


def main():
    args = parse_arguments()

    try:
        # Read schema file
        with open(args.schema_file, 'r') as f:
            schema = json.load(f)

        # Validate table existence in schema
        if args.table_name not in schema:
            print(f"Error: Table '{args.table_name}' not found in schema file '{args.schema_file}'")
            sys.exit(1)

        # Extract column information
        columns = schema[args.table_name].get('columns', {})
        if not columns:
            print(f"Error: No columns found for table '{args.table_name}' in schema file")
            sys.exit(1)

        # Get column names in order (keys of the columns dictionary)
        column_names = list(columns.keys())
        column_header = ','.join(column_names)

        # Get corresponding column types
        column_types = [columns[name] for name in column_names]

        # Calculate the number of rows if target size is specified
        if hasattr(args, 'target_size_bytes'):
            # Estimate size with a sample
            avg_row_size = estimate_row_size(column_types)
            header_size = len(column_header.encode('utf-8')) + 1  # +1 for newline
            num_rows = max(1, (args.target_size_bytes - header_size) // avg_row_size)
            print(f"Targeting ~{args.target_size} file size. Estimated row size: {avg_row_size} bytes")
            print(f"Will generate {num_rows} rows to approximate the target size")
        else:
            num_rows = args.num_rows

        # Set up multiprocessing
        num_cores = multiprocessing.cpu_count()
        print(f"Generating {num_rows} rows for table '{args.table_name}' using {num_cores} cores...")
        print(f"Schema File: {args.schema_file}")
        print(f"Output File: {args.output_file}")

        # Write header to output file
        with open(args.output_file, 'w') as f:
            f.write(column_header + '\n')

        # Generate data using multiprocessing
        with multiprocessing.Pool(processes=num_cores) as pool:
            # Process chunks of data to avoid memory issues with very large datasets
            chunk_size = min(1000, max(1, num_rows // (num_cores * 10)))

            with open(args.output_file, 'a') as f:
                # Process in chunks to avoid storing all tasks in memory
                for chunk_start in range(0, num_rows, chunk_size):
                    # Calculate the actual size of this chunk (might be smaller for the last chunk)
                    current_chunk_size = min(chunk_size, num_rows - chunk_start)

                    # Use imap_unordered with a generator expression and appropriate chunksize for better memory efficiency
                    # imap_unordered processes items as they become available and doesn't wait for order preservation
                    # This is faster and uses less memory than regular imap
                    # The chunksize parameter controls how many tasks each worker gets at once
                    worker_chunksize = max(1, min(100, current_chunk_size // num_cores))
                    results = pool.imap_unordered(
                        worker_task,
                        ((i, column_types) for i in range(chunk_start, chunk_start + current_chunk_size)),
                        chunksize=worker_chunksize
                    )

                    # Write results and explicitly flush to ensure data is written to disk
                    for row in results:
                        f.write(row + '\n')

                    # Flush after each chunk to ensure data is written to disk
                    f.flush()

                    # Report progress for large datasets
                    if num_rows > 10000 and (chunk_start + current_chunk_size) % (num_rows // 10) < chunk_size:
                        progress = (chunk_start + current_chunk_size) / num_rows * 100
                        print(f"Progress: {progress:.1f}% ({chunk_start + current_chunk_size}/{num_rows} rows)",
                              flush=True)

        # Report final file size
        final_size = os.path.getsize(args.output_file)
        size_str = format_size(final_size)
        print(
            f"Successfully generated {args.output_file} with {num_rows} data rows ({size_str}) for table '{args.table_name}'.")

    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in schema file '{args.schema_file}'")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
