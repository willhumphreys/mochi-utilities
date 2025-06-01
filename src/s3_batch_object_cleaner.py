#!/usr/bin/env python3

import boto3
import logging
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError
import argparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Hard-coded configuration values
#SYMBOL = "AAPL"  # Symbol to search for and delete
SYMBOL = "DPZ"  # Symbol to search for and delete
PROFILE_NAME = "mochi-admin"  # AWS profile to use
MAX_CONCURRENCY = 8  # Maximum concurrent bucket operations
MAX_KEYS = 1000  # Maximum keys per API call
DRY_RUN = False  # Set to True for simulation, False for actual deletion

# List of buckets to process
BUCKETS = [
    "mochi-prod-aggregated-trades",
    "mochi-prod-athena-query-staging",
    "mochi-prod-backtest-traders",
    "mochi-prod-backtest-trades",
    "mochi-prod-final-trader-ranking",
    "mochi-prod-prepared-historical-data",
    "mochi-prod-summary-graphs",
    "mochi-prod-ticker-meta",
    "mochi-prod-trade-extracts",
    "mochi-prod-trade-performance-graphs",
    "mochi-prod-raw-historical-data"
]


def delete_objects_from_bucket(bucket_name, symbol, profile_name, max_keys, dry_run, all_symbols=False):
    """
    Delete all objects containing the specified symbol from a bucket, or all objects if all_symbols is True
    """
    # Create a boto3 session with the given profile
    session = boto3.Session(profile_name=profile_name)
    s3 = session.client('s3')

    logger.info(f"Processing bucket: {bucket_name}")

    # Use pagination to handle large numbers of objects
    paginator = s3.get_paginator('list_objects_v2')

    total_deleted = 0
    total_found = 0

    try:
        # Iterate through pages of objects
        for page in paginator.paginate(Bucket=bucket_name, MaxKeys=max_keys):
            if 'Contents' not in page:
                logger.info(f"No objects found in bucket {bucket_name}")
                continue

            # Filter for objects containing the symbol or all objects
            objects_to_delete = []
            for obj in page['Contents']:
                key = obj['Key']
                if all_symbols or (symbol and symbol in key):
                    objects_to_delete.append({'Key': key})
                    total_found += 1
                    logger.debug(f"Found matching object: {key}")

            # If no matching objects in this page, continue to next page
            if not objects_to_delete:
                continue

            logger.info(f"Found {len(objects_to_delete)} objects "
                        f"{'in total' if all_symbols else f'containing {symbol}'} in {bucket_name}")

            # Delete the objects if not in dry run mode
            if not dry_run and objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i + 1000]
                    response = s3.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': batch}
                    )
                    deleted_count = len(response.get('Deleted', []))
                    total_deleted += deleted_count

                    if 'Errors' in response:
                        for error in response['Errors']:
                            logger.error(
                                f"Error deleting {error['Key']}: {error['Code']} - {error['Message']}"
                            )

                logger.info(f"Deleted {len(objects_to_delete)} objects from {bucket_name}")
            elif dry_run:
                logger.info(f"[DRY RUN] Would delete {len(objects_to_delete)} objects from {bucket_name}")

    except ClientError as e:
        logger.error(f"Error processing bucket {bucket_name}: {e}")
        return 0, 0

    return total_found, total_deleted

def process_all_buckets(symbol, profile_name, max_keys, dry_run, all_symbols=False):
    """
    Process all buckets in parallel using ThreadPoolExecutor
    """
    total_found = 0
    total_deleted = 0

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
        futures = {
            executor.submit(
                delete_objects_from_bucket,
                bucket,
                symbol,
                profile_name,
                max_keys,
                dry_run,
                all_symbols
            ): bucket for bucket in BUCKETS
        }

        for future in futures:
            bucket = futures[future]
            try:
                found, deleted = future.result()
                total_found += found
                total_deleted += deleted
            except Exception as e:
                logger.error(f"Error processing bucket {bucket}: {e}")

    return total_found, total_deleted

def main():
    parser = argparse.ArgumentParser(description="S3 Batch Object Cleaner")
    parser.add_argument('--all-symbols', action='store_true', help='Delete ALL objects in the buckets')
    args = parser.parse_args()

    deleting_everything = args.all_symbols
    symbol = None if deleting_everything else SYMBOL

    if not DRY_RUN:
        if deleting_everything:
            print("WARNING: About to DELETE ALL files in the selected buckets.")
        else:
            print(f"WARNING: About to DELETE files containing symbol '{SYMBOL}'.")
        confirmation = input("Are you sure you want to proceed? (y/N): ")
        if confirmation.lower() != 'y':
            print("Operation cancelled.")
            return

    logger.info("Starting deletion of objects " +
                ("(ALL SYMBOLS)" if deleting_everything else f"containing '{SYMBOL}'"))
    if DRY_RUN:
        logger.info("DRY RUN MODE - No objects will be deleted")
    else:
        logger.info("DELETION MODE - Objects will be permanently deleted")

    logger.info(f"Using AWS profile: {PROFILE_NAME}")
    logger.info(f"Max concurrency: {MAX_CONCURRENCY}")
    logger.info(f"Max keys per request: {MAX_KEYS}")

    total_found, total_deleted = process_all_buckets(symbol, PROFILE_NAME, MAX_KEYS, DRY_RUN, deleting_everything)

    if DRY_RUN:
        logger.info(f"DRY RUN SUMMARY: Found {total_found} objects " +
                    ("(ALL SYMBOLS)" if deleting_everything else f"containing '{SYMBOL}'") + " across all buckets")
    else:
        logger.info(
            f"SUMMARY: Found {total_found} objects and deleted {total_deleted} objects " +
            ("(ALL SYMBOLS)" if deleting_everything else f"containing '{SYMBOL}'") + " across all buckets"
        )

if __name__ == "__main__":
    main()