import boto3
import logging
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize the S3 client
# Credentials will be automatically retrieved from environment variables,
# AWS configuration files, or IAM role if running on EC2/Lambda
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


def list_all_buckets():
    """
    List all S3 buckets in the AWS account
    
    Returns:
        list: List of bucket names
    """
    try:
        response = s3_client.list_buckets()
        bucket_names = [bucket['Name'] for bucket in response['Buckets']]
        logger.info(f"Found {len(bucket_names)} buckets in the account.")
        return bucket_names
    except ClientError as e:
        logger.error(f"Failed to list buckets: {e}")
        return []


def delete_bucket_contents(bucket_name):
    """
    Delete all objects from the specified bucket, including versioned objects
    
    Args:
        bucket_name (str): Name of the bucket to empty
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        bucket = s3_resource.Bucket(bucket_name)

        # Check if bucket has versioning enabled
        versioning = s3_client.get_bucket_versioning(Bucket=bucket_name)

        if 'Status' in versioning and versioning['Status'] == 'Enabled':
            # Delete all versions of all objects (including delete markers)
            logger.info(f"Bucket {bucket_name} has versioning enabled. Deleting all versions.")
            bucket.object_versions.delete()
        else:
            # Delete all objects in the bucket
            logger.info(f"Deleting all objects in bucket {bucket_name}")
            bucket.objects.all().delete()

        logger.info(f"Successfully emptied bucket {bucket_name}")
        return True

    except ClientError as e:
        logger.error(f"Error emptying bucket {bucket_name}: {e}")
        return False


def main():
    """
    Main function to delete contents of all buckets with user confirmation
    """
    buckets = list_all_buckets()

    if not buckets:
        logger.info("No buckets found. Nothing to do.")
        return

    print(f"Found {len(buckets)} buckets: {', '.join(buckets)}")

    # Ask for confirmation
    confirmation = input(f"Are you sure you want to delete ALL CONTENTS from ALL {len(buckets)} buckets? (yes/no): ")

    if confirmation.lower() != 'yes':
        logger.info("Operation cancelled by user.")
        return

    # Delete contents of each bucket
    successful = 0
    failed = 0

    for bucket_name in buckets:
        print(f"Processing bucket: {bucket_name}")
        if delete_bucket_contents(bucket_name):
            successful += 1
        else:
            failed += 1

    logger.info(f"Operation completed. Successfully emptied {successful} buckets. Failed to empty {failed} buckets.")


if __name__ == "__main__":
    main()