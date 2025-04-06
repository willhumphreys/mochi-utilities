import os
import tempfile
import pandas as pd
import boto3
import botocore
import sys

def decompress_lzo(lzo_file_path, output_dir):
    """
    Decompress an LZO file using the lzop command-line tool.

    Args:
        lzo_file_path (str): Path to the LZO file
        output_dir (str): Directory to output the decompressed file

    Returns:
        str: Path to the decompressed file, or None if decompression failed
    """
    # Get the base filename without the .lzo extension
    base_filename = os.path.basename(lzo_file_path)
    if base_filename.endswith('.lzo'):
        base_filename = base_filename[:-4]

    decompressed_path = os.path.join(output_dir, base_filename)

    try:
        # Use the lzop command-line tool to decompress
        import subprocess

        # Command to decompress the file
        cmd = ['lzop', '-d', '-o', decompressed_path, lzo_file_path]

        # Run the command
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Check if the command was successful
        if result.returncode != 0:
            print(f"Error decompressing file: {result.stderr}")
            return None

        print(f"Successfully decompressed to {decompressed_path}")
        return decompressed_path

    except Exception as e:
        print(f"Exception during decompression: {str(e)}")
        return None


def analyze_date_range(csv_file):
    """
    Analyze a CSV file to determine the date range.

    Args:
        csv_file (str): Path to the CSV file

    Returns:
        dict: Analysis results including min/max date, frequency, and row count
    """
    try:
        # Determine the file frequency based on filename
        frequency = "Unknown"
        if "day" in csv_file.lower():
            frequency = "Daily"
        elif "hour" in csv_file.lower():
            frequency = "Hourly"
        elif "min" in csv_file.lower():
            frequency = "Minute"

        # Read the CSV file
        df = pd.read_csv(csv_file)

        # Check if 't' column exists (timestamp)
        if 't' in df.columns:
            # Convert the timestamp column to datetime
            df['datetime'] = pd.to_datetime(df['t'], unit='ms')

            # Calculate the min and max dates
            min_date = df['datetime'].min()
            max_date = df['datetime'].max()

            # Return the analysis results
            return {
                'file': csv_file,
                'min_date': min_date,
                'max_date': max_date,
                'frequency': frequency,
                'row_count': len(df)
            }
        else:
            print(f"Warning: CSV file {csv_file} does not have a 't' column for timestamp.")
            return None

    except Exception as e:
        print(f"Error analyzing file {csv_file}: {str(e)}")
        return None


def download_from_s3(bucket_name, s3_key, local_path):
    """
    Download a file from S3 to a local path.

    Args:
        bucket_name (str): S3 bucket name
        s3_key (str): S3 object key
        local_path (str): Local path to save the file

    Returns:
        bool: True if download was successful, False otherwise
    """
    try:
        s3_client = boto3.client('s3')
        print(f"Downloading s3://{bucket_name}/{s3_key} to {local_path}")
        s3_client.download_file(bucket_name, s3_key, local_path)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f"Error: The object s3://{bucket_name}/{s3_key} does not exist.")
        else:
            print(f"Error downloading from S3: {str(e)}")
        return False
    except Exception as e:
        print(f"Unexpected error downloading from S3: {str(e)}")
        return False


def main():
    # Get ticker symbol from command line arguments or prompt user
    if len(sys.argv) > 1:
        ticker = sys.argv[1].upper()
    else:
        ticker = input("Enter ticker symbol: ").upper()

    print(f"Processing data for {ticker}")

    # S3 bucket information
    bucket_name = "mochi-prod-raw-historical-data"

    # S3 keys for the files to process
    s3_keys = [
        f"stocks/{ticker}/polygon/{ticker}_polygon_day.csv.lzo",
        f"stocks/{ticker}/polygon/{ticker}_polygon_hour.csv.lzo",
        f"stocks/{ticker}/polygon/{ticker}_polygon_min.csv.lzo"
    ]

    # Create a temporary directory for the downloaded and decompressed files
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Temporary directory: {temp_dir}")
        results = []

        for s3_key in s3_keys:
            # Local path for the downloaded LZO file
            local_lzo_path = os.path.join(temp_dir, os.path.basename(s3_key))

            # Download the file from S3
            if not download_from_s3(bucket_name, s3_key, local_lzo_path):
                print(f"Warning: Failed to download s3://{bucket_name}/{s3_key}. Skipping.")
                continue

            print(f"Processing: {local_lzo_path}")

            # Decompress the file
            decompressed_file = decompress_lzo(local_lzo_path, temp_dir)

            if decompressed_file:
                # Analyze the decompressed file
                analysis = analyze_date_range(decompressed_file)
                if analysis:
                    results.append(analysis)

        # Print the results in a formatted table
        if results:
            print("\n" + "=" * 100)
            print(f"{'File':<25} {'Frequency':<10} {'Start Date':<25} {'End Date':<25} {'# Records':<10}")
            print("-" * 100)

            for result in results:
                print(f"{os.path.basename(result['file']):<25} "
                      f"{result['frequency']:<10} "
                      f"{result['min_date'].strftime('%Y-%m-%d %H:%M:%S'):<25} "
                      f"{result['max_date'].strftime('%Y-%m-%d %H:%M:%S'):<25} "
                      f"{result['row_count']:<10}")

            print("=" * 100)
        else:
            print("No files were successfully analyzed.")

if __name__ == "__main__":
    main()
