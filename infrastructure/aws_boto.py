import logging
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import re

load_dotenv()

base_path = os.getenv('local_data_path')


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name:    File to upload
    :param bucket:       Bucket to upload to
    :param object_name:  S3 object name. If not specified then file_name is used
    :return:             True if file was uploaded, else False
    """
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def resolve_s3_prefix(file: str) -> str:
    """
    Map file extension to correct S3 Bronze prefix.

    :param file: Filename to evaluate
    :return:     S3 prefix string
    """
    if file.endswith('.csv'):
        return "raw_stats"
    elif file.endswith('.json'):
        return "raw_stats_reference_data"   
    else:
        return "raw_stats_unknown"          


def upload_files_by_region(base_path: str, bucket: str) -> None:
    """
    Scans a local directory, extracts region codes from filenames,
    and uploads files to S3 partitioned by file type and region.

    CSV  → raw_stats/region=XX/filename.csv
    JSON → raw_stats_reference_data/region=XX/filename.json

    :param base_path: Local directory path containing the files
    :param bucket:    S3 bucket name to upload to
    """

    # Collect valid files
    file_names = [
        file for file in os.listdir(base_path)
        if file.endswith(('.csv', '.json'))
    ]

    # Build region map
    file_region_map = {}
    for file in file_names:
        match = re.match(r'^([A-Z]{2})', file)
        file_region_map[file] = match.group(1) if match else "unknown"

    # Upload partitioned by file type and region
    for file, region in file_region_map.items():
        full_path = os.path.join(base_path, file)
        prefix    = resolve_s3_prefix(file)      
        obj_name  = f"{prefix}/region={region}/{file}"

        upload_file(file_name=full_path, bucket=bucket, object_name=obj_name)
        print(f"Uploaded: {obj_name}")


# Usage
upload_files_by_region(base_path=base_path, bucket="yt-pipeline-bronze")