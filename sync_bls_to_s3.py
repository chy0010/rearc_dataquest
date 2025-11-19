import os
import requests
import boto3
from botocore.exceptions import ClientError

# --- Configuration ---
BUCKET_NAME = "rearc-dataquest-quest"
LOCAL_FOLDER = "bls_files"
BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"

# Create local folder if it doesn't exist
if not os.path.exists(LOCAL_FOLDER):
    os.makedirs(LOCAL_FOLDER)

# List of files to download
files = [
    'pr.class',
    'pr.contacts',
    'pr.data.0.Current',
    'pr.data.1.AllData',
    'pr.duration',
    'pr.footnote',
    'pr.measure',
    'pr.period'
]

# AWS S3 client
s3 = boto3.client('s3')

# Browser headers to avoid 403
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/119.0 Safari/537.36"
}

# Download & upload
for filename in files:
    local_path = os.path.join(LOCAL_FOLDER, filename)

    # Download file if it doesn't exist locally
    if not os.path.exists(local_path):
        url = BLS_BASE_URL + filename
        print(f"Downloading {filename}...")
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            with open(local_path, "wb") as f:
                f.write(resp.content)
            print(f"{filename} downloaded!")
        else:
            print(f"Failed to download {filename}: Status {resp.status_code}")
            continue  # Skip upload if download failed

    # Upload to S3 if not already present
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=filename)
        print(f"{filename} already exists in S3, skipping upload.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Uploading {filename} to S3...")
            s3.upload_file(local_path, BUCKET_NAME, filename)
            print(f"{filename} uploaded!")
        else:
            print(f"Error checking {filename} in S3: {e}")
