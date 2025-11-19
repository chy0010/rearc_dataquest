import requests
import boto3
import json

BUCKET_NAME = "rearc-dataquest-quest"
FILE_NAME = "us_population.json"

url = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"

# Fetch data
print("Fetching population data...")
response = requests.get(url)

if response.status_code != 200:
    print("Failed:", response.status_code)
    print(response.text)
    exit()

data = response.json()

# Save locally
with open(FILE_NAME, "w") as f:
    json.dump(data, f, indent=4)

print(f"Saved locally: {FILE_NAME}")

# Upload to S3
s3 = boto3.client("s3")

print("Uploading to S3...")

s3.upload_file(FILE_NAME, BUCKET_NAME, FILE_NAME)

print("Upload complete!")
