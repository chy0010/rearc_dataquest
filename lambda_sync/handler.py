import os
import boto3

s3 = boto3.client("s3")

def lambda_handler(event, context):
    bucket = os.environ["BUCKET_NAME"]
    pop_key = os.environ["POP_KEY"]

    # Re-copy population file to trigger S3 event
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": pop_key},
        Key=pop_key
    )

    return {"status": "sync completed"}
