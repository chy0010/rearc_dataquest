import os
import boto3
import pandas as pd
import io

s3 = boto3.client("s3")

def handler(event, context):
    bucket = os.environ["BUCKET_NAME"]
    ts_key = os.environ["TS_KEY"]
    pop_key = os.environ["POP_KEY"]

    # Read the CSV
    ts_bytes = s3.get_object(Bucket=bucket, Key=ts_key)["Body"].read()
    ts_text = ts_bytes.decode("utf-8")
    df_ts = pd.read_csv(io.StringIO(ts_text))

    # Read the JSON
    pop_bytes = s3.get_object(Bucket=bucket, Key=pop_key)["Body"].read()
    pop_json = pop_bytes.decode("utf-8")
    import json
    df_pop = pd.json_normalize(json.loads(pop_json)["data"])

    print("Report Lambda executed successfully")
    print("Rows in BLS file:", len(df_ts))
    print("Rows in population file:", len(df_pop))

    return {"status": "report done"}
