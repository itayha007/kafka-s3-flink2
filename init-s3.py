import boto3
import json
import tempfile
import os

# --- S3 Client Setup ---
s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",  # LocalStack endpoint
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

BUCKET_NAME = "dev-bucket"
OBJECT_KEY = "large.json"
TARGET_SIZE_MB = 500
APPROX_OBJ_SIZE = 1024  # ~1KB per object
NUM_OBJECTS = (TARGET_SIZE_MB * 1024 * 1024) // APPROX_OBJ_SIZE

# --- Ensure bucket exists ---
s3_client.create_bucket(Bucket=BUCKET_NAME)

print(f"Generating ~{TARGET_SIZE_MB}MB JSON with {NUM_OBJECTS:,} objects...")

# --- Use a temporary file to avoid memory issues ---
with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp:
    tmp.write("[")
    for i in range(NUM_OBJECTS):
        obj = {
            "id": i,
            "name": f"User {i}",
            "description": "x" * 1000  # ~1 KB string
        }
        json.dump(obj, tmp)
        if i < NUM_OBJECTS - 1:
            tmp.write(",")
        if i % 10000 == 0:
            print(f"  → Generated {i:,} objects...")
    tmp.write("]")
    tmp_path = tmp.name

print(f"File generated at: {tmp_path}")
print("Uploading to S3...")

# --- Upload file to S3 ---
s3_client.upload_file(tmp_path, BUCKET_NAME, OBJECT_KEY)

print(f"Upload complete: s3://{BUCKET_NAME}/{OBJECT_KEY}")

# --- Cleanup local file ---
os.remove(tmp_path)
print("Temporary file removed ✅")
