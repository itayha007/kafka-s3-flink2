import os
import json
import time
import random
import tempfile
from typing import Set

import boto3
from botocore.config import Config
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

# -----------------------------
# Config (override via env vars)
# -----------------------------
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "broker:9093")
TOPIC_NAME        = os.getenv("TOPIC_NAME", "input")
BUCKET_NAME       = os.getenv("BUCKET_NAME", "dev-bucket")
S3_ENDPOINT       = os.getenv("S3_ENDPOINT", "http://localstack:4566")  # inside docker network, use 'localstack'
TOTAL_MESSAGES    = int(os.getenv("TOTAL_MESSAGES", "100"))
LARGE_COUNT       = int(os.getenv("LARGE_COUNT", "10"))                   # 5–10 as requested
LARGE_SIZE_MB     = int(os.getenv("LARGE_SIZE_MB", "500"))
SMALL_SIZE_BYTES  = int(os.getenv("SMALL_SIZE_BYTES", "1024"))           # ~1KB small object

assert 0 <= LARGE_COUNT <= TOTAL_MESSAGES, "LARGE_COUNT must be in [0, TOTAL_MESSAGES]"

# ---------------
# S3 client setup
# ---------------
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    config=Config(s3={"addressing_style": "path"})  # LocalStack-friendly
)

def ensure_bucket(name: str):
    try:
        s3.create_bucket(Bucket=name)
        print(f"✅ Created bucket {name}")
    except Exception as e:
        # Bucket may already exist
        print(f"ℹ️ Bucket {name} exists or cannot be created again: {e}")

def put_small_object(key: str):
    # ~ SMALL_SIZE_BYTES JSON object written to a temp file (stream-safe)
    with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
        obj = {
            "type": "small",
            "key": key,
            "data": "x" * max(0, SMALL_SIZE_BYTES - 50)  # approximate size
        }
        json.dump(obj, tmp)
        tmp_path = tmp.name
    s3.upload_file(tmp_path, BUCKET_NAME, key)
    os.remove(tmp_path)

def put_large_object(key: str, size_mb: int):
    # Stream a single large JSON file to disk, then upload (no giant strings in memory)
    # Structure: {"type":"large","key":"...","payload":"AAAAAA...."}
    target_bytes = size_mb * 1024 * 1024
    header = f'{{"type":"large","key":"{key}","payload":"'
    footer = '"}'

    with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
        written = 0
        tmp.write(header)
        written += len(header)

        chunk = "A" * (1024 * 1024)  # 1 MB per chunk
        while written + len(chunk) + len(footer) < target_bytes:
            tmp.write(chunk)
            written += len(chunk)

        # write the remaining bytes (if any)
        remaining = max(0, target_bytes - written - len(footer))
        if remaining:
            tmp.write("A" * remaining)
            written += remaining

        tmp.write(footer)
        tmp_path = tmp.name

    s3.upload_file(tmp_path, BUCKET_NAME, key)
    os.remove(tmp_path)

# -----------------
# Kafka helpers
# -----------------
def wait_for_kafka():
    for _ in range(60):
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
            admin.list_topics()
            admin.close()
            print("✅ Kafka is ready")
            return
        except Exception as e:
            print(f"⏳ Waiting for Kafka... {e}")
            time.sleep(2)
    raise RuntimeError("Kafka not reachable")

def ensure_topic():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        admin.create_topics(
            new_topics=[NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=1)],
            validate_only=False
        )
        print(f"✅ Topic '{TOPIC_NAME}' created")
    except Exception as e:
        print(f"ℹ️ Topic '{TOPIC_NAME}' exists or cannot be created again: {e}")
    finally:
        admin.close()

def choose_large_ids(total: int, count: int) -> Set[int]:
    if count <= 0:
        return set()
    # Randomly distribute a few large objects across the range
    return set(random.sample(range(1, total + 1), count))

def produce_all():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        batch_size=32_768,
        acks="all",
    )

    large_ids = choose_large_ids(TOTAL_MESSAGES, LARGE_COUNT)
    start = time.time()

    for msg_id in range(1, TOTAL_MESSAGES + 1):
        key = f"{msg_id}"

        # Create S3 object first (so the key is valid when consumer reads)
        if msg_id in large_ids:
            put_large_object(key, LARGE_SIZE_MB)
        else:
            put_small_object(key)

        # Produce Kafka message pointing at that object
        message = {
            "bucket": BUCKET_NAME,
            "item_id": [key],
        }
        headers = [("source.type", b"s3")]

        producer.send(
            TOPIC_NAME,
            value=message,
            headers=headers
        )

        if msg_id % 100 == 0:
            elapsed = time.time() - start
            rate = msg_id / elapsed if elapsed > 0 else 0.0
            print(f"➡️ Created & sent {msg_id:,}/{TOTAL_MESSAGES:,}  (~{rate:.0f} msg/sec)")

    producer.flush()
    producer.close()

    elapsed = time.time() - start
    print(f"✅ Finished: {TOTAL_MESSAGES:,} S3 objects + Kafka messages in {elapsed:.2f}s")
    print(f"⚡ Throughput ≈ {TOTAL_MESSAGES/elapsed:.0f} msg/sec")

if __name__ == "__main__":
    print("🔧 Ensuring S3 bucket...")
    ensure_bucket(BUCKET_NAME)

    print("🔧 Waiting for Kafka & ensuring topic...")
    wait_for_kafka()
    ensure_topic()

    print("🚀 Creating S3 objects and producing Kafka messages...")
    produce_all()
