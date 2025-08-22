import time
import json
import random
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

BOOTSTRAP_SERVERS = "broker:9093"
TOPIC_NAME = "input"
TOTAL_MESSAGES = 1_000_000   # stop after sending these

def wait_for_kafka():
    for i in range(30):
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

def create_topic():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=1)]
    try:
        admin.create_topics(new_topics=topic_list, validate_only=False)
        print(f"✅ Topic '{TOPIC_NAME}' created")
    except Exception:
        print(f"⚠️ Topic '{TOPIC_NAME}' may already exist")
    finally:
        admin.close()

def produce_messages():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,    # slight batching for better throughput
        batch_size=32_768
    )

    start = time.time()
    for msg_id in range(1, TOTAL_MESSAGES + 1):
        msg = {
            "id": msg_id,
            "msg": random.choice([
                "hello from python",
                "streaming rocks!",
                "docker-compose FTW",
                "check kafka-ui",
                "random data incoming"
            ]),
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "value": random.randint(1, 100)
        }

        producer.send(TOPIC_NAME, msg)

        # simple progress logging every 100k messages
        if msg_id % 100_000 == 0:
            elapsed = time.time() - start
            print(f"➡️ Sent {msg_id:,}/{TOTAL_MESSAGES:,} messages ({msg_id/elapsed:.0f} msg/sec)")

    producer.flush()
    producer.close()

    elapsed = time.time() - start
    print(f"✅ Finished sending {TOTAL_MESSAGES:,} messages in {elapsed:.2f} seconds")
    print(f"⚡ Throughput ≈ {TOTAL_MESSAGES/elapsed:.0f} msg/sec")

if __name__ == "__main__":
    wait_for_kafka()
    create_topic()
    produce_messages()
