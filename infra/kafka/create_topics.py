import os
from confluent_kafka.admin import AdminClient, NewTopic

def create_topics():
    # Kafka bootstrap servers
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    # Topic specifications from architecture docs
    topic_specs = {
        "raw-events": {
            "num_partitions": 6,
            "replication_factor": 1,
            "config": {"retention.ms": "86400000"}  # 24h retention
        },
        "scored-events": {
            "num_partitions": 6,
            "replication_factor": 1,
            "config": {"retention.ms": "259200000"} # 72h retention
        },
        "alerts": {
            "num_partitions": 3,
            "replication_factor": 1,
            "config": {"retention.ms": "604800000"} # 7d retention
        },
        "model-updates": {
            "num_partitions": 1,
            "replication_factor": 1,
            "config": {"retention.ms": "2592000000"} # 30d retention
        }
    }

    # Fetch existing topics
    metadata = admin_client.list_topics(timeout=10)
    existing_topics = set(metadata.topics.keys())

    new_topics = []
    for topic_name, spec in topic_specs.items():
        if topic_name in existing_topics:
            print(f"Topic '{topic_name}' already exists. Skipping.")
        else:
            new_topic = NewTopic(
                topic_name,
                num_partitions=spec["num_partitions"],
                replication_factor=spec["replication_factor"],
                config=spec["config"]
            )
            new_topics.append(new_topic)

    if not new_topics:
        print("No new topics to create.")
        return

    # Create new topics
    fs = admin_client.create_topics(new_topics)

    # Wait for each operation to finish and print confirmation
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")

if __name__ == "__main__":
    create_topics()
