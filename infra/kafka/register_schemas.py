import json
import os

import requests


def register_schemas():
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

    schemas_to_register = [
        {"topic": "raw-events-value", "file_path": "schemas/raw_event.avsc"},
        {"topic": "scored-events-value", "file_path": "schemas/scored_event.avsc"},
        {"topic": "alerts-value", "file_path": "schemas/alert_event.avsc"}
    ]

    base_dir = os.path.dirname(os.path.abspath(__file__))

    for schema_info in schemas_to_register:
        topic_subject = schema_info["topic"]
        file_path = os.path.join(base_dir, schema_info["file_path"])

        try:
            with open(file_path) as f:
                schema_content = f.read()

            payload = {
                "schema": schema_content
            }

            # Post to Confluent Schema Registry
            headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
            url = f"{schema_registry_url}/subjects/{topic_subject}/versions"

            response = requests.post(url, headers=headers, data=json.dumps(payload))

            if response.status_code in (200, 201):
                result = response.json()
                print(f"Schema for subject '{topic_subject}' registered successfully with id {result.get('id')}.")
            else:
                print(f"Failed to register schema for subject '{topic_subject}'. Status Code: {response.status_code}, Response: {response.text}")

        except Exception as e:
            print(f"Error registering schema '{schema_info['file_path']}': {e}")

if __name__ == "__main__":
    register_schemas()
