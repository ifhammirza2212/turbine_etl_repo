import json
import os

METADATA_PATH = os.path.join("etl_outputs", "etl_history", "pipeline_metadata.json")


def load_metadata() -> dict:
    if not os.path.exists(METADATA_PATH):
        return {}
    with open(METADATA_PATH) as metadata_file:
        return json.load(metadata_file)


def save_metadata(metadata: dict) -> None:
    os.makedirs(os.path.dirname(METADATA_PATH), exist_ok=True)
    with open(METADATA_PATH, "w") as metadata_file:
        json.dump(metadata, metadata_file, indent=2)
