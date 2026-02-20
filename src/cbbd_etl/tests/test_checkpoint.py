import json

import boto3
from botocore.stub import Stubber

from cbbd_etl.checkpoint import CheckpointStore


def test_checkpoint_get_put():
    client = boto3.client("dynamodb", region_name="us-east-1")
    stubber = Stubber(client)

    store = CheckpointStore("us-east-1", table_name="cbbd_checkpoints")
    store._client = client

    payload = {"last_ingested_date": "2026-01-28"}
    stubber.add_response(
        "put_item",
        {},
        {
            "TableName": "cbbd_checkpoints",
            "Item": {
                "endpoint": {"S": "games"},
                "parameter_hash": {"S": "abc"},
                "payload": {"S": json.dumps(payload)},
            },
        },
    )

    stubber.add_response(
        "get_item",
        {"Item": {"endpoint": {"S": "games"}, "parameter_hash": {"S": "abc"}, "payload": {"S": json.dumps(payload)}}},
        {"TableName": "cbbd_checkpoints", "Key": {"endpoint": {"S": "games"}, "parameter_hash": {"S": "abc"}}},
    )

    with stubber:
        store.put("games", "abc", payload)
        cp = store.get("games", "abc")
        assert cp is not None
        assert cp.payload["last_ingested_date"] == "2026-01-28"
