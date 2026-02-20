from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import boto3


@dataclass
class Checkpoint:
    endpoint: str
    parameter_hash: str
    payload: Dict[str, Any]


class CheckpointStore:
    def __init__(self, region: str, table_name: Optional[str] = None) -> None:
        self.table_name = table_name or os.getenv("CBBD_CHECKPOINT_TABLE", "cbbd_checkpoints")
        self._client = boto3.client("dynamodb", region_name=region)

    def get(self, endpoint: str, parameter_hash: str) -> Optional[Checkpoint]:
        resp = self._client.get_item(
            TableName=self.table_name,
            Key={
                "endpoint": {"S": endpoint},
                "parameter_hash": {"S": parameter_hash},
            },
        )
        item = resp.get("Item")
        if not item:
            return None
        payload = json.loads(item["payload"]["S"])
        return Checkpoint(endpoint=endpoint, parameter_hash=parameter_hash, payload=payload)

    def put(self, endpoint: str, parameter_hash: str, payload: Dict[str, Any]) -> None:
        self._client.put_item(
            TableName=self.table_name,
            Item={
                "endpoint": {"S": endpoint},
                "parameter_hash": {"S": parameter_hash},
                "payload": {"S": json.dumps(payload, default=str)},
            },
        )
