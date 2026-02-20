from __future__ import annotations

import gzip
import io
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, List

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class S3Path:
    bucket: str
    key: str

    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


class S3IO:
    def __init__(self, bucket: str, region: str) -> None:
        self.bucket = bucket
        self.region = region
        self._client = boto3.client("s3", region_name=region)

    def _put_with_retry(self, key: str, body: bytes, max_attempts: int = 5) -> None:
        delay = 0.5
        for attempt in range(1, max_attempts + 1):
            try:
                self._client.put_object(Bucket=self.bucket, Key=key, Body=body)
                return
            except Exception:
                if attempt >= max_attempts:
                    raise
                time.sleep(delay)
                delay = min(8.0, delay * 2)

    def ensure_prefixes(self, prefixes: List[str]) -> None:
        for prefix in prefixes:
            key = prefix.rstrip("/") + "/.keep"
            self._put_with_retry(key, b"")

    def put_json_gz(self, key: str, records: Iterable[Any]) -> None:
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            for rec in records:
                line = json.dumps(rec, default=str).encode("utf-8")
                gz.write(line + b"\n")
        buf.seek(0)
        self._put_with_retry(key, buf.read())

    def put_parquet(self, key: str, table: pa.Table) -> None:
        sink = io.BytesIO()
        pq.write_table(table, sink, compression="snappy")
        sink.seek(0)
        self._put_with_retry(key, sink.read())

    def put_deadletter(self, key: str, payload: dict) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
        self._put_with_retry(key, body)

    def put_tmp(self, key: str, payload: bytes) -> None:
        self._put_with_retry(key, payload)

    def list_keys(self, prefix: str) -> list[str]:
        keys = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def delete_keys(self, keys: List[str]) -> None:
        if not keys:
            return
        for i in range(0, len(keys), 1000):
            batch = keys[i : i + 1000]
            payload = {"Objects": [{"Key": key} for key in batch]}
            self._client.delete_objects(Bucket=self.bucket, Delete=payload)

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
            return True
        except self._client.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey"}:
                return False
            raise

    def get_object_bytes(self, key: str) -> bytes:
        obj = self._client.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()


def make_part_key(prefix: str, *parts: str) -> str:
    return "/".join([prefix.strip("/")] + [p.strip("/") for p in parts])


def new_run_id() -> str:
    return uuid.uuid4().hex
