"""Shared test fixtures for cbbd_etl test suite.

Provides moto-based AWS mocks and realistic sample data fixtures
matching the CBBD API response shapes.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List

import boto3
import pytest
from moto import mock_aws

from cbbd_etl.config import Config


# ---------------------------------------------------------------------------
# AWS credential safety — prevent accidental real AWS calls
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True, scope="session")
def aws_credentials():
    """Set fake AWS credentials for the entire test session."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    yield
    # Cleanup is not strictly necessary for session-scoped but keeps things tidy
    for key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN",
        "AWS_DEFAULT_REGION",
    ):
        os.environ.pop(key, None)


# ---------------------------------------------------------------------------
# Moto-based AWS service fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def s3_bucket():
    """Create a moto mock S3 bucket named 'hoops-edge' in us-east-1.

    Yields the boto3 S3 client so tests can make additional assertions.
    """
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="hoops-edge")
        yield client


@pytest.fixture()
def dynamodb_table():
    """Create a moto mock DynamoDB table 'cbbd_checkpoints'.

    Hash key: endpoint (S), Range key: parameter_hash (S).
    Yields the boto3 DynamoDB client.
    """
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName="cbbd_checkpoints",
            KeySchema=[
                {"AttributeName": "endpoint", "KeyType": "HASH"},
                {"AttributeName": "parameter_hash", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "endpoint", "AttributeType": "S"},
                {"AttributeName": "parameter_hash", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield client


@pytest.fixture()
def glue_client():
    """Create a moto mock Glue client.

    Yields the boto3 Glue client.
    """
    with mock_aws():
        client = boto3.client("glue", region_name="us-east-1")
        yield client


# ---------------------------------------------------------------------------
# Configuration fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_config() -> Config:
    """Return a Config object with realistic test values."""
    return Config({
        "bucket": "hoops-edge",
        "region": "us-east-1",
        "api": {
            "base_url": "https://api.collegebasketballdata.com",
            "timeout_seconds": 10,
            "max_concurrency": 3,
            "rate_limit_per_sec": 3,
            "retry": {
                "max_attempts": 3,
                "base_delay_seconds": 0.01,
                "max_delay_seconds": 0.05,
            },
        },
        "seasons": {"start": 2024, "end": 2024},
        "endpoints": {},
        "s3_layout": {
            "raw_prefix": "raw",
            "bronze_prefix": "bronze",
            "silver_prefix": "silver",
            "gold_prefix": "gold",
            "ref_prefix": "ref",
            "meta_prefix": "meta",
            "deadletter_prefix": "deadletter",
            "tmp_prefix": "tmp",
            "athena_prefix": "athena",
        },
    })


# ---------------------------------------------------------------------------
# Sample data fixtures — realistic CBBD API response shapes
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_game_records() -> List[Dict[str, Any]]:
    """Return sample records matching the CBBD API /games response shape."""
    return [
        {
            "id": 401596281,
            "season": 2024,
            "seasonType": "regular",
            "startDate": "2023-11-06T23:30:00.000Z",
            "startTimeTbd": False,
            "neutralSite": False,
            "conferenceGame": False,
            "gameType": "regular",
            "status": "final",
            "period": 2,
            "clock": "0:00",
            "homeTeamId": 150,
            "homeTeam": "Kentucky",
            "homeConference": "SEC",
            "homeScore": 95,
            "awayTeamId": 2724,
            "awayTeam": "New Mexico State",
            "awayConference": "CUSA",
            "awayScore": 63,
            "venueId": 3800,
            "venue": "Rupp Arena",
        },
    ]


@pytest.fixture()
def sample_plays_records() -> List[Dict[str, Any]]:
    """Return sample records matching the CBBD API /plays response shape."""
    return [
        {
            "id": 5801000001,
            "gameId": 401596281,
            "period": 1,
            "clock": "19:45",
            "secondsRemaining": 1185,
            "teamId": 150,
            "team": "Kentucky",
            "playType": "Jumper",
            "playText": "Reed Sheppard made Jumper.",
            "scoringPlay": True,
            "shootingPlay": True,
            "scoreValue": 2,
            "wallclock": "2023-11-06T23:35:12.000Z",
            "homeScore": 2,
            "awayScore": 0,
        },
        {
            "id": 5801000002,
            "gameId": 401596281,
            "period": 1,
            "clock": "19:20",
            "secondsRemaining": 1160,
            "teamId": 2724,
            "team": "New Mexico State",
            "playType": "Turnover",
            "playText": "Turnover by New Mexico State.",
            "scoringPlay": False,
            "shootingPlay": False,
            "scoreValue": 0,
            "wallclock": "2023-11-06T23:36:00.000Z",
            "homeScore": 2,
            "awayScore": 0,
        },
        {
            "id": 5801000003,
            "gameId": 401596281,
            "period": 1,
            "clock": "18:55",
            "secondsRemaining": 1135,
            "teamId": 150,
            "team": "Kentucky",
            "playType": "Three Point Jumper",
            "playText": "Antonio Reeves made Three Point Jumper. Assisted by Rob Dillingham.",
            "scoringPlay": True,
            "shootingPlay": True,
            "scoreValue": 3,
            "wallclock": "2023-11-06T23:37:10.000Z",
            "homeScore": 5,
            "awayScore": 0,
        },
        {
            "id": 5801000004,
            "gameId": 401596281,
            "period": 2,
            "clock": "15:00",
            "secondsRemaining": 900,
            "teamId": 2724,
            "team": "New Mexico State",
            "playType": "Free Throw",
            "playText": "Free Throw made.",
            "scoringPlay": True,
            "shootingPlay": True,
            "scoreValue": 1,
            "wallclock": "2023-11-07T00:05:00.000Z",
            "homeScore": 50,
            "awayScore": 30,
        },
    ]
