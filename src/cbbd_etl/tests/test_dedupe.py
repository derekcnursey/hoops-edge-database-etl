from cbbd_etl.normalize import dedupe_records


def test_dedupe_records():
    records = [
        {"gameId": 1, "teamId": 10},
        {"gameId": 1, "teamId": 10},
        {"gameId": 2, "teamId": 11},
    ]
    deduped = dedupe_records(records, ("gameId", "teamId"))
    assert len(deduped) == 2
