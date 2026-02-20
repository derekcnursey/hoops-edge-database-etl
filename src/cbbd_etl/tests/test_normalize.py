import pyarrow as pa

from cbbd_etl.normalize import normalize_records


def test_normalize_casts_types():
    records = [{"gameId": "123", "season": "2024", "startTime": "2024-11-01T12:00:00Z", "active": "true"}]
    table = normalize_records("fct_games", records)
    assert table.schema.field("gameId").type == pa.int64()
    assert table.schema.field("season").type == pa.int32()
    assert pa.types.is_timestamp(table.schema.field("startTime").type)
    assert table.column("gameId").to_pylist()[0] == 123
