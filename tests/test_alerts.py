from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import sqlite3

from spotifygpt.alerts import Alert, detect_alerts
from spotifygpt.importer import init_db, store_alerts


@dataclass(frozen=True)
class SampleStream:
    track_key: str
    end_time: str


def _timestamp(base: datetime, days: int, hours: int) -> str:
    return (base + timedelta(days=days, hours=hours)).strftime("%Y-%m-%d %H:%M")


def test_detect_deriva_alert() -> None:
    base = datetime(2023, 1, 2)
    streams = []
    for _ in range(25):
        streams.append(SampleStream("track-a", _timestamp(base, 0, 10)))
    for _ in range(5):
        streams.append(SampleStream("track-b", _timestamp(base, 1, 10)))

    for _ in range(25):
        streams.append(SampleStream("track-c", _timestamp(base, 7, 10)))
    for _ in range(5):
        streams.append(SampleStream("track-d", _timestamp(base, 8, 10)))

    alerts = detect_alerts(streams)

    assert any(alert.alert_type == "deriva" for alert in alerts)


def test_detect_bloqueo_alert() -> None:
    base = datetime(2023, 2, 6)
    streams = []
    for _ in range(30):
        streams.append(SampleStream("loop-track", _timestamp(base, 0, 12)))
    for _ in range(5):
        streams.append(SampleStream("alt-track", _timestamp(base, 0, 13)))

    alerts = detect_alerts(streams)

    assert any(alert.alert_type == "bloqueo" for alert in alerts)


def test_detect_caos_alert() -> None:
    base = datetime(2023, 3, 6)
    streams = []
    for hour in range(24):
        streams.append(SampleStream(f"track-{hour}", _timestamp(base, 0, hour)))
        streams.append(SampleStream(f"track-{hour}", _timestamp(base, 1, hour)))

    alerts = detect_alerts(streams)

    assert any(alert.alert_type == "caos" for alert in alerts)


def test_store_alerts_persists_evidence(tmp_path) -> None:
    db_path = tmp_path / "alerts.db"
    alert = Alert(
        alert_type="caos",
        detected_at="2023-03-06",
        evidence={"week_start": "2023-03-06", "normalized_entropy": 0.91},
    )
    with sqlite3.connect(db_path) as connection:
        init_db(connection)
        stored = store_alerts(connection, [alert])

        row = connection.execute(
            "SELECT alert_type, detected_at, evidence FROM alerts"
        ).fetchone()

    assert stored == 1
    assert row is not None
    assert row[0] == "caos"
    assert row[1] == "2023-03-06"
    assert json.loads(row[2]) == alert.evidence
