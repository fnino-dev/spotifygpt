from __future__ import annotations

from datetime import datetime

from spotifygpt import cli


def test_cli_simulate_session_novelty_valid_flow(capsys, monkeypatch) -> None:
    monkeypatch.setattr(cli, "get_time_block", lambda _moment: "EVENING")
    monkeypatch.setattr(cli, "datetime", type("_FakeDatetime", (), {"now": staticmethod(lambda: datetime(2024, 1, 1, 18, 0))}))

    rc = cli.main(
        [
            "simulate-session-novelty",
            "--events",
            "c,s,e,e,c",
            "--candidates",
            "n1,n2,a1,n3,a2",
            "--anchors",
            "a1,a2",
        ]
    )

    assert rc == 0
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert lines[0] == "final_state=CRITICO"
    assert lines[1] == "time_block=EVENING"
    assert "exploration=0.10" in lines[2]
    assert "anchor_ratio=0.90" in lines[2]
    assert "anchor_every_n=1" in lines[2]
    assert lines[3] == "sequenced=a1,n1,a2,n2,n3"


def test_cli_simulate_session_novelty_invalid_events(capsys) -> None:
    rc = cli.main(
        [
            "simulate-session-novelty",
            "--events",
            "c,wat",
            "--time-block",
            "EVENING",
            "--candidates",
            "n1,n2",
        ]
    )

    assert rc == 1
    captured = capsys.readouterr()
    assert "Invalid event 'wat'" in captured.err


def test_cli_simulate_session_novelty_empty_candidates(capsys) -> None:
    rc = cli.main(
        [
            "simulate-session-novelty",
            "--events",
            "c,s",
            "--time-block",
            "EVENING",
            "--candidates",
            "",
        ]
    )

    assert rc == 1
    captured = capsys.readouterr()
    assert "Invalid --candidates value" in captured.err
