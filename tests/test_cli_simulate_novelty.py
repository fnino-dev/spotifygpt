from __future__ import annotations

from spotifygpt.cli import main


def test_cli_simulate_novelty_happy_path(capsys) -> None:
    rc = main(
        [
            "simulate-novelty",
            "--time-block",
            "EVENING",
            "--state",
            "FRAGIL",
            "--candidates",
            "n1,n2,a1,n3,a2",
            "--anchors",
            "a1,a2",
        ]
    )

    assert rc == 0
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert "exploration=0.20" in lines[0]
    assert "anchor_ratio=0.80" in lines[0]
    assert "anchor_every_n=2" in lines[0]
    assert lines[1] == "n1,a1,n2,a2,n3"


def test_cli_simulate_novelty_empty_anchors(capsys) -> None:
    rc = main(
        [
            "simulate-novelty",
            "--time-block",
            "MORNING",
            "--state",
            "NEUTRO",
            "--candidates",
            "n1,n2,n3",
        ]
    )

    assert rc == 0
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert "anchor_every_n=None" in lines[0]
    assert lines[1] == "n1,n2,n3"


def test_cli_simulate_novelty_invalid_state_returns_nonzero(capsys) -> None:
    rc = main(
        [
            "simulate-novelty",
            "--time-block",
            "EVENING",
            "--state",
            "BAD",
            "--candidates",
            "n1,n2",
            "--anchors",
            "a1",
        ]
    )

    assert rc == 1
    captured = capsys.readouterr()
    assert "Invalid --state value" in captured.err
