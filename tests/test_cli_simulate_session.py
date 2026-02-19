from __future__ import annotations

from spotifygpt.cli import main


def test_cli_simulate_session_prints_transitions(capsys) -> None:
    assert main(["simulate-session", "c", "s", "e", "e", "c"]) == 0
    captured = capsys.readouterr()

    lines = [line for line in captured.out.splitlines() if line.strip()]
    assert len(lines) == 5
    assert "#1 complete: state=NEUTRO" in lines[0]
    assert "#2 skip: state=NEUTRO" in lines[1]
    assert "#3 early-skip: state=FRAGIL" in lines[2]
    assert "#4 early-skip: state=CRITICO" in lines[3]


def test_cli_simulate_session_rejects_invalid_event(capsys) -> None:
    assert main(["simulate-session", "complete", "wat"]) == 1
    captured = capsys.readouterr()
    assert "Invalid event 'wat'" in captured.err
