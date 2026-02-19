from __future__ import annotations

from spotifygpt.session_state import SessionEvent, SessionState, SessionStateMachine


def test_state_transitions_skip_and_complete_runs() -> None:
    machine = SessionStateMachine()

    assert machine.apply(SessionEvent(skipped=False, early_skip=False)).state is SessionState.NEUTRO
    assert machine.apply(SessionEvent(skipped=False, early_skip=False)).state is SessionState.NEUTRO
    warm_snapshot = machine.apply(SessionEvent(skipped=False, early_skip=False))
    assert warm_snapshot.state is SessionState.CALIENTE
    assert warm_snapshot.complete_run_len == 3
    assert warm_snapshot.skip_run_len == 0

    fragil_snapshot = machine.apply(SessionEvent(skipped=True, early_skip=False))
    assert fragil_snapshot.state is SessionState.NEUTRO
    assert fragil_snapshot.complete_run_len == 0
    assert fragil_snapshot.skip_run_len == 1

    fragil_early = machine.apply(SessionEvent(skipped=True, early_skip=True))
    assert fragil_early.state is SessionState.FRAGIL
    assert fragil_early.skip_run_len == 2

    critico_snapshot = machine.apply(SessionEvent(skipped=True, early_skip=True))
    assert critico_snapshot.state is SessionState.CRITICO
    assert critico_snapshot.skip_run_len == 3
    assert critico_snapshot.early_skip_recent == 2


def test_critical_by_skip_run_length_and_window_rollover() -> None:
    machine = SessionStateMachine(early_skip_window_size=5)

    machine.apply(SessionEvent(skipped=True, early_skip=False))
    fragil = machine.apply(SessionEvent(skipped=True, early_skip=False))
    assert fragil.state is SessionState.FRAGIL

    machine.apply(SessionEvent(skipped=True, early_skip=False))
    critico = machine.apply(SessionEvent(skipped=True, early_skip=False))
    assert critico.state is SessionState.CRITICO
    assert critico.skip_run_len == 4

    # Window rollover should keep only most recent 5 values.
    for _ in range(5):
        machine.apply(SessionEvent(skipped=False, early_skip=False))
    rolled = machine.snapshot()
    assert rolled.early_skip_recent == 0
    assert rolled.skip_run_len == 0


def test_interventions_mapping() -> None:
    machine = SessionStateMachine()

    neutral = machine.snapshot().interventions
    assert neutral.exploration_multiplier == 0.8
    assert neutral.should_inject_anchor is False
    assert neutral.should_suggest_reset is False

    fragil = machine.apply(SessionEvent(skipped=True, early_skip=False))
    fragil = machine.apply(SessionEvent(skipped=True, early_skip=False))
    assert fragil.state is SessionState.FRAGIL
    assert fragil.interventions.exploration_multiplier == 0.4
    assert fragil.interventions.should_inject_anchor is True
    assert fragil.interventions.should_suggest_reset is False

    machine.apply(SessionEvent(skipped=True, early_skip=True))
    critico = machine.apply(SessionEvent(skipped=True, early_skip=True))
    assert critico.state is SessionState.CRITICO
    assert critico.interventions.exploration_multiplier == 0.2
    assert critico.interventions.should_inject_anchor is True
    assert critico.interventions.should_suggest_reset is True
