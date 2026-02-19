"""Session state machine and intervention heuristics."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum


class SessionState(str, Enum):
    """Coarse-grained session health state."""

    CALIENTE = "CALIENTE"
    NEUTRO = "NEUTRO"
    FRAGIL = "FRAGIL"
    CRITICO = "CRITICO"


@dataclass(frozen=True)
class SessionEvent:
    """A single playback outcome consumed by the state machine."""

    skipped: bool
    early_skip: bool


@dataclass(frozen=True)
class SessionInterventions:
    """Interventions derived from the current state."""

    exploration_multiplier: float
    should_inject_anchor: bool
    should_suggest_reset: bool


@dataclass(frozen=True)
class SessionSnapshot:
    """Current machine state + counters and interventions."""

    state: SessionState
    skip_run_len: int
    complete_run_len: int
    early_skip_recent: int
    interventions: SessionInterventions


@dataclass
class SessionStateMachine:
    """Minimal v1 state machine for skip-run behavior."""

    early_skip_window_size: int = 5
    skip_run_len: int = 0
    complete_run_len: int = 0
    _early_skip_recent_window: deque[bool] = field(default_factory=deque)

    def apply(self, event: SessionEvent) -> SessionSnapshot:
        if event.early_skip:
            self.skip_run_len += 1
            self.complete_run_len = 0
        elif event.skipped:
            self.skip_run_len += 1
            self.complete_run_len = 0
        else:
            self.complete_run_len += 1
            self.skip_run_len = 0

        self._early_skip_recent_window.append(event.early_skip)
        while len(self._early_skip_recent_window) > self.early_skip_window_size:
            self._early_skip_recent_window.popleft()

        return self.snapshot()

    def snapshot(self) -> SessionSnapshot:
        early_skip_recent = sum(self._early_skip_recent_window)
        state = self._resolve_state(early_skip_recent)
        return SessionSnapshot(
            state=state,
            skip_run_len=self.skip_run_len,
            complete_run_len=self.complete_run_len,
            early_skip_recent=early_skip_recent,
            interventions=self._resolve_interventions(state),
        )

    def _resolve_state(self, early_skip_recent: int) -> SessionState:
        if self.skip_run_len >= 4 or early_skip_recent >= 2:
            return SessionState.CRITICO
        if self.skip_run_len >= 2:
            return SessionState.FRAGIL
        if self.complete_run_len >= 3:
            return SessionState.CALIENTE
        return SessionState.NEUTRO

    @staticmethod
    def _resolve_interventions(state: SessionState) -> SessionInterventions:
        exploration_multiplier = {
            SessionState.CALIENTE: 1.0,
            SessionState.NEUTRO: 0.8,
            SessionState.FRAGIL: 0.4,
            SessionState.CRITICO: 0.2,
        }[state]
        return SessionInterventions(
            exploration_multiplier=exploration_multiplier,
            should_inject_anchor=state in (SessionState.FRAGIL, SessionState.CRITICO),
            should_suggest_reset=state is SessionState.CRITICO,
        )
