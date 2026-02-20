"""Deterministic listening context decision layer (v1)."""

from __future__ import annotations

from dataclasses import dataclass

from spotifygpt.diurnal import AFTERNOON, EVENING, LATE_NIGHT, MORNING, NIGHT, get_feature_prior
from spotifygpt.musical_dna import MusicalDNA
from spotifygpt.session_state import SessionState

_TIME_BLOCKS = {MORNING, AFTERNOON, EVENING, NIGHT, LATE_NIGHT}

_STATE_CONFIG: dict[str, dict[str, float | int | str | None]] = {
    SessionState.CALIENTE.value: {
        "exploration_multiplier": 1.0,
        "anchor_ratio": 0.15,
        "anchor_every_n": None,
        "sequencing_strategy": "flow_explore",
        "energy_width": 0.18,
        "tempo_width": 24,
    },
    SessionState.NEUTRO.value: {
        "exploration_multiplier": 0.8,
        "anchor_ratio": 0.25,
        "anchor_every_n": 5,
        "sequencing_strategy": "balanced",
        "energy_width": 0.14,
        "tempo_width": 20,
    },
    SessionState.FRAGIL.value: {
        "exploration_multiplier": 0.4,
        "anchor_ratio": 0.45,
        "anchor_every_n": 3,
        "sequencing_strategy": "stabilize_with_anchors",
        "energy_width": 0.10,
        "tempo_width": 16,
    },
    SessionState.CRITICO.value: {
        "exploration_multiplier": 0.2,
        "anchor_ratio": 0.65,
        "anchor_every_n": 2,
        "sequencing_strategy": "recovery_mode",
        "energy_width": 0.08,
        "tempo_width": 12,
    },
}

_TEMPO_PRIOR_BY_BLOCK: dict[str, int] = {
    MORNING: 112,
    AFTERNOON: 124,
    EVENING: 122,
    NIGHT: 108,
    LATE_NIGHT: 92,
}


@dataclass(frozen=True)
class ListeningContext:
    time_block: str
    session_state: str
    mode: str | None
    dna_profile: MusicalDNA
    fatigue_score: float = 0.0
    energy_override: float | None = None


@dataclass(frozen=True)
class RecommendationPlan:
    target_energy_range: tuple[float, float]
    target_tempo_range: tuple[int, int]
    exploration_multiplier: float
    anchor_ratio: float
    anchor_every_n: int | None
    sequencing_strategy: str
    explanation: dict[str, str]


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _validate_context(context: ListeningContext) -> None:
    if context.time_block not in _TIME_BLOCKS:
        valid = ", ".join(sorted(_TIME_BLOCKS))
        raise ValueError(f"Unknown time_block '{context.time_block}'. Expected one of: {valid}")
    if context.session_state not in _STATE_CONFIG:
        valid = ", ".join(sorted(_STATE_CONFIG))
        raise ValueError(f"Unknown session_state '{context.session_state}'. Expected one of: {valid}")
    if not 0.0 <= context.fatigue_score <= 1.0:
        raise ValueError("fatigue_score must be between 0.0 and 1.0")
    if context.energy_override is not None and not 0.0 <= context.energy_override <= 1.0:
        raise ValueError("energy_override must be between 0.0 and 1.0")


def compute_recommendation_plan(context: ListeningContext) -> RecommendationPlan:
    """Compute a deterministic recommendation plan from listening context."""

    _validate_context(context)

    state_cfg = _STATE_CONFIG[context.session_state]
    dna_energy = context.dna_profile.feature_summary["energy"].mean
    dna_tempo = context.dna_profile.feature_summary["tempo"].mean
    diurnal_prior = get_feature_prior(context.time_block)

    energy_center = (0.6 * dna_energy) + (0.4 * diurnal_prior["energy"])
    energy_center -= context.fatigue_score * 0.25
    energy_source = "dna+diurnal"
    if context.energy_override is not None:
        energy_center = context.energy_override
        energy_source = "override"

    energy_center = _clamp(energy_center, 0.0, 1.0)
    energy_width = float(state_cfg["energy_width"])
    energy_low = _clamp(energy_center - energy_width, 0.0, 1.0)
    energy_high = _clamp(energy_center + energy_width, 0.0, 1.0)

    tempo_center = (0.6 * dna_tempo) + (0.4 * _TEMPO_PRIOR_BY_BLOCK[context.time_block])
    tempo_center -= context.fatigue_score * 20.0
    tempo_center += (energy_center - 0.5) * 20.0

    tempo_width = int(state_cfg["tempo_width"])
    tempo_low = max(40, int(round(tempo_center - tempo_width)))
    tempo_high = min(220, int(round(tempo_center + tempo_width)))

    exploration_multiplier = float(state_cfg["exploration_multiplier"])
    exploration_multiplier = _clamp(exploration_multiplier * (1.0 - 0.5 * context.fatigue_score), 0.1, 1.2)

    anchor_ratio = float(state_cfg["anchor_ratio"])
    anchor_ratio = _clamp(anchor_ratio + (0.10 * context.fatigue_score), 0.05, 0.8)

    return RecommendationPlan(
        target_energy_range=(round(energy_low, 3), round(energy_high, 3)),
        target_tempo_range=(tempo_low, tempo_high),
        exploration_multiplier=round(exploration_multiplier, 3),
        anchor_ratio=round(anchor_ratio, 3),
        anchor_every_n=state_cfg["anchor_every_n"],
        sequencing_strategy=str(state_cfg["sequencing_strategy"]),
        explanation={
            "time_block": context.time_block,
            "session_state": context.session_state,
            "mode": context.mode or "unspecified",
            "energy_source": energy_source,
            "fatigue_adjustment": f"-{context.fatigue_score * 0.25:.3f} energy, -{context.fatigue_score * 20.0:.1f} tempo",
        },
    )
