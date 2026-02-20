from __future__ import annotations

import pytest

from spotifygpt.context_engine import ListeningContext, compute_recommendation_plan
from spotifygpt.diurnal import AFTERNOON, EVENING, LATE_NIGHT, MORNING, NIGHT
from spotifygpt.musical_dna import FeatureSummary, MusicalDNA
from spotifygpt.session_state import SessionState


def _dna() -> MusicalDNA:
    summary = FeatureSummary(count=10, mean=0.6, std=0.1, min=0.1, max=0.9, p10=0.2, p50=0.6, p90=0.8)
    tempo = FeatureSummary(count=10, mean=120.0, std=8.0, min=90.0, max=140.0, p10=100.0, p50=120.0, p90=130.0)
    return MusicalDNA(
        feature_summary={
            "danceability": summary,
            "energy": summary,
            "tempo": tempo,
            "valence": summary,
            "acousticness": summary,
            "instrumentalness": summary,
            "liveness": summary,
            "speechiness": summary,
        },
        tempo_bands=[],
        energy_dance_matrix={"low": {"low": 0, "med": 0, "high": 0}, "med": {"low": 0, "med": 0, "high": 0}, "high": {"low": 0, "med": 0, "high": 0}},
        taste_axes={"chill_to_hype": 0.5, "dark_to_happy": 0.5, "organic_to_synthetic": 0.5, "vocal_to_instrumental": 0.5},
        track_count=10,
    )


@pytest.mark.parametrize("state", [member.value for member in SessionState])
def test_compute_plan_covers_all_session_states(state: str):
    plan = compute_recommendation_plan(
        ListeningContext(
            time_block=MORNING,
            session_state=state,
            mode="TECHNO",
            dna_profile=_dna(),
        )
    )
    assert plan.explanation["session_state"] == state
    assert 0.0 <= plan.anchor_ratio <= 0.8


@pytest.mark.parametrize("time_block", [MORNING, AFTERNOON, EVENING, NIGHT, LATE_NIGHT])
def test_compute_plan_covers_all_time_blocks(time_block: str):
    plan = compute_recommendation_plan(
        ListeningContext(
            time_block=time_block,
            session_state=SessionState.NEUTRO.value,
            mode=None,
            dna_profile=_dna(),
        )
    )
    assert plan.explanation["time_block"] == time_block
    assert plan.target_tempo_range[0] <= plan.target_tempo_range[1]


def test_fatigue_reduces_energy_tempo_and_exploration():
    fresh = compute_recommendation_plan(
        ListeningContext(
            time_block=AFTERNOON,
            session_state=SessionState.CALIENTE.value,
            mode="URBANO",
            dna_profile=_dna(),
            fatigue_score=0.0,
        )
    )
    tired = compute_recommendation_plan(
        ListeningContext(
            time_block=AFTERNOON,
            session_state=SessionState.CALIENTE.value,
            mode="URBANO",
            dna_profile=_dna(),
            fatigue_score=1.0,
        )
    )

    assert tired.target_energy_range[0] < fresh.target_energy_range[0]
    assert tired.target_tempo_range[1] < fresh.target_tempo_range[1]
    assert tired.exploration_multiplier < fresh.exploration_multiplier


def test_energy_override_takes_precedence():
    base = compute_recommendation_plan(
        ListeningContext(
            time_block=EVENING,
            session_state=SessionState.NEUTRO.value,
            mode="RAP",
            dna_profile=_dna(),
            fatigue_score=0.5,
        )
    )
    forced = compute_recommendation_plan(
        ListeningContext(
            time_block=EVENING,
            session_state=SessionState.NEUTRO.value,
            mode="RAP",
            dna_profile=_dna(),
            fatigue_score=0.5,
            energy_override=0.9,
        )
    )

    assert forced.explanation["energy_source"] == "override"
    assert forced.target_energy_range[1] > base.target_energy_range[1]


def test_invalid_inputs_raise():
    with pytest.raises(ValueError, match="Unknown time_block"):
        compute_recommendation_plan(
            ListeningContext(
                time_block="SUNRISE",
                session_state=SessionState.NEUTRO.value,
                mode=None,
                dna_profile=_dna(),
            )
        )

    with pytest.raises(ValueError, match="fatigue_score"):
        compute_recommendation_plan(
            ListeningContext(
                time_block=MORNING,
                session_state=SessionState.NEUTRO.value,
                mode=None,
                dna_profile=_dna(),
                fatigue_score=2.0,
            )
        )
