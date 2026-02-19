from __future__ import annotations

from datetime import datetime

import pytest

from spotifygpt.diurnal import (
    AFTERNOON,
    EVENING,
    LATE_NIGHT,
    MORNING,
    NIGHT,
    get_feature_prior,
    get_time_block,
)


def test_weekday_boundary_0559_vs_0600():
    assert get_time_block(datetime(2024, 1, 3, 5, 59)) == LATE_NIGHT
    assert get_time_block(datetime(2024, 1, 3, 6, 0)) == MORNING


def test_weekday_boundaries_across_all_blocks():
    assert get_time_block(datetime(2024, 1, 3, 11, 59)) == MORNING
    assert get_time_block(datetime(2024, 1, 3, 12, 0)) == AFTERNOON
    assert get_time_block(datetime(2024, 1, 3, 16, 59)) == AFTERNOON
    assert get_time_block(datetime(2024, 1, 3, 17, 0)) == EVENING
    assert get_time_block(datetime(2024, 1, 3, 20, 59)) == EVENING
    assert get_time_block(datetime(2024, 1, 3, 21, 0)) == NIGHT


def test_weekend_shift_starts_one_hour_later():
    # Saturday morning starts at 07:00 instead of 06:00.
    assert get_time_block(datetime(2024, 1, 6, 6, 30)) == LATE_NIGHT
    assert get_time_block(datetime(2024, 1, 6, 7, 0)) == MORNING


def test_weekend_shift_evening_and_night_transition():
    # Saturday evening ends at 21:59 and night starts at 22:00.
    assert get_time_block(datetime(2024, 1, 6, 21, 59)) == EVENING
    assert get_time_block(datetime(2024, 1, 6, 22, 0)) == NIGHT


def test_feature_prior_is_deterministic_and_copy_safe():
    first = get_feature_prior(MORNING)
    second = get_feature_prior(MORNING)

    assert first == second

    first["energy"] = -1
    third = get_feature_prior(MORNING)
    assert third["energy"] != -1


def test_feature_prior_unknown_block_raises():
    with pytest.raises(ValueError, match="Unknown time block"):
        get_feature_prior("SUNRISE")
