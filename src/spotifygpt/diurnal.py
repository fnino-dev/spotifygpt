from __future__ import annotations

from datetime import datetime

MORNING = "MORNING"
AFTERNOON = "AFTERNOON"
EVENING = "EVENING"
NIGHT = "NIGHT"
LATE_NIGHT = "LATE_NIGHT"

_BLOCKS = (MORNING, AFTERNOON, EVENING, NIGHT, LATE_NIGHT)

# Weekday boundaries (inclusive start minute, inclusive end minute).
# Times are interpreted in local wall clock time from the input datetime.
_WEEKDAY_SCHEDULE: tuple[tuple[str, int, int], ...] = (
    (NIGHT, 0, 119),
    (LATE_NIGHT, 120, 359),
    (MORNING, 360, 719),
    (AFTERNOON, 720, 1019),
    (EVENING, 1020, 1259),
    (NIGHT, 1260, 1439),
)

# Weekend schedule shifts by +1 hour compared to weekdays.
_WEEKEND_SCHEDULE: tuple[tuple[str, int, int], ...] = (
    (NIGHT, 0, 179),
    (LATE_NIGHT, 180, 419),
    (MORNING, 420, 779),
    (AFTERNOON, 780, 1079),
    (EVENING, 1080, 1319),
    (NIGHT, 1320, 1439),
)

_FEATURE_PRIORS: dict[str, dict[str, float]] = {
    MORNING: {
        "energy": 0.62,
        "valence": 0.58,
        "danceability": 0.52,
        "acousticness": 0.42,
        "instrumentalness": 0.35,
    },
    AFTERNOON: {
        "energy": 0.68,
        "valence": 0.62,
        "danceability": 0.60,
        "acousticness": 0.30,
        "instrumentalness": 0.22,
    },
    EVENING: {
        "energy": 0.64,
        "valence": 0.50,
        "danceability": 0.66,
        "acousticness": 0.25,
        "instrumentalness": 0.18,
    },
    NIGHT: {
        "energy": 0.54,
        "valence": 0.44,
        "danceability": 0.58,
        "acousticness": 0.36,
        "instrumentalness": 0.28,
    },
    LATE_NIGHT: {
        "energy": 0.38,
        "valence": 0.34,
        "danceability": 0.40,
        "acousticness": 0.55,
        "instrumentalness": 0.52,
    },
}


def get_time_block(moment: datetime) -> str:
    """Map a datetime to a deterministic diurnal time block."""
    schedule = _WEEKEND_SCHEDULE if moment.weekday() >= 5 else _WEEKDAY_SCHEDULE
    minute_of_day = moment.hour * 60 + moment.minute
    for block, start_minute, end_minute in schedule:
        if start_minute <= minute_of_day <= end_minute:
            return block
    raise RuntimeError(f"Unsupported minute_of_day value: {minute_of_day}")


def get_feature_prior(block: str) -> dict[str, float]:
    """Return static feature priors for the provided time block."""
    if block not in _BLOCKS:
        valid = ", ".join(_BLOCKS)
        raise ValueError(f"Unknown time block '{block}'. Expected one of: {valid}")
    return dict(_FEATURE_PRIORS[block])
