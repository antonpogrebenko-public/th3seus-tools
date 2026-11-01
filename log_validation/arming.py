"""Which samples of a flight were recorded while the vehicle was disarmed.

The at-rest noise window must not include armed time: spinning propellers add
vibration that is not sensor noise. PX4 logs the arming state in two places;
`actuator_armed.armed` is preferred, `vehicle_status.arming_state` is the
fallback. Both are step signals, so the last message at or before a sample
gives its state.
"""

from __future__ import annotations

import numpy as np

from . import ulog_loader as ul

TOPIC_ARMED = "actuator_armed"
TOPIC_STATUS = "vehicle_status"
# vehicle_status.arming_state: 0 = init, 1 = standby; 2 = armed and above.
DISARMED_STATES = frozenset({0, 1})
NO_ARMING_NOTE = "no arming state: neither 'actuator_armed' nor 'vehicle_status' in log"


def _held_state(msg_t_s: np.ndarray, values: np.ndarray, t_s: np.ndarray):
    """Step-hold values at t_s; returns (held values, has a message at or before)."""
    idx = np.searchsorted(msg_t_s, np.asarray(t_s, float), side="right") - 1
    known = idx >= 0
    return values[np.where(known, idx, 0)], known


def disarmed_mask(ulog, t_s: np.ndarray) -> np.ndarray | None:
    """Boolean per `t_s`: True where the vehicle was disarmed.

    Samples before the first arming message are not disarmed. None when
    neither arming topic (with its field) is readable.
    """
    try:
        s = ul.series(ulog, TOPIC_ARMED, "armed")
        held, known = _held_state(s.t_s, s.values, t_s)
        return known & (held == 0)
    except KeyError:
        pass
    try:
        s = ul.series(ulog, TOPIC_STATUS, "arming_state")
    except KeyError:
        return None
    held, known = _held_state(s.t_s, s.values, t_s)
    return known & np.isin(held, sorted(DISARMED_STATES))
