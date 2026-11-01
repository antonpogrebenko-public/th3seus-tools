"""Which samples of a flight were recorded while the vehicle was disarmed.

The at-rest noise window must not include armed time: spinning propellers add
vibration that is not sensor noise. PX4 logs the arming state in two places;
`actuator_armed.armed` is preferred, `vehicle_status.arming_state` is the
fallback. Both are step signals, so the last message at or before a sample
gives its state. Right after a disarm the propellers still spin down, so the
first DISARM_SETTLE_S seconds after an armed-to-disarmed change do not count.
"""

from __future__ import annotations

import numpy as np

from . import ulog_loader as ul

TOPIC_ARMED = "actuator_armed"
TOPIC_STATUS = "vehicle_status"
# vehicle_status.arming_state: 0 = init, 1 = standby; 2 = armed and above.
DISARMED_STATES = frozenset({0, 1})
DISARM_SETTLE_S = 10.0
NO_ARMING_NOTE = "no arming state: neither 'actuator_armed' nor 'vehicle_status' in log"


def _held_state(
    msg_t_s: np.ndarray, values: np.ndarray, t_s: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    """Step-hold values at t_s; returns (held values, has a message at or before)."""
    idx = np.searchsorted(msg_t_s, np.asarray(t_s, float), side="right") - 1
    known = idx >= 0
    return values[np.where(known, idx, 0)], known


def _settled(msg_t_s: np.ndarray, disarmed: np.ndarray, t_s: np.ndarray) -> np.ndarray:
    """True where t_s is at least DISARM_SETTLE_S after the latest armed-to-disarmed change.

    `disarmed` is the per-message state. A series that starts disarmed has no
    change before its first armed message and loses nothing.
    """
    t_s = np.asarray(t_s, float)
    change_t_s = msg_t_s[1:][disarmed[1:] & ~disarmed[:-1]]
    idx = np.searchsorted(change_t_s, t_s, side="right") - 1
    latest = change_t_s[np.maximum(idx, 0)] if change_t_s.size else t_s
    return (idx < 0) | (t_s - latest >= DISARM_SETTLE_S)


def _disarmed(msg_t_s: np.ndarray, disarmed: np.ndarray, t_s: np.ndarray) -> np.ndarray:
    held, known = _held_state(msg_t_s, disarmed, t_s)
    return known & held & _settled(msg_t_s, disarmed, t_s)


def disarmed_mask(ulog, t_s: np.ndarray) -> np.ndarray | None:
    """Boolean per `t_s`: True where the vehicle was disarmed and has settled.

    Samples before the first arming message, and the first DISARM_SETTLE_S
    seconds after a disarm, are not disarmed. None when neither arming topic
    (with its field) is readable.
    """
    try:
        s = ul.series(ulog, TOPIC_ARMED, "armed")
        return _disarmed(s.t_s, s.values == 0, t_s)
    except KeyError:
        pass
    try:
        s = ul.series(ulog, TOPIC_STATUS, "arming_state")
    except KeyError:
        return None
    return _disarmed(s.t_s, np.isin(s.values, sorted(DISARMED_STATES)), t_s)
