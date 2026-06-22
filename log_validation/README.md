# log_validation

Validate the `hitl-sensors` noise models against **real** PX4 flight logs.

The HITL sim ships synthetic IMU/GPS/baro/mag noise (`hitl-sensors`). This tool
downloads public `.ulg` logs from PX4 flight-review, measures the real sensor
noise from each log's on-ground (stationary) segment, and compares it against
the model defaults so you can tell whether the simulated noise is realistic.

## What it validates

| Sensor | Model param | How it's measured |
|--------|-------------|-------------------|
| Gyro   | `gyro_noise_density` | first-diff white-noise std × √dt, stationary |
| Accel  | `accel_noise_density` | first-diff white-noise std × √dt, stationary |
| Gyro   | `gyro_bias_sigma` | Allan-deviation floor (approximate) |
| Baro   | `noise_sigma` | first-diff std of `baro_alt_meter`, stationary |
| Mag    | `noise_sigma_gauss` | first-diff std per axis, stationary |
| GPS    | `horizontal_noise_sigma` | std of on-ground local N/E position |
| GPS    | `altitude_noise_sigma` | std of on-ground GPS altitude |
| GPS    | `velocity_noise_sigma` | std of on-ground GPS velocity |
| GPS    | `update_rate_hz` | 1 / median timestamp interval |

Model defaults are mirrored from the Rust crate in `reference.py` (kept in sync
with `hitl-sensors/src/*.rs`).

### Not yet validated

- **GPS `delay_ms`** — needs motion cross-correlation against the EKF estimate.
- **Physics models** (`hitl-physics`) — out of scope here; physics replay needs
  the airframe's mass/prop/inertia, which logs do not carry.

## Setup

```bash
cd th3seus-tools
python -m pip install -r log_validation/requirements.txt
```

## Usage

```bash
# 1. Download a small batch of quadrotor logs (respect the rate limit)
python -m log_validation.download_logs --mav-type Quadrotor -n 10 \
    -d data/downloaded/

# 2. Validate the models against them
python -m log_validation data/downloaded/

# Inspect what topics/fields a specific log carries
python -m log_validation data/downloaded/<log_id>.ulg --list-topics
```

Output is a per-log table plus an aggregate (median across logs). Verdict band:
`ok` = measured within 1/3x..3x of model; `FLAG` = model likely mis-set.

## Tests

```bash
python -m pytest log_validation/tests -q
```

Tests cover the estimators on synthetic signals with known parameters (no real
logs required).

## Caveats

- Logs do not carry airframe specs, so this validates **sensor noise**, not
  flight dynamics.
- A clean noise floor needs an on-ground / pre-arm segment; logs that start
  mid-air are skipped with a note.
- Bias rows are order-of-magnitude (Gauss-Markov vs Allan signatures differ).
