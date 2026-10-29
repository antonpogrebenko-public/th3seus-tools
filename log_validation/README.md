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

## Calibration run

This calibrates the TH3SEUS simulator from public PX4 flight logs and writes the
result as a directory that can be reviewed and committed. Every input is a folder of
`.ulg` files named by their Flight Review log ID. Outputs name logs only by that ID
and vehicles only by the manifest's vehicle ID, never by a path, user or host name.

The manifest is an optional CSV with the columns `log_id` and `vehicle_id`. With it,
logs not listed are skipped, at most `--max-per-vehicle` logs (default 3, the
smallest IDs) are kept per vehicle, and the split and the ranges count each vehicle
once. The count of logs without a manifest row is reported.

The passes, each reading the same folder and manifest:

```bash
# 1. IMU noise at rest (old and new estimator)
python -m log_validation.batch_analyze LOGS --manifest manifest.csv -o noise.json

# 2. Battery resistance and open-circuit curve, with two held-out checks
python -m log_validation.battery_fit --log-dir LOGS --manifest manifest.csv \
    --out-json battery.json --out-md battery.md

# 3. Vehicle response: control effectiveness, lag, hover, motor lag, in-flight noise
python -m log_validation.response LOGS --manifest manifest.csv -o response.json

# 4. Compare TH3SEUS's predictions with real logs of the reference kits
python -m log_validation.reference_airframes response.json -o airframes.json
```

Write a `run-info.json` with `date`, `tool_commit`, `parameters` (caps, gates,
cut-offs, seeds, N) and `commands`, then build the directory:

```bash
python -m log_validation.calibration_report --noise noise.json --battery battery.json \
    --response response.json --airframes airframes.json --run-info run-info.json \
    --out-dir calibration --forbid SOME_PRIVATE_STRING
```

A large corpus can run in chunks, for example one folder per batch of logs. For the
noise and response passes, run each chunk to its own JSON file and merge them (a log
ID in two parts is an error):

```bash
python -m log_validation.outputs merge -o noise.json noise-part1.json noise-part2.json
```

For the battery pass, extract each chunk with `--extract-to`, then fit once over all
the extract folders with `--obs-dir`:

```bash
python -m log_validation.battery_fit --log-dir CHUNK1 --extract-to obs1
python -m log_validation.battery_fit --log-dir CHUNK2 --extract-to obs2
python -m log_validation.battery_fit --obs-dir obs1 obs2 --manifest manifest.csv \
    --out-json battery.json --out-md battery.md
```

The calibration directory holds:

- `report.md`: the run (date, tool commit, parameters, commands), IMU noise before
  and after the estimator fix, the battery fit with both held-out checks, the response
  distributions, the reference-airframe comparison, a summary of the randomization
  ranges, the gaps TH3SEUS does not model, and the counts of logs analyzed, skipped
  and failed per pass with their reasons.
- `ranges.csv`: the p5, p25, p50, p75 and p95 of every quantity, overall and per
  battery cell count and airframe ID, with the numbers of logs and vehicles.
- `log-ids/noise.txt`, `log-ids/battery.txt`, `log-ids/response.txt`: the Flight
  Review log IDs behind each pass's numbers, one per line, sorted.
- `noise-results.json`, `noise-profiles.json`, `battery-fit.json`,
  `response-results.json`, `airframes.json`, `run-info.json`: the inputs, normalized.

Every file is checked before it is written: the write is refused (exit status 2, and
nothing is written) when any output contains the home directory, the user or host
name, the folder of an input file, or a `--forbid` string. Only those are checked, so
pass `--forbid` for every other private string, such as a cloud account ID or a
bucket name. `--out-dir` must be new or empty. The same inputs give the same files,
except the Run date line of `report.md`.

Deployment note: re-seeding the TH3SEUS profiles from this run lowers the simulated
IMU noise for every TH3SEUS user, because the old estimator overstated the per-sample
noise. The report's before and after table gives the measured factor in its current
and new columns, per MCU family; check them before deploying.

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
