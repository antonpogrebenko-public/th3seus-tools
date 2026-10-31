# log_validation

Validate the `hitl-sensors` noise models against **real** PX4 flight logs, and
calibrate the TH3SEUS simulator from them.

The HITL sim ships synthetic IMU/GPS/baro/mag noise (`hitl-sensors`). This tool
downloads public `.ulg` logs from PX4 flight-review, measures the real sensor
noise from each log's on-ground (stationary) segment, and compares it against
the model defaults so you can tell whether the simulated noise is realistic.
The calibration run (below) adds passes for the battery (internal resistance and
open-circuit curve), the vehicle response (control effectiveness, lag, hover command,
motor lag, in-flight gyro noise) and a comparison of TH3SEUS's predictions with real
logs of reference kits, and writes the ranges TH3SEUS randomizes over.

## What it validates

| Sensor | Model param | How it's measured |
|--------|-------------|-------------------|
| Gyro   | `gyro_noise_density` | first-diff white-noise std x sqrt(dt), stationary |
| Accel  | `accel_noise_density` | first-diff white-noise std x sqrt(dt), stationary |
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

- **GPS `delay_ms`**: needs motion cross-correlation against the EKF estimate.
- **Physics replay** (`hitl-physics`): the calibration run measures the battery and
  the vehicle response from logs, and compares TH3SEUS's predictions with them only
  for reference kits whose build is published; replaying a log through the physics
  would need the airframe's mass, props and inertia, which logs do not carry.

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
once. Where each count is reported:

- Manifest rows without a log, logs without a manifest row and logs over the cap:
  printed to stderr by `batch_analyze`, `response` and the battery extract
  (`battery_fit --extract-to`).
- Logs without a manifest row: also in the battery fit's JSON
  (`logs_without_manifest_row`) and markdown, and in the report's battery counts.
- All of them, with the failed downloads, in the report's Counts section, when
  `run-info.json` carries them as `inputs` (below).

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

`batch_analyze`, `response` and `battery_fit` take `--jobs N` to run the logs in N
worker processes (default 1); the output is the same for any N, and a log whose
worker dies is recorded as such without stopping the others.

Exit statuses, per command (a usage error is 2 for all of them):

| Command | 0 | 1 | 2 |
|---|---|---|---|
| `batch_analyze` | done | error: no such folder, bad manifest, private data refused | |
| `battery_fit` | done; target met | target missed, both files written | error |
| `response` | done | | error |
| `reference_airframes` | done | | error |
| `outputs merge` | done | | error |
| `calibration_report` | done | | error, private data refused |

`batch_analyze` keeps 1 for an error, as it had before these passes. `battery_fit`
exits with status 1 when the held-out median voltage error misses the 0.1 V per cell
target, even though it has written `battery.json` and `battery.md`; a script that
stops on any non-zero status must accept 1 there and go on.

Write a `run-info.json` with `date`, `tool_commit`, `parameters` (caps, gates,
cut-offs, seeds, N) and `commands`, and optionally `inputs`, the input counts, for
example `{"manifest_rows": N, "rows_without_log": N, "logs_without_row": N,
"capped_out": N, "download_failed": N}`. The report's Counts section shows `inputs`
as a table, or says that the input counts were not provided. Then build the
directory:

```bash
python -m log_validation.calibration_report --noise noise.json --battery battery.json \
    --response response.json --airframes airframes.json --run-info run-info.json \
    --out-dir calibration --forbid SOME_PRIVATE_STRING
```

A large corpus can run in chunks, for example one folder per batch of logs. For the
noise and response passes, run each chunk to its own JSON file and merge them (a log
ID in two parts is an error). Each chunk applies the per-vehicle cap to its own logs
only, so pass the manifest to the merge: it keeps the `--max-per-vehicle` smallest
log IDs of each vehicle over all the parts (default 3; give the cap the passes used),
keeps entries whose log has no manifest row, and prints how many entries it dropped:

```bash
python -m log_validation.outputs merge --manifest manifest.csv --max-per-vehicle 3 \
    -o noise.json noise-part1.json noise-part2.json
```

For the battery pass, extract each chunk with `--extract-to`, then fit once over all
the extract folders with `--obs-dir`:

```bash
python -m log_validation.battery_fit --log-dir CHUNK1 --extract-to obs1
python -m log_validation.battery_fit --log-dir CHUNK2 --extract-to obs2
python -m log_validation.battery_fit --obs-dir obs1 obs2 --manifest manifest.csv \
    --out-json battery.json --out-md battery.md
```

A successful extract run exits with status 0. The fit exits with status 1 when the
0.1 V per cell target is missed, after writing both files, as in the one-pass run above.

The calibration directory holds:

- `report.md`: the run (date, tool commit, parameters, commands), IMU noise before
  and after the estimator fix, the battery fit with both held-out checks, the response
  distributions, the reference-airframe comparison, a summary of the randomization
  ranges, the gaps TH3SEUS does not model, and the counts of logs analyzed, skipped
  and failed per pass with their reasons.
- `ranges.csv`: the p5, p25, p50, p75 and p95 of every quantity, overall and per
  battery cell count and airframe ID, with the numbers of logs and vehicles. A class
  needs at least 5 vehicles; the overall row is given whenever one vehicle has a value.
- `log-ids/noise.txt`, `log-ids/battery.txt`, `log-ids/response.txt`: the Flight
  Review log IDs behind each pass's numbers, one per line, sorted.
- `noise-results.json`, `battery-fit.json`, `response-results.json`, `airframes.json`,
  `run-info.json`: the inputs, normalized.
- `noise-profiles.json`: derived from `noise-results.json` (the `aggregate` profiles
  per MCU family, board and IMU chip, and over all logs); these are the values
  TH3SEUS is seeded from.

Every file is checked before it is written: the write is refused (exit status 2, and
nothing is written) when any output contains the home directory, the user or host
name, the folder of an input file, or a `--forbid` string. Only those are checked, so
pass `--forbid` for every other private string, such as a cloud account ID or a
bucket name. `--out-dir` must be new or empty. The Run date line of `report.md` is the
`date` of `run-info.json`, so the same inputs give the same files.

Deployment note: re-seeding the TH3SEUS profiles from this run lowers the simulated
IMU noise for every TH3SEUS user, because the old estimator overstated the per-sample
noise. The report's before and after table gives the measured factor in its current
and new columns, per MCU family; check them before deploying. Re-seeding also changes
the gyro bias sigma and the baro, mag and GPS parameters measured in the still window,
because the still window and the Allan fit now use `sensor_combined` (when the log has
it) rather than `sensor_gyro`; compare those profile values with the deployed ones too.

The kit table `reference_kits.json` gives each build value with the public page it
came from. A required value that no maker publishes is `null` with no source, and that
kit is listed as not compared, naming the missing sources; fill a value only from a
page that states it.

The committed run is in [`calibration/2026-10-31/`](calibration/2026-10-31/report.md):
8,671 public Flight Review quadrotor logs for IMU noise and the battery, and 953 logs
for the response pass (the pass refuses the 90 of them not logged at the rate
controller's rate), at most 3 logs per vehicle in each set. Its `run-info.json` gives
the parameters and commands.

## Tests

```bash
python -m pytest log_validation/tests -q
```

Tests cover the noise estimators, the battery fit, the response fits, the
reference-airframe comparison and the calibration report on synthetic logs and
signals with known parameters (no real logs required).

## Caveats

- Logs do not carry airframe specs. The noise validation covers **sensor noise**
  only; the calibration passes measure the battery and the vehicle response, and
  check TH3SEUS's flight dynamics only against reference kits with a published build.
- A clean noise floor needs an on-ground / pre-arm segment; logs that start
  mid-air are skipped with a note.
- Bias rows are order-of-magnitude (Gauss-Markov vs Allan signatures differ).
