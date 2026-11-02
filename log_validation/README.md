# log_validation

Validate the `hitl-sensors` noise models against **real** PX4 flight logs, and
calibrate the TH3SEUS simulator from them.

The HITL sim ships synthetic IMU/GPS/baro/mag noise (`hitl-sensors`). This tool
downloads public `.ulg` logs from PX4 flight-review, measures the real sensor
noise from each log's disarmed still window (the longest run of at least 3 s where the
vehicle is disarmed and the gyro is quiet), and compares it against
the model defaults so you can tell whether the simulated noise is realistic.
The calibration run (below) adds passes for the battery (internal resistance and
open-circuit curve), the vehicle response (control effectiveness, lag, hover command,
motor lag, in-flight gyro noise) and a comparison of TH3SEUS's predictions with real
logs of reference kits, and writes the ranges TH3SEUS randomizes over. It also builds
per-part tables (IMU chip, barometer chip, magnetometer chip, GPS receiver) and a board
table from the noise results, and writes the proposed values as Rust text for the
`hitl-physics` and `hitl-daemon` repositories.

## What it validates

| Sensor | Model param | How it's measured |
|--------|-------------|-------------------|
| Gyro   | `gyro_noise_density` | first-diff white-noise std x sqrt(dt), disarmed still window |
| Accel  | `accel_noise_density` | first-diff white-noise std x sqrt(dt), disarmed still window |
| Gyro   | `gyro_bias_sigma` | Allan-deviation floor (approximate) |
| Baro   | `noise_sigma` | first-diff std of `baro_alt_meter`, disarmed still window |
| Mag    | `noise_sigma_gauss` | first-diff std per axis, disarmed still window |
| GPS    | `horizontal_noise_sigma` | std of local N/E position, disarmed still window |
| GPS    | `altitude_noise_sigma` | std of GPS altitude, disarmed still window |
| GPS    | `velocity_noise_sigma` | std of GPS velocity, disarmed still window |
| GPS    | `update_rate_hz` | 1 / median timestamp interval |

Model defaults are mirrored from the Rust crate in `reference.py` (kept in sync
with `hitl-sensors/src/*.rs`).

### Per-part values in the noise pass

Existing per-log keys keep their meaning (the profile-seeding script reads them). The
noise pass adds, for the tables below:

- `parts.imu`, `parts.baro`, `parts.mag`, `parts.gps`: which part produced the samples
  each value was measured on, each with `name`, `devtype`, `device_id` and `source`
  (the board stays in `hardware.fc_board`). An unknown device type keeps its number and
  a name that marks it unknown. A calibration parameter that is read but holds no
  device ID adds an error such as `parts: CAL_GYRO0_ID is not a device ID`.
- `baro.chip_noise_sigma`, `baro.chip_rate_hz`, `baro.chip_allan_slope`,
  `baro.chip_resolution_m`: the raw barometer's own noise, rate, Allan slope and
  resolution. `baro.noise_sigma` still measures the fused altitude.
- `mag.rate_hz`, `mag.allan_slope`, `mag.resolution_gauss`, next to
  `mag.noise_sigma_gauss`. These seven values are computed over the sensor's new
  readings in the still window: a sample that only publishes the previous reading again
  is not counted. Where the topic has `timestamp_sample` (when the sensor took the
  sample), a sample is such a repeat when its sample time equals the previous sample's,
  so a genuine reading equal to the previous one still counts; without it, a sample
  equal to the previous one is taken as a repeat. `baro.chip_rate_hz` and `mag.rate_hz`
  are therefore the rate of new readings, not the rate the topic is logged at. The
  barometer's four values and the magnetometer's slope and resolution need 16 readings
  in the still window; with fewer, the entry gets an error giving the count (or saying
  the reading is constant). `mag.rate_hz` needs only two.
- `gps.fix_type_mode` (the most common fix type among samples with a 3D fix or better,
  extrapolated samples not counted) and, as medians over the samples with that fix
  type, `gps.eph_median_m`, `gps.epv_median_m`, `gps.speed_accuracy_median_m_s`: the
  receiver's own accuracy estimates. A log with no 3D-fix sample gets none of them. A
  sample whose estimate is 0 or less does not count (a receiver that does not report an
  estimate sends 0); when no sample is left, that key is missing and the entry's errors
  say the receiver reports no such accuracy.
- `gps.rest_spread_horizontal_m`, `gps.rest_spread_altitude_m`, `gps.rest_window_s`: the
  total spread of the GPS position over the still window, a cross-check of the
  receiver's estimates, and the window's length. They are recorded only when the still
  window lasts at least `--gps-rest-min-s` seconds (default 60). The two spreads are
  measured at rest, so they count toward the noise profiles and `log-ids/noise.txt`
  like the other values measured at rest; `gps.rest_window_s` does not. None of the
  three is a part-table column, as few logs rest that long.
- `param.IMU_GYRO_RATEMAX` and `param.IMU_GYRO_CUTOFF`, when the log has them.

GPS is read from `vehicle_gps_position`, or from `sensor_gps` when the log has no
`vehicle_gps_position`; `parts.gps.source` names the topic. The GPS accuracy keys
(`gps.fix_type_mode`, `gps.eph_median_m`, `gps.epv_median_m`,
`gps.speed_accuracy_median_m_s`) cover the whole log, not the still window, so a log
without a still window still gets them.

The noise profiles and `log-ids/noise.txt` keep holding only values measured at rest.
These keys do not count toward them: the `param.*` keys, the GPS fix type and accuracy
keys above, `gps.rest_window_s`, and the white-noise evidence of the part tables
(`baro.chip_rate_hz`, `baro.chip_allan_slope`, `baro.chip_resolution_m`, `mag.rate_hz`,
`mag.allan_slope`, `mag.resolution_gauss`). A log without a still window therefore
stays out of `log-ids/noise.txt`, as before.

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
# 1. IMU noise at rest (old and new estimator), part identities and per-part values
python -m log_validation.batch_analyze LOGS --manifest manifest.csv -o noise.json \
    [--gps-rest-min-s 60]

# 2. Battery resistance and open-circuit curve, with two held-out checks
python -m log_validation.battery_fit --log-dir LOGS --manifest manifest.csv \
    --out-json battery.json --out-md battery.md \
    [--capacity-bounds 1000,2500,5000,10000,20000]

# 3. Vehicle response: control effectiveness, lag, hover, motor lag, in-flight noise
python -m log_validation.response LOGS --manifest manifest.csv -o response.json

# 4. Compare TH3SEUS's predictions with real logs of the reference kits
python -m log_validation.reference_airframes response.json -o airframes.json

# 5. Per-part tables, board table and Rust text from the merged noise results
python -m log_validation.part_tables noise.json --out-dir parts --run-date YYYY-MM-DD \
    [--catalog-spellings spellings.json]
```

`battery_fit --capacity-bounds` takes the class bounds in mAh, comma separated (default
`1000,2500,5000,10000,20000`). Next to the spread per cell count, the fit reports the
internal resistance per cell for each cell count and pack-capacity class: the capacity
is `BAT1_CAPACITY`, else `BAT_CAPACITY`, and a log with an unset or non-positive
capacity falls in the "unknown capacity" class (a logged `BAT1_CAPACITY` of -1 or 0
wins over a `BAT_CAPACITY` value). A class with fewer than 5 vehicles is
left out. The emitted Rust constants stay per cell count.

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
| `part_tables` | done | | error, private data refused |
| `outputs merge` | done | | error |
| `calibration_report` | done | | error, private data refused |

`batch_analyze` keeps 1 for an error, as it had before these passes. `battery_fit`
exits with status 1 when the held-out median voltage error misses the 0.1 V per cell
target, even though it has written `battery.json` and `battery.md`; a script that
stops on any non-zero status must accept 1 there and go on.

Write a `run-info.json` with `date`, `tool_commit`, `parameters` (caps, gates,
cut-offs, seeds, N; also `--gps-rest-min-s`, `--capacity-bounds` and the `part_tables`
options, whether or not they are the defaults, for provenance) and `commands`, and
optionally `inputs`, the input counts, for example (each N a count):

```
{"manifest_rows": N, "rows_without_log": N, "logs_without_row": N, "capped_out": N,
 "download_failed": N}
```

The report's Counts section shows `inputs` as a table, or says that the input counts
were not provided. Then build the directory:

```bash
python -m log_validation.calibration_report --noise noise.json --battery battery.json \
    --response response.json --airframes airframes.json --run-info run-info.json \
    --parts parts --out-dir calibration --forbid SOME_PRIVATE_STRING
```

`--noise` and `--response` may also be gzipped (a name ending in `.gz`), as a
committed calibration directory holds them. `--parts` is optional: the `part_tables`
output folder. Without it the directory has no part tables and no
`log-ids/parts.txt`. The folder must have been built from these noise results: when
its `part-tables.json` names a log ID that `--noise` does not hold, the command exits
with 2 and writes nothing.

### Part tables

`part_tables` reads the merged noise results (a JSON list, or the same list gzipped
when the name ends in `.gz`), not raw logs. Options and defaults:

| Option | Default | Meaning |
|---|---|---|
| `--out-dir DIR` | required | must be new or empty |
| `--run-date YYYY-MM-DD` | required | written into the tables and the Rust comments |
| `--min-vehicles` | 10 | a value needs at least this many contributing vehicles |
| `--allan-slope-tolerance` | 0.15 | allowed distance of the Allan slope from -0.5 |
| `--sim-rate-baro`, `--sim-rate-mag` | 50, 50 | simulator output rate (Hz) per sensor |
| `--catalog-spellings FILE` | none | extra spellings for new variants (below) |

It writes, into `--out-dir`: `part-tables.json`, `board-table.json`, and
`rust/imu_chip.rs`, `rust/baro_chip.rs`, `rust/mag_chip.rs`, `rust/gps_chipset.rs`,
`rust/daemon_aliases.rs`. Each Rust file holds the complete enum and profile function
of one part type, in the structure of `hitl-physics` 0.13.1 `src/build.rs`;
`daemon_aliases.rs` holds, per parser, the match arms for `hitl-daemon` 0.27.0
(`parse_imu_chip`, `parse_baro_chip`, `parse_mag_chip`, `parse_gps_chipset`). The GPS
block is `if` lines to paste before the substring rules of `parse_gps_chipset`. A value
with a proposed number carries a comment with the old value, its source, the run date
and the vehicle count. The same inputs give byte-identical files, and all of them are
ASCII: the JSON files write any other character of the input text (a board, part or log
name) as a `\u` escape, and the pass refuses (exit 2) a new variant whose log name is not
printable ASCII, as the Rust text would hold it as it is.

The rules:

- Each row gives logs, vehicles, and the median, p25 and p75 over vehicle medians (a
  vehicle's median is over its own logs; a log without a vehicle ID is its own vehicle).
  A row gets a proposed value only with at least `--min-vehicles` vehicles. A row
  without one gives a reason, the first that applies: "no eligible log" (no log of the
  row has a finite value; for the barometer and magnetometer, none has one together
  with all three white-noise evidence values, the Allan slope, the resolution and a
  rate above 0), "too few logs pass the white-noise check" (barometer and magnetometer:
  the eligible logs' vehicles would reach the threshold), then "below the vehicle
  threshold". The report shows it in its "No proposal because" column; on a barometer
  or magnetometer row without a proposal whose logs were checked, the white-noise
  failure counts follow the reason. A row with a proposal shows no counts there, but
  `part-tables.json` carries `checked`, `failed_slope` and `failed_resolution` on every
  row (0 on the IMU and GPS rows, which are not checked).
- Parts map to TH3SEUS entries through a pinned mirror of the two repositories' tables
  and parsers (`th3seus_parts.py`). A part with no entry gets its own row as a candidate
  new variant, whose current value is the `Other` entry's. Its name is the log name split
  at each run of characters other than letters and digits, each piece capitalized
  (`EMLID_REACH` gives `EmlidReach`); the pass refuses (exit 2) a proposed variant whose
  name is not a usable Rust variant, such as one from a log name `OTHER` or `1ABC`.
- Barometer and magnetometer: a log counts toward a row only when its Allan slope is
  within `--allan-slope-tolerance` of -0.5 and its per-sample value is at least its
  resolution (the white-noise check). The row says how many logs failed each condition.
- Barometer and magnetometer values are converted with
  `value * sqrt(sim rate / log rate)`, using each log's own rate, when that rate is at
  least the simulator rate. The log rate is `baro.chip_rate_hz` or `mag.rate_hz`: the
  rate of new readings in the still window (a re-published sample is not counted, by
  its sample time where the log has one), not the rate the topic is logged at. A log
  below the simulator rate keeps its value (factor 1): the PX4 logger keeps one sample
  per interval instead of averaging, so a slowly logged series (raw barometer and
  magnetometer topics are often logged at about 1 Hz) holds samples at the driver's own,
  unknown rate. The row shows the median log rate and factor, and flags a median factor
  above 3 or below 1/3.
- The `Other` entry pools, per vehicle, every non-excluded part of the type that passes
  the rules above. Generic names (such as `UAVCAN`, `UBX`, `NMEA`) and unidentified
  parts count only toward `Other`; simulated parts count nowhere.
- GPS rows use the receiver's own accuracy estimates and update rate. A log counts toward
  a GPS variant only when its `gps.fix_type_mode` is the fix class TH3SEUS models for
  it: RTK fixed for `UbloxF9P` and `SeptentrioMosaic`, standalone 3D for every other
  variant. It counts toward `Other` only when its fix-type mode is 3 (standalone 3D).
  A log whose mode is RTK fixed (6) therefore counts only toward a variant of the
  rtk_fixed class (`UbloxF9P`, `SeptentrioMosaic`), and a log with any other mode (for
  example 4 or 5, DGPS or RTK float) or no mode counts toward no GPS row.
- `board-table.json` is `{"boards": {BOARD: {"logs", "vehicles", "imu", "baro", "mag",
  "imu_gyro_ratemax_median", "imu_gyro_cutoff_median"}}}`; the `imu`, `baro` and `mag`
  maps give each chip's share of the board's vehicles (each vehicle's most common chip).

The catalog spellings file is `{"imu": {"<spelling>": "<PX4 log name>"}, "baro": {...},
"mag": {...}, "gps": {...}}`; the spellings are emitted, normalized, next to the log names
of the new variants. Log names are compared after normalizing (so `icp-201xx` matches
`ICP201XX`), and a spelling that is empty after normalizing (such as `-`) is skipped with
a comment. Every spelling and log name in the file must be printable ASCII without `"`
and `\`, as the Rust text holds them. The pass exits with 2 and writes nothing for a bad
option, an unreadable, corrupt or malformed input, or private data in an output. It
guards against
the home directory, user and host name and the input folders; there is no `--forbid`
option, so run `calibration_report --forbid` over the result for other private strings.

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
  and failed per pass with their reasons. A log counts as analyzed when it is behind the
  pass's numbers (listed in `log-ids/`); any other log counts as failed when one of its
  errors starts with a step label followed by `failed: ` or `unavailable: ` (such as
  `analysis failed: ValueError`), or with `worker process died`, and as skipped otherwise.
- `ranges.csv`: the p5, p25, p50, p75 and p95 of every quantity, overall and per
  battery cell count and airframe ID, with the numbers of logs and vehicles. A class
  needs at least 5 vehicles; the overall row is given whenever one vehicle has a value.
- `log-ids/noise.txt`, `log-ids/battery.txt`, `log-ids/response.txt`: the Flight
  Review log IDs behind each pass's numbers, one per line, sorted.
- `noise-results.json`, `battery-fit.json`, `response-results.json`, `airframes.json`,
  `run-info.json`: the inputs, normalized. A per-log result file whose JSON text is
  larger than 10 MiB (`noise-results.json`, `response-results.json`) is written as
  `noise-results.json.gz` / `response-results.json.gz` instead: deterministic gzip
  (compression level 9, modification time 0, no file name), so the same inputs give
  the same bytes with the same Python and zlib (the header is fixed; another zlib
  build may compress the same text differently, while the decompressed text stays
  the same). Decompress before the profile-seeding script reads them
  (`gunzip -k noise-results.json.gz`). `part_tables` and `calibration_report` read the
  `.gz` directly. The report, lists, ranges, profiles and tables always stay plain.
- With `--parts`: `part-tables.json`, `board-table.json` and `rust/imu_chip.rs`,
  `rust/baro_chip.rs`, `rust/mag_chip.rs`, `rust/gps_chipset.rs`,
  `rust/daemon_aliases.rs`, copied from the `part_tables` folder; `part-tables.json` is
  copied without its `log_ids` key, which `log-ids/parts.txt` holds (at the full corpus
  the list is several MB). `log-ids/parts.txt` names the logs behind a proposed part
  value only (a log whose values were all refused is not in it). And a part-tables
  section in `report.md` (every row with its current and proposed value, the reason when
  there is none, the barometer and magnetometer rates and factors, the boards with the
  most vehicles).
- `rust/battery_tables.rs` (always): the same text `battery_fit --emit-rust` prints.
- `noise-profiles.json`: derived from `noise-results.json` (the `aggregate` profiles
  per MCU family, board and IMU chip, and over all logs); these are the values
  TH3SEUS is seeded from.

Every file is checked before it is written: the write is refused (exit status 2, and
nothing is written) when any output contains the home directory, the user or host
name, the folder of an input file, or a `--forbid` string. Only those are checked, so
pass `--forbid` for every other private string, such as a cloud account ID or a
bucket name. `--out-dir` (here and for `part_tables`) must be new or an empty folder
(a symlink to one works). Every output is first written to a temporary folder. An
existing empty folder is then filled in place and keeps its permissions; if that copy
fails, the files it made are removed again. A new folder is created by renaming a
complete hidden copy (`.<name>.<random>`) made beside it, so its parent must be
writable; a run killed before the rename can leave that hidden folder behind: delete
it. The Run date line of `report.md` is the `date` of `run-info.json`, so the same
inputs give the same files. Every JSON output of every pass is ASCII: a character
outside ASCII in the input text is written as a `\u` escape (the check above looks at
the text before escaping, so a private string with such a character is still found).

Deployment note: re-seeding the TH3SEUS profiles from this run lowers the simulated
IMU noise for every TH3SEUS user, because the old estimator overstated the per-sample
noise. The report's before and after table gives the measured factor in its current
and new columns, per MCU family; check them before deploying. Re-seeding also changes
the gyro bias sigma and the baro, mag and GPS parameters measured in the still window,
because the still window and the Allan fit now use `sensor_combined` (when the log has
it) rather than `sensor_gyro`; compare those profile values with the deployed ones too.

The at-rest values (the noise densities, the gyro bias sigma, and the baro, mag and GPS
values measured in the still window) now exclude armed and hover windows, which
measured propeller vibration instead of sensor noise. A log without arming topics gives
no at-rest values and its entry has the error `imu.stationary_window: no arming state:
neither 'actuator_armed' nor 'vehicle_status' in log`. A log with no disarmed still
window gives none either, with the error `imu.stationary_window: no disarmed quiet >=3s
run`. The first 10 s after a disarm are left out, because the propellers are still
spinning down. Fewer logs therefore give at-rest values than before.

The kit table `reference_kits.json` gives each build value with the public page it
came from. A required value that no maker publishes is `null` with no source, and that
kit is listed as not compared, naming the missing sources; fill a value only from a
page that states it.

The committed run is in [`calibration/2026-11-02/`](calibration/2026-11-02/report.md):
35,968 public Flight Review logs of all vehicle types for IMU noise, the per-part values
and the battery, and 1,355 quadrotor logs for the response pass (the pass refuses the
130 of them not logged at the rate controller's rate), at most 3 logs per vehicle in
each set. Its `run-info.json` gives the parameters and commands. Its at-rest values come
from disarmed still windows only, so 3,671 of the noise logs give them. Its Counts
section was rendered with the earlier failure rule, which also counted a failure word
anywhere in a note: it shows 27 failed noise logs, 26 under the current rule.

## Tests

```bash
python -m pytest log_validation/tests -q
```

Tests cover the noise estimators, the part identification and per-part values, the part
tables and their Rust text, the battery fit, the response fits, the
reference-airframe comparison and the calibration report on synthetic logs and
signals with known parameters (no real logs required).

## Caveats

- Logs do not carry airframe specs. The noise validation covers **sensor noise**
  only; the calibration passes measure the battery and the vehicle response, and
  check TH3SEUS's flight dynamics only against reference kits with a published build.
- At-rest values need a disarmed quiet run of at least 3 s that starts at least 10 s
  after any disarm; a log recorded only while armed (PX4's default, `SDLOG_MODE` 0:
  from arming to disarm) gets none.
- Bias rows are order-of-magnitude (Gauss-Markov vs Allan signatures differ).
