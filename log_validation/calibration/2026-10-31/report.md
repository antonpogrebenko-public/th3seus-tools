# TH3SEUS calibration from public PX4 flight logs

## Run

- Run date: 2026-10-31
- Tool commit: 93e1645546a1a3669f59f3ca6abd0c35894e8b44
- Parameters:
  - `batches`: "9 batches of up to 20 GB; each batch folder holds the logs of both sets, so the noise and battery passes skip the logs only in the response set (133 in all) as logs without a manifest row, and the response pass skips the logs only in the noise and battery set (7,851 in all)"
  - `battery_holdout_frac`: 0.2
  - `battery_split_seed`: 20260623
  - `jobs`: 8
  - `local_tool_commit`: "93e1645546a1a3669f59f3ca6abd0c35894e8b44"
  - `max_per_vehicle`: 3
  - `noise_and_battery_set`: "the logs that the rate-loop data screen kept as full-rate, reduced-rate or scenario logs"
  - `reference_min_vehicles`: 5
  - `reproduce`: "the rate-loop data screen and the step that builds the manifests are not public; every entry of noise-results.json and response-results.json carries its Flight Review log ID and hashed vehicle ID, so the two manifests can be rebuilt from them, and log-ids/ lists the logs behind each pass's numbers"
  - `response_gates`: {"min_first_stage_r2": 0.05, "min_motor_cmd_std": 0.02, "min_motor_r2": 0.5, "min_r2": 0.1, "min_torque_std": 0.002, "noise_cutoff_hz": 20.0}
  - `response_min_logged_rate_hz`: "200 with 5 % tolerance, on the torque setpoint, rate and rate setpoint"
  - `response_set`: "the logs that the rate-loop data screen kept as full-rate; the response pass checks the logged rates again and refuses the logs not logged at the rate controller's rate (see Counts)"
  - `selection`: "quadrotor logs that the rate-loop data screen marked as real flights; duplicate uploads removed; at most 3 logs per vehicle (the smallest log IDs); vehicle IDs hashed; the passes found further simulation logs among them (SYS_HITL set, or a simulated sensor or board) and skipped them (see Counts)"
  - `source`: "public PX4 Flight Review logs, read from a mirror; raw logs never left the cloud runner"

Commands:

```
# In the cloud, per batch NNNN (LOGS/ holds the batch's downloaded logs and is deleted after the batch):
python -m log_validation.batch_analyze LOGS/ -o noise-NNNN.json --manifest manifest-noise.csv --jobs 8
python -m log_validation.battery_fit --log-dir LOGS/ --extract-to battery-obs/ --manifest manifest-noise.csv --jobs 8
python -m log_validation.response LOGS/ -o response-NNNN.json --manifest manifest-response.csv --jobs 8
# The runner keeps noise-NNNN.json and response-NNNN.json as RUN/parts/ and packs the files of battery-obs/ (by name, no folders) into RUN/battery-obs/NNNN.tar:
(cd battery-obs/ && tar -cf ../NNNN.tar -- *)
# Locally, on the downloaded run folder RUN/ (manifests, parts/ and battery-obs/*.tar; no raw logs):
python -m log_validation.outputs merge -o noise-results.json --manifest RUN/manifest-noise.csv RUN/parts/noise-*.json
python -m log_validation.outputs merge -o response-results.json --manifest RUN/manifest-response.csv RUN/parts/response-*.json
mkdir -p battery-obs/
for t in RUN/battery-obs/*.tar; do tar -xf "$t" -C battery-obs/; done
python -m log_validation.battery_fit --obs-dir battery-obs/ --manifest RUN/manifest-noise.csv --out-json battery-fit.json --out-md battery-fit.md
python -m log_validation.reference_airframes response-results.json --kits log_validation/reference_kits.json -o airframes.json --min-vehicles 5
python -m log_validation.calibration_report --noise noise-results.json --battery battery-fit.json --response response-results.json --airframes airframes.json --run-info run-info.json --out-dir log_validation/calibration/2026-10-31 --forbid PRIVATE
```

## IMU noise before and after

Median noise density at rest per MCU family. The current column is the estimator TH3SEUS uses today; the new column is the fixed estimator. Both medians are taken over the same logs, those with both values; Logs is their number. Per-sample noise is density times sqrt(400), the noise one 400 Hz tick adds.

Gyro, rad/s/sqrt(Hz); per sample rad/s:

| MCU family | Logs | Current (TH3SEUS estimator) | New | Current / new | Current per sample at 400 Hz | New per sample at 400 Hz |
|---|---|---|---|---|---|---|
| STM32F4 | 112 | 0.004162 | 0.001101 | 3.78 | 0.08324 | 0.02202 |
| STM32F7 | 265 | 0.03825 | 0.001859 | 20.6 | 0.765 | 0.03719 |
| STM32H7 | 6448 | 0.02914 | 0.001138 | 25.6 | 0.5828 | 0.02277 |
| ALL | 6973 | 0.02901 | 0.001166 | 24.9 | 0.5802 | 0.02331 |

Accelerometer, m/s^2/sqrt(Hz); per sample m/s^2:

| MCU family | Logs | Current (TH3SEUS estimator) | New | Current / new | Current per sample at 400 Hz | New per sample at 400 Hz |
|---|---|---|---|---|---|---|
| STM32F4 | 111 | 0.03994 | 0.01091 | 3.66 | 0.7989 | 0.2183 |
| STM32F7 | 265 | 1.036 | 0.04853 | 21.3 | 20.72 | 0.9706 |
| STM32H7 | 6450 | 0.7924 | 0.03554 | 22.3 | 15.85 | 0.7107 |
| ALL | 6974 | 0.7829 | 0.03562 | 22 | 15.66 | 0.7123 |

## Battery

Internal resistance per cell, from current steps. Deployed is the value the simulator uses.

| Cells | Median mohm | p10 | p90 | Deployed mohm | Steps | Vehicles | Deployed from |
|---|---|---|---|---|---|---|---|
| 1S | 20.15 | 12.88 | 36.79 | 20.15 | 11619 | 108 | own fit |
| 2S | 5.95 | 2.19 | 10.70 | 5.95 | 1210 | 18 | own fit |
| 3S | 7.16 | 1.91 | 26.60 | 7.16 | 6773 | 131 | own fit |
| 4S | 3.91 | 1.10 | 16.67 | 3.12 | 47326 | 752 | pooled median: implausible (median below the 4 mohm/cell floor) |
| 5S | 8.07 | 1.24 | 23.92 | 8.07 | 733 | 20 | own fit |
| 6S | 3.03 | 1.22 | 10.42 | 3.12 | 136197 | 1082 | pooled median: implausible (median below the 4 mohm/cell floor) |
| 8S | 3.28 | 1.12 | 10.51 | 3.12 | 14967 | 43 | pooled median: implausible (median below the 4 mohm/cell floor) |
| 12S | 2.48 | 1.42 | 4.38 | 3.12 | 47606 | 95 | pooled median: implausible (median below the 4 mohm/cell floor) |
| 14S | 3.36 | 1.10 | 9.25 | 3.12 | 10847 | 13 | pooled median: implausible (median below the 4 mohm/cell floor) |

Pooled median: 3.12 mohm per cell over 277278 steps.

Held-out checks:

| Check | Value |
|---|---|
| Held-out logs with a voltage error (MAE) | 1148 of 1730 real held-out logs, usable or not |
| Voltage MAE, median (V per cell) | 0.08789 |
| Voltage MAE, p75 (V per cell) | 0.1622 |
| Target of 0.1 V per cell | met |
| Step check: steps (vehicles) | 172073 (564) |
| Step check: median error (V per cell) | 0.03753 |
| Step check: p75 error (V per cell) | 0.09596 |

## Vehicle response

Overall distributions over vehicle medians (each vehicle counts once). Effectiveness is in motor-command space; lag is the body-rate lag minus the log's filter delay, an upper bound on motor lag.

| Quantity | Logs | Vehicles | p5 | p25 | p50 | p75 | p95 |
|---|---|---|---|---|---|---|---|
| effectiveness.pitch | 288 | 194 | 77.24 | 141.6 | 205.4 | 342.2 | 610.7 |
| effectiveness.roll | 293 | 190 | 84.75 | 148.4 | 224.6 | 357.7 | 736.2 |
| effectiveness.yaw | 27 | 23 | 16.85 | 51.81 | 77.73 | 239.8 | 585.2 |
| hover.command | 680 | 361 | 0.2102 | 0.3546 | 0.4457 | 0.5338 | 0.6974 |
| inflight_gyro_noise.pitch | 674 | 358 | 0.005617 | 0.01304 | 0.02563 | 0.04957 | 0.1461 |
| inflight_gyro_noise.roll | 674 | 358 | 0.00705 | 0.01509 | 0.02833 | 0.05546 | 0.2063 |
| inflight_gyro_noise.yaw | 674 | 358 | 0.002927 | 0.007017 | 0.01452 | 0.02728 | 0.104 |
| lag_minus_filter_s.pitch | 307 | 205 | 0.007068 | 0.01732 | 0.02363 | 0.03842 | 0.06158 |
| lag_minus_filter_s.roll | 314 | 201 | 0.007068 | 0.01704 | 0.02242 | 0.03527 | 0.06046 |
| lag_minus_filter_s.yaw | 27 | 23 | 0.001956 | 0.0075 | 0.01258 | 0.02218 | 0.06103 |
| motor_lag_s | 75 | 42 | 0.0231 | 0.03944 | 0.056 | 0.071 | 0.1082 |
| rate.torque_setpoint_hz | 953 | 458 | 399.3 | 665.3 | 730.8 | 800 | 819 |

## Reference airframes

A kit is compared when it has at least 5 vehicles. Each kit is compared on the logs of `log-ids/response.txt` whose airframe ID is the kit's. Ratio is the TH3SEUS prediction over the real median.

### DJI F450 (airframe 4011, 9 vehicles)

not compared: no source for motor_kv; no source for motor_weight_g; no source for esc_continuous_amps; no source for prop_diameter_in; no source for prop_pitch_in; no source for prop_blade_count; no source for battery_weight_g

### Holybro S500 (airframe 4015, 5 vehicles)

not compared: no source for frame_weight_g; no source for motor_kv; no source for motor_weight_g; no source for esc_continuous_amps; no source for prop_blade_count; no source for battery_cells; no source for battery_capacity_mah; no source for battery_c_rating; no source for battery_weight_g; no source for fc_weight_g

### Holybro X500 V2 (airframe 4019, 12 vehicles)

not compared: no source for frame_weight_g; no source for motor_weight_g; no source for prop_blade_count; no source for battery_capacity_mah; no source for battery_c_rating; no source for battery_weight_g

### BetaFPV Beta75X 2S (airframe 4041, 12 vehicles)

not compared: no source for motor_weight_g; no source for prop_pitch_in

Recommendations:

- thrust basis K_CT (reported, never changed) (hover.command): no change. 0 compared kits have data; a hover throttle discrepancy would be reported for the thrust basis K_CT, which is never recommended for change
- motor lag tau = 0.02 + 15/KV (lag): no change. no change recommended: 0 compared kits have data, at least 2 are needed
- inertia and arm-length formulas (effectiveness): no change. no change recommended: 0 compared kits have data, at least 2 are needed

## Randomization ranges

The full table, with the cell-count and airframe classes, is in `ranges.csv`. Overall rows:

| Quantity | Logs | Vehicles | p5 | p25 | p50 | p75 | p95 | Class rows |
|---|---|---|---|---|---|---|---|---|
| accel.accel_noise_density | 7038 | 3856 | 0.002136 | 0.01368 | 0.03588 | 0.06976 | 0.1759 | 16 |
| battery.r_mohm_per_cell | 4162 | 2221 | 1.52 | 2.934 | 5.048 | 10.62 | 25.33 | 9 |
| effectiveness.pitch | 288 | 194 | 77.24 | 141.6 | 205.4 | 342.2 | 610.7 | 9 |
| effectiveness.roll | 293 | 190 | 84.75 | 148.4 | 224.6 | 357.7 | 736.2 | 9 |
| effectiveness.yaw | 27 | 23 | 16.85 | 51.81 | 77.73 | 239.8 | 585.2 | 2 |
| gyro.gyro_noise_density | 7038 | 3856 | 0.0002095 | 0.0005236 | 0.001168 | 0.002437 | 0.005888 | 16 |
| hover.command | 680 | 361 | 0.2102 | 0.3546 | 0.4457 | 0.5338 | 0.6974 | 13 |
| inflight_gyro_noise.pitch | 674 | 358 | 0.005617 | 0.01304 | 0.02563 | 0.04957 | 0.1461 | 13 |
| inflight_gyro_noise.roll | 674 | 358 | 0.00705 | 0.01509 | 0.02833 | 0.05546 | 0.2063 | 13 |
| inflight_gyro_noise.yaw | 674 | 358 | 0.002927 | 0.007017 | 0.01452 | 0.02728 | 0.104 | 13 |
| lag_minus_filter_s.pitch | 307 | 205 | 0.007068 | 0.01732 | 0.02363 | 0.03842 | 0.06158 | 9 |
| lag_minus_filter_s.roll | 314 | 201 | 0.007068 | 0.01704 | 0.02242 | 0.03527 | 0.06046 | 9 |
| lag_minus_filter_s.yaw | 27 | 23 | 0.001956 | 0.0075 | 0.01258 | 0.02218 | 0.06103 | 2 |
| motor_lag_s | 75 | 42 | 0.0231 | 0.03944 | 0.056 | 0.071 | 0.1082 | 3 |
| rate.torque_setpoint_hz | 953 | 458 | 399.3 | 665.3 | 730.8 | 800 | 819 | 13 |

## Gaps TH3SEUS does not model

- Vibration: not modelled. In flight, the median gyro noise (the angular velocity minus its moving average) is 0.02301 rad/s per sample at a median 783.7 Hz. On the density basis (per-sample std times sqrt(1 / rate), which treats the signal as white) that is 0.0009519 rad/s/sqrt(Hz), against 0.001165 at rest: a ratio of 0.817 (674 flight logs, 7038 at-rest logs). The ratio is biased low: the logged in-flight signal is low-passed at IMU_GYRO_CUTOFF, usually well below half the logged rate, while it is divided by sqrt(logged rate). The two bases are not exactly comparable, so both are given.
- Wind: not modelled. These passes do not measure it.
- Ground effect: not modelled. These passes do not measure it.
- Motor-to-motor differences: not modelled. The motor lag is a median over the ESCs of a log, so the spread between motors is not kept.
- ESC dynamics: not modelled beyond a first-order lag with delay. Measured motor lag: median 0.056 s, p5 to p95 0.0231 to 0.1082 s over 42 vehicles.

## Counts

### Inputs

| Input | Count |
|---|---|
| Manifest rows | 9624 |
| Manifest rows without a log | 0 |
| Failed downloads | 0 |
| candidates_noise_and_battery | 29091 |
| candidates_response | 2988 |
| capped_out_noise_and_battery | 12461 |
| capped_out_response | 1336 |
| copies_removed_noise_and_battery | 7959 |
| copies_removed_response | 699 |
| logs_downloaded | 8804 |
| logs_in_both_sets | 820 |
| logs_only_in_noise_and_battery_set | 7851 |
| logs_only_in_response_set | 133 |
| selected_noise_and_battery | 8671 |
| selected_response | 953 |

### Noise

Analyzed 7038, skipped 1633, failed 0.

| Reason (details removed, numbers as N) | Logs |
|---|---|
| gps.horizontal_noise_sigma: "topic 'vehicle_gps_position' not in log." | 2612 |
| imu.stationary_window: no quiet >=Ns run | 1553 |
| mag.noise_sigma_gauss: "topic 'sensor_mag' not in log." | 911 |
| baro.noise_sigma: "topic 'vehicle_air_data' not in log." | 460 |
| simulation log skipped | 78 |
| gps.horizontal_noise_sigma: too few GPS samples on ground | 33 |
| mag.noise_sigma_gauss: need at least N samples | 20 |
| gyro.legacy_noise_density: need at least N timestamps | 15 |
| accel.legacy_noise_density: need at least N timestamps | 12 |
| gyro.legacy_noise_density: "topic 'sensor_gyro' not in log." | 12 |
| accel.legacy_noise_density: "topic 'sensor_accel' not in log." | 9 |
| imu.stationary_window: neither 'sensor_combined' nor 'sensor_gyro' in log | 2 |
| baro.noise_sigma: need at least N samples | 1 |

### Battery

Analyzed 5878, skipped 2926, failed n/a. Failed logs are counted with skipped.

| Reason (details removed, numbers as N) | Logs |
|---|---|
| load error or no usable battery data | 2264 |
| fit log without a usable step or OCV sample | 418 |
| log without a manifest row, left out | 133 |
| simulation log excluded | 78 |
| holdout log not scored | 33 |

### Response

Analyzed 953, skipped 0, failed 0.

| Reason (details removed, numbers as N) | Logs |
|---|---|
| ESC RPM: no esc_status | 756 |
| yaw: low fit quality | 488 |
| roll: low fit quality | 278 |
| no hover segment longer than N s | 273 |
| pitch: low fit quality | 251 |
| pitch: no in-air samples with all signals finite | 199 |
| roll: no in-air samples with all signals finite | 199 |
| yaw: no in-air samples with all signals finite | 199 |
| yaw: lag below filter delay | 95 |
| not logged at the rate controller's rate | 90 |
| ESC RPM: no ESC fitted | 88 |
| pitch: weak instrument | 66 |
| yaw: weak instrument | 45 |
| roll: weak instrument | 33 |
| ESC RPM: esc_status logged at N Hz, need N Hz | 32 |
| pitch: lag below filter delay | 23 |
| roll: implausible gain | 18 |
| roll: lag below filter delay | 18 |
| pitch: implausible gain | 15 |
| hover segments too short for the in-flight noise at N Hz | 6 |
| yaw: low excitation | 5 |
| yaw: implausible gain | 3 |
| ESC RPM: all zero | 2 |
| roll: low excitation | 2 |
| pitch: fit failed | 1 |
| pitch: low excitation | 1 |
| roll: fit failed | 1 |
| yaw: fit failed | 1 |
