# TH3SEUS calibration from public PX4 flight logs

## Run

- Run date: 2026-10-30
- Tool commit: ef33a52191bd42a171c52c5ca201c343dac8f273
- Parameters:
  - `batches`: "9 batches of up to 20 GB; each batch folder holds the logs of both sets, so the noise and battery passes skip the response-only logs as logs without a manifest row"
  - `battery_holdout_frac`: 0.2
  - `battery_split_seed`: 20260623
  - `jobs`: 8
  - `local_tool_commit`: "58215506fb1686d83078501484f177e337c7a8a4, with the kit table committed alongside this directory; the cloud passes ran at tool_commit"
  - `max_per_vehicle`: 3
  - `noise_and_battery_set`: "logs logged at full or reduced rate and scenario logs of that screen"
  - `reference_min_vehicles`: 5
  - `response_gates`: {"min_first_stage_r2": 0.05, "min_motor_cmd_std": 0.02, "min_motor_r2": 0.5, "min_r2": 0.1, "min_torque_std": 0.002, "noise_cutoff_hz": 20.0}
  - `response_min_logged_rate_hz`: "200 with 5 % tolerance, on the torque setpoint, rate and rate setpoint"
  - `response_set`: "the full-rate logs of that screen"
  - `selection`: "real (not simulated) quadrotor logs that passed a separate rate-loop data screen; duplicate uploads removed; at most 3 logs per vehicle (the smallest log IDs); vehicle IDs hashed"
  - `source`: "public PX4 Flight Review logs, read from a mirror; raw logs never left the cloud runner"

Commands:

```
# In the cloud, per batch NNNN (LOGS/ holds the batch's downloaded logs and is deleted after the batch):
python -m log_validation.batch_analyze LOGS/ -o noise-NNNN.json --manifest manifest-noise.csv --jobs 8
python -m log_validation.battery_fit --log-dir LOGS/ --extract-to battery-obs/ --manifest manifest-noise.csv --jobs 8
python -m log_validation.response LOGS/ -o response-NNNN.json --manifest manifest-response.csv --jobs 8
# Locally, on the downloaded run folder RUN/ (pass outputs, manifests and battery extracts; no raw logs):
python -m log_validation.outputs merge -o noise-results.json --manifest RUN/manifest-noise.csv RUN/parts/noise-*.json
python -m log_validation.outputs merge -o response-results.json --manifest RUN/manifest-response.csv RUN/parts/response-*.json
for t in RUN/battery-obs/*.tar; do tar -xf "$t" -C battery-obs/; done
python -m log_validation.battery_fit --obs-dir battery-obs/ --manifest RUN/manifest-noise.csv --out-json battery-fit.json --out-md battery-fit.md
python -m log_validation.reference_airframes response-results.json --kits log_validation/reference_kits.json -o airframes.json --min-vehicles 5
python -m log_validation.calibration_report --noise noise-results.json --battery battery-fit.json --response response-results.json --airframes airframes.json --run-info run-info.json --out-dir log_validation/calibration/2026-10-30 --forbid PRIVATE
```

## IMU noise before and after

Median noise density at rest per MCU family. The current column is the estimator TH3SEUS uses today; the new column is the fixed estimator. Both medians are taken over the same logs, those with both values; Logs is their number. Per-sample noise is density times sqrt(400), the noise one 400 Hz tick adds.

Gyro, rad/s/sqrt(Hz); per sample rad/s:

| MCU family | Logs | Current (TH3SEUS estimator) | New | Current / new | Current per sample at 400 Hz | New per sample at 400 Hz |
|---|---|---|---|---|---|---|
| STM32F4 | 112 | 0.004162 | 0.001101 | 3.78 | 0.08324 | 0.02202 |
| STM32F7 | 267 | 0.03804 | 0.001852 | 20.5 | 0.7608 | 0.03705 |
| STM32H7 | 6448 | 0.02914 | 0.001138 | 25.6 | 0.5828 | 0.02277 |
| ALL | 6975 | 0.02897 | 0.001165 | 24.9 | 0.5794 | 0.0233 |

Accelerometer, m/s^2/sqrt(Hz); per sample m/s^2:

| MCU family | Logs | Current (TH3SEUS estimator) | New | Current / new | Current per sample at 400 Hz | New per sample at 400 Hz |
|---|---|---|---|---|---|---|
| STM32F4 | 111 | 0.03994 | 0.01091 | 3.66 | 0.7989 | 0.2183 |
| STM32F7 | 267 | 1.033 | 0.04851 | 21.3 | 20.66 | 0.9701 |
| STM32H7 | 6450 | 0.7924 | 0.03554 | 22.3 | 15.85 | 0.7107 |
| ALL | 6976 | 0.7826 | 0.03559 | 22 | 15.65 | 0.7118 |

## Battery

Internal resistance per cell, from current steps. Deployed is the value the simulator uses.

| Cells | Median mohm | p10 | p90 | Deployed mohm | Steps | Vehicles | Deployed from |
|---|---|---|---|---|---|---|---|
| 1S | 20.15 | 12.88 | 36.79 | 20.15 | 11619 | 108 | own fit |
| 2S | 5.95 | 2.19 | 10.70 | 5.95 | 1210 | 18 | own fit |
| 3S | 7.16 | 1.91 | 26.60 | 7.16 | 6773 | 131 | own fit |
| 4S | 3.91 | 1.10 | 16.67 | 3.12 | 47326 | 752 | pooled median |
| 5S | 8.07 | 1.24 | 23.92 | 8.07 | 733 | 20 | own fit |
| 6S | 3.03 | 1.22 | 10.42 | 3.12 | 136197 | 1082 | pooled median |
| 8S | 3.28 | 1.12 | 10.51 | 3.12 | 14967 | 43 | pooled median |
| 10S | 2.90 | 2.63 | 3.87 | 3.12 | 395 | 1 | pooled median |
| 12S | 2.48 | 1.42 | 4.38 | 3.12 | 47606 | 95 | pooled median |
| 14S | 3.36 | 1.10 | 9.25 | 3.12 | 10847 | 13 | pooled median |

Pooled median: 3.12 mohm per cell over 277673 steps.

Held-out checks:

| Check | Value |
|---|---|
| Held-out logs scored | 1148 of 1737 |
| Voltage MAE, median (V per cell) | 0.08792 |
| Voltage MAE, p75 (V per cell) | 0.1622 |
| Target of 0.1 V per cell | met |
| Step check: steps (vehicles) | 172073 (564) |
| Step check: median error (V per cell) | 0.03751 |
| Step check: p75 error (V per cell) | 0.09595 |

## Vehicle response

Overall distributions over vehicle medians (each vehicle counts once). Effectiveness is in motor-command space; lag is the body-rate lag minus the log's filter delay, an upper bound on motor lag.

| Quantity | Logs | Vehicles | p5 | p25 | p50 | p75 | p95 |
|---|---|---|---|---|---|---|---|
| effectiveness.pitch | 221 | 154 | 72.79 | 129 | 189.6 | 308.5 | 612.9 |
| effectiveness.roll | 228 | 149 | 82.9 | 141 | 212.4 | 321.8 | 737.1 |
| effectiveness.yaw | 22 | 19 | 15.52 | 47.64 | 86.36 | 239.7 | 689.7 |
| hover.command | 681 | 361 | 0.2087 | 0.3544 | 0.4434 | 0.5323 | 0.6973 |
| inflight_gyro_noise.pitch | 675 | 358 | 0.00713 | 0.01537 | 0.02745 | 0.05327 | 0.1509 |
| inflight_gyro_noise.roll | 675 | 358 | 0.007921 | 0.01782 | 0.0306 | 0.06201 | 0.2063 |
| inflight_gyro_noise.yaw | 675 | 358 | 0.003391 | 0.007992 | 0.0155 | 0.02744 | 0.1071 |
| lag_minus_filter_s.pitch | 307 | 205 | 0.007068 | 0.01732 | 0.02363 | 0.03842 | 0.06158 |
| lag_minus_filter_s.roll | 314 | 201 | 0.007068 | 0.01704 | 0.02242 | 0.03527 | 0.06046 |
| lag_minus_filter_s.yaw | 27 | 23 | 0.001956 | 0.0075 | 0.01258 | 0.02218 | 0.06103 |
| motor_lag_s | 75 | 42 | 0.0231 | 0.03944 | 0.056 | 0.071 | 0.1082 |
| rate.torque_setpoint_hz | 953 | 458 | 399.3 | 665.3 | 730.8 | 800 | 819 |

## Reference airframes

A kit is compared when it has at least 5 vehicles. Ratio is the TH3SEUS prediction over the real median.

### DJI F450 (airframe 4011, 9 vehicles)

not compared: no source for motor_kv; no source for motor_weight_g; no source for esc_continuous_amps; no source for prop_diameter_in; no source for prop_pitch_in; no source for prop_blade_count; no source for battery_weight_g

### Holybro S500 (airframe 4015, 5 vehicles)

not compared: no source for frame_weight_g; no source for motor_kv; no source for motor_weight_g; no source for esc_continuous_amps; no source for prop_blade_count; no source for battery_cells; no source for battery_capacity_mah; no source for battery_c_rating; no source for battery_weight_g; no source for fc_weight_g

### Holybro X500 V2 (airframe 4019, 12 vehicles)

not compared: no source for frame_weight_g; no source for motor_weight_g; no source for prop_blade_count; no source for battery_capacity_mah; no source for battery_c_rating; no source for battery_weight_g

### BetaFPV Beta75X 2S (airframe 4041, 12 vehicles)

not compared: no source for motor_weight_g; no source for prop_pitch_in

Recommendations:

- thrust basis K_CT (reported, never changed) (hover.command): no change. hover throttle discrepancy is reported for the thrust basis K_CT, which is never recommended for change
- motor lag tau = 0.02 + 15/KV (lag): no change. no change recommended: 0 compared kits have data, at least 2 are needed
- inertia and arm-length formulas (effectiveness): no change. no change recommended: 0 compared kits have data, at least 2 are needed

## Randomization ranges

The full table, with the cell-count and airframe classes, is in `ranges.csv`. Overall rows:

| Quantity | Logs | Vehicles | p5 | p25 | p50 | p75 | p95 | Class rows |
|---|---|---|---|---|---|---|---|---|
| accel.accel_noise_density | 7102 | 3885 | 0.001929 | 0.01319 | 0.03559 | 0.06952 | 0.1754 | 17 |
| battery.r_mohm_per_cell | 4164 | 2222 | 1.52 | 2.933 | 5.047 | 10.62 | 25.33 | 9 |
| effectiveness.pitch | 221 | 154 | 72.79 | 129 | 189.6 | 308.5 | 612.9 | 8 |
| effectiveness.roll | 228 | 149 | 82.9 | 141 | 212.4 | 321.8 | 737.1 | 8 |
| effectiveness.yaw | 22 | 19 | 15.52 | 47.64 | 86.36 | 239.7 | 689.7 | 2 |
| gyro.gyro_noise_density | 7102 | 3885 | 0.0001954 | 0.0005132 | 0.001156 | 0.00241 | 0.005825 | 17 |
| hover.command | 681 | 361 | 0.2087 | 0.3544 | 0.4434 | 0.5323 | 0.6973 | 13 |
| inflight_gyro_noise.pitch | 675 | 358 | 0.00713 | 0.01537 | 0.02745 | 0.05327 | 0.1509 | 13 |
| inflight_gyro_noise.roll | 675 | 358 | 0.007921 | 0.01782 | 0.0306 | 0.06201 | 0.2063 | 13 |
| inflight_gyro_noise.yaw | 675 | 358 | 0.003391 | 0.007992 | 0.0155 | 0.02744 | 0.1071 | 13 |
| lag_minus_filter_s.pitch | 307 | 205 | 0.007068 | 0.01732 | 0.02363 | 0.03842 | 0.06158 | 9 |
| lag_minus_filter_s.roll | 314 | 201 | 0.007068 | 0.01704 | 0.02242 | 0.03527 | 0.06046 | 9 |
| lag_minus_filter_s.yaw | 27 | 23 | 0.001956 | 0.0075 | 0.01258 | 0.02218 | 0.06103 | 2 |
| motor_lag_s | 75 | 42 | 0.0231 | 0.03944 | 0.056 | 0.071 | 0.1082 | 3 |
| rate.torque_setpoint_hz | 953 | 458 | 399.3 | 665.3 | 730.8 | 800 | 819 | 13 |

## Gaps TH3SEUS does not model

- Vibration: not modelled. In flight, the median gyro noise (the angular velocity minus its moving average) is 0.02539 rad/s per sample at a median 786.2 Hz. On the density basis (per-sample std times sqrt(1 / rate), which treats the signal as white) that is 0.001009 rad/s/sqrt(Hz), against 0.001151 at rest: a ratio of 0.877 (675 flight logs, 7102 at-rest logs). The ratio is biased low: the logged in-flight signal is low-passed at IMU_GYRO_CUTOFF, usually well below half the logged rate, while it is divided by sqrt(logged rate). The two bases are not exactly comparable, so both are given.
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
| Logs without a manifest row | 0 |
| Logs over the per-vehicle cap | 13797 |
| Failed downloads | 0 |
| candidates_noise_and_battery | 29091 |
| candidates_response | 2988 |
| capped_out_noise_and_battery | 12461 |
| capped_out_response | 1336 |
| copies_removed_noise_and_battery | 7959 |
| copies_removed_response | 699 |
| logs_downloaded | 8804 |
| selected_noise_and_battery | 8671 |
| selected_response | 953 |

### Noise

Analyzed 7102, skipped 1569, failed 0.

| Reason (details removed, numbers as N) | Logs |
|---|---|
| gps.horizontal_noise_sigma: "topic 'vehicle_gps_position' not in log. Available topics: ['sensor_accel', 'sensor_combined', 'sensor_gyro', 'sensor_mag', 'vehicle_air_data']" | 1614 |
| imu.stationary_window: no quiet >=Ns run | 1559 |
| gps.horizontal_noise_sigma: "topic 'vehicle_gps_position' not in log. Available topics: ['sensor_accel', 'sensor_combined', 'sensor_gyro', 'vehicle_air_data']" | 570 |
| mag.noise_sigma_gauss: "topic 'sensor_mag' not in log. Available topics: ['sensor_accel', 'sensor_combined', 'sensor_gyro', 'vehicle_air_data']" | 570 |
| baro.noise_sigma: "topic 'vehicle_air_data' not in log. Available topics: ['sensor_accel', 'sensor_combined', 'sensor_gyro']" | 304 |
| gps.horizontal_noise_sigma: "topic 'vehicle_gps_position' not in log. Available topics: ['sensor_accel', 'sensor_combined', 'sensor_gyro']" | 304 |
| mag.noise_sigma_gauss: "topic 'sensor_mag' not in log. Available topics: ['sensor_accel', 'sensor_combined', 'sensor_gyro']" | 304 |
| baro.noise_sigma: "topic 'vehicle_air_data' not in log. Available topics: ['sensor_accel', 'sensor_combined', 'sensor_gyro', 'sensor_mag']" | 113 |
| gps.horizontal_noise_sigma: "topic 'vehicle_gps_position' not in log. Available topics: ['sensor_accel', 'sensor_combined', 'sensor_gyro', 'sensor_mag']" | 113 |
| accel.legacy_noise_density: "topic 'sensor_accel' not in log. Available topics: ['sensor_combined', 'sensor_mag', 'vehicle_air_data', 'vehicle_gps_position']" | 60 |
| gyro.legacy_noise_density: "topic 'sensor_gyro' not in log. Available topics: ['sensor_combined', 'sensor_mag', 'vehicle_air_data', 'vehicle_gps_position']" | 60 |
| baro.noise_sigma: "topic 'vehicle_air_data' not in log. Available topics: ['sensor_accel', 'sensor_combined', 'sensor_gyro', 'sensor_mag', 'vehicle_gps_position']" | 33 |
| gps.horizontal_noise_sigma: too few GPS samples on ground | 33 |
| mag.noise_sigma_gauss: "topic 'sensor_mag' not in log. Available topics: ['sensor_accel', 'sensor_combined', 'sensor_gyro', 'vehicle_air_data', 'vehicle_gps_position']" | 21 |
| mag.noise_sigma_gauss: need at least N samples | 20 |
| gyro.legacy_noise_density: need at least N timestamps | 15 |
| accel.legacy_noise_density: need at least N timestamps | 12 |
| simulation log skipped | 8 |
| accel.legacy_noise_density: "topic 'sensor_accel' not in log. Available topics: ['sensor_combined', 'vehicle_air_data']" | 5 |
| baro.noise_sigma: "topic 'vehicle_air_data' not in log. Available topics: ['sensor_accel', 'sensor_combined', 'sensor_gyro', 'vehicle_gps_position']" | 5 |
| gps.horizontal_noise_sigma: "topic 'vehicle_gps_position' not in log. Available topics: ['sensor_combined', 'vehicle_air_data']" | 5 |
| gyro.legacy_noise_density: "topic 'sensor_gyro' not in log. Available topics: ['sensor_combined', 'vehicle_air_data']" | 5 |
| mag.noise_sigma_gauss: "topic 'sensor_mag' not in log. Available topics: ['sensor_accel', 'sensor_combined', 'sensor_gyro', 'vehicle_gps_position']" | 5 |
| mag.noise_sigma_gauss: "topic 'sensor_mag' not in log. Available topics: ['sensor_combined', 'vehicle_air_data']" | 5 |
| accel.legacy_noise_density: "topic 'sensor_accel' not in log. Available topics: ['sensor_combined']" | 4 |
| baro.noise_sigma: "topic 'vehicle_air_data' not in log. Available topics: ['sensor_combined']" | 4 |
| gps.horizontal_noise_sigma: "topic 'vehicle_gps_position' not in log. Available topics: ['sensor_combined']" | 4 |
| gyro.legacy_noise_density: "topic 'sensor_gyro' not in log. Available topics: ['sensor_combined']" | 4 |
| mag.noise_sigma_gauss: "topic 'sensor_mag' not in log. Available topics: ['sensor_combined']" | 4 |
| baro.noise_sigma: "topic 'vehicle_air_data' not in log. Available topics: ['sensor_accel', 'sensor_combined']" | 3 |
| gps.horizontal_noise_sigma: "topic 'vehicle_gps_position' not in log. Available topics: ['sensor_accel', 'sensor_combined']" | 3 |
| gyro.legacy_noise_density: "topic 'sensor_gyro' not in log. Available topics: ['sensor_accel', 'sensor_combined']" | 3 |
| mag.noise_sigma_gauss: "topic 'sensor_mag' not in log. Available topics: ['sensor_accel', 'sensor_combined']" | 3 |
| imu.stationary_window: neither 'sensor_combined' nor 'sensor_gyro' in log | 2 |
| accel.legacy_noise_density: "topic 'sensor_accel' not in log. Available topics: ['sensor_combined', 'sensor_mag', 'vehicle_air_data']" | 1 |
| accel.legacy_noise_density: "topic 'sensor_accel' not in log. Available topics: ['sensor_combined', 'vehicle_air_data', 'vehicle_gps_position']" | 1 |
| baro.noise_sigma: need at least N samples | 1 |
| gps.horizontal_noise_sigma: "topic 'vehicle_gps_position' not in log. Available topics: ['sensor_combined', 'sensor_mag', 'vehicle_air_data']" | 1 |
| gyro.legacy_noise_density: "topic 'sensor_gyro' not in log. Available topics: ['sensor_combined', 'sensor_mag', 'vehicle_air_data']" | 1 |
| gyro.legacy_noise_density: "topic 'sensor_gyro' not in log. Available topics: ['sensor_combined', 'vehicle_air_data', 'vehicle_gps_position']" | 1 |
| mag.noise_sigma_gauss: "topic 'sensor_mag' not in log. Available topics: ['sensor_combined', 'vehicle_air_data', 'vehicle_gps_position']" | 1 |

### Battery

Analyzed 5880, skipped 2340, failed n/a. Failed logs are counted with skipped.

| Reason (details removed, numbers as N) | Logs |
|---|---|
| load error or no usable battery data | 2332 |
| holdout log not scored | 589 |
| log without a manifest row, left out | 133 |
| simulation log excluded | 8 |

### Response

Analyzed 953, skipped 0, failed 0.

| Reason (details removed, numbers as N) | Logs |
|---|---|
| ESC RPM: no esc_status | 756 |
| yaw: low fit quality | 488 |
| roll: low fit quality | 278 |
| no hover segment | 272 |
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
