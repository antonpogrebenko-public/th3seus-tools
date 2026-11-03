# TH3SEUS calibration from public PX4 flight logs

## Run

- Run date: 2026-11-02
- Tool commit: 9f8a749b6ebb95d5b78255742cb74db94b987f49
- Parameters:
  - `at_rest_window`: "every at-rest value (IMU, barometer and magnetometer noise, GPS rest spread) comes from a still run of at least 3 s while the vehicle is disarmed (actuator_armed, else vehicle_status.arming_state 0 or 1), starting at least 10 s after a disarm; a log recorded only while armed has no at-rest values"
  - `batches`: "32 batches of up to 20 GB; each batch folder holds the logs of both sets, so the noise and battery passes skip the logs only in the response set (506 in all) as logs without a manifest row, and the response pass skips the logs only in the noise and battery set (35,119 in all)"
  - `battery_capacity_bounds_mah`: "1000,2500,5000,10000,20000"
  - `battery_holdout_frac`: 0.2
  - `battery_split_seed`: 20260623
  - `gps_rest_min_s`: 60.0
  - `jobs`: 8
  - `local_tool_commit`: "68a8087df756bad41eb8e2a4c3e94bd1bccffe7b"
  - `max_per_vehicle`: 3
  - `noise_and_battery_set`: "every log of the mirror that the catalog step could read and marked as real hardware (a log marked as simulated, or whose simulation flag is unknown, is left out); all vehicle types"
  - `part_tables`: {"allan_slope_tolerance": 0.15, "catalog_spellings": "none", "min_vehicles": 10, "sim_rate_baro_hz": 50.0, "sim_rate_mag_hz": 50.0}
  - `reference_min_vehicles`: 5
  - `reproduce`: "the catalog step, the rate-loop data screen and the step that builds the manifests are not public; every entry of noise-results.json and response-results.json carries its Flight Review log ID and hashed vehicle ID, so the two manifests can be rebuilt from them, and log-ids/ lists the logs behind each pass's numbers"
  - `response_gates`: {"min_first_stage_r2": 0.05, "min_motor_cmd_std": 0.02, "min_motor_r2": 0.5, "min_r2": 0.1, "min_torque_std": 0.002, "noise_cutoff_hz": 20.0}
  - `response_min_logged_rate_hz`: "200 with 5 % tolerance, on the torque setpoint, rate and rate setpoint"
  - `response_set`: "the logs that the rate-loop data screen kept as full-rate (quadrotors); the response pass checks the logged rates again and refuses the logs not logged at the rate controller's rate (see Counts)"
  - `selection`: "duplicate uploads removed (the same file content in the noise and battery set; the copies the data screen marked in the response set); at most 3 logs per vehicle (the smallest log IDs); vehicle IDs hashed; the passes found further simulation logs among them (SYS_HITL set, or a simulated sensor or board) and skipped them (see Counts)"
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
python -m log_validation.part_tables noise-results.json --out-dir parts --run-date 2026-11-02
python -m log_validation.calibration_report --noise noise-results.json --battery battery-fit.json --response response-results.json --airframes airframes.json --run-info run-info.json --parts parts --out-dir log_validation/calibration/2026-11-02 --forbid PRIVATE
```

## IMU noise before and after

Median noise density at rest per MCU family. The current column is the estimator TH3SEUS uses today; the new column is the fixed estimator. Both medians are taken over the same logs, those with both values; Logs is their number. Per-sample noise is density times sqrt(400), the noise one 400 Hz tick adds.

Gyro, rad/s/sqrt(Hz); per sample rad/s:

| MCU family | Logs | Current (TH3SEUS estimator) | New | Current / new | Current per sample at 400 Hz | New per sample at 400 Hz |
|---|---|---|---|---|---|---|
| STM32F4 | 235 | 0.002403 | 8.833e-05 | 27.2 | 0.04806 | 0.001767 |
| STM32F7 | 477 | 0.0025 | 7.621e-05 | 32.8 | 0.04999 | 0.001524 |
| STM32H7 | 2353 | 0.004106 | 0.0001309 | 31.4 | 0.08212 | 0.002619 |
| ALL | 3179 | 0.003409 | 0.0001151 | 29.6 | 0.06819 | 0.002301 |

Accelerometer, m/s^2/sqrt(Hz); per sample m/s^2:

| MCU family | Logs | Current (TH3SEUS estimator) | New | Current / new | Current per sample at 400 Hz | New per sample at 400 Hz |
|---|---|---|---|---|---|---|
| STM32F4 | 235 | 0.02899 | 0.001297 | 22.4 | 0.5798 | 0.02593 |
| STM32F7 | 477 | 0.03595 | 0.001253 | 28.7 | 0.719 | 0.02507 |
| STM32H7 | 2347 | 0.03564 | 0.001482 | 24.1 | 0.7129 | 0.02963 |
| ALL | 3173 | 0.0348 | 0.001433 | 24.3 | 0.6961 | 0.02865 |

## Part tables

Rules: at least 10 vehicles per proposed value; Allan slope within 0.15 of -0.5; simulator rates 50.0 Hz (barometer) and 50.0 Hz (magnetometer). Rust text for hitl-physics 0.13.1 and hitl-daemon 0.27.0.

### IMU

| Part | Parameter | Entry | Current | Proposed | Logs | Vehicles | p25 | p75 | No proposal because |
|---|---|---|---|---|---|---|---|---|---|
| Mpu6000 (MPU6000) | gyro_noise_density | existing | 0.0008727 | 7.141e-05 | 104 | 70 | 6.042e-05 | 0.00011 |  |
| Mpu6000 (MPU6000) | accel_noise_density | existing | 0.00637 | 0.001504 | 104 | 70 | 0.001281 | 0.002218 |  |
| Mpu6500 (MPU6500) | gyro_noise_density | existing | 0.00094 | n/a | 13 | 9 | 9.754e-05 | 0.0002504 | below the vehicle threshold |
| Mpu6500 (MPU6500) | accel_noise_density | existing | 0.00686 | n/a | 13 | 9 | 0.001079 | 0.001286 | below the vehicle threshold |
| Icm20689 (ICM20689) | gyro_noise_density | existing | 8.73e-05 | 9.295e-05 | 209 | 149 | 6.661e-05 | 0.0001383 |  |
| Icm20689 (ICM20689) | accel_noise_density | existing | 0.000785 | 0.001502 | 209 | 149 | 0.0009855 | 0.002408 |  |
| Icm42688p (ICM42688P) | gyro_noise_density | existing | 4.88e-05 | 7.997e-05 | 525 | 373 | 4.747e-05 | 0.0001538 |  |
| Icm42688p (ICM42688P) | accel_noise_density | existing | 0.000686 | 0.001368 | 525 | 373 | 0.0007074 | 0.002725 |  |
| Bmi270 (BMI270) | gyro_noise_density | existing | 0.00014 | 0.0001092 | 41 | 33 | 8.53e-05 | 0.0002594 |  |
| Bmi270 (BMI270) | accel_noise_density | existing | 0.00245 | 0.001326 | 41 | 33 | 0.001024 | 0.002784 |  |
| Lsm6dso | gyro_noise_density | existing | 6.11e-05 | n/a | 0 | 0 | n/a | n/a | no eligible log |
| Lsm6dso | accel_noise_density | existing | 0.000588 | n/a | 0 | 0 | n/a | n/a | no eligible log |
| Adis16470 (ADIS16470) | gyro_noise_density | candidate (new) | 0.0008727 | 7.152e-05 | 23 | 17 | 3.016e-05 | 0.0001045 |  |
| Adis16470 (ADIS16470) | accel_noise_density | candidate (new) | 0.00637 | 0.001139 | 23 | 17 | 0.0006902 | 0.001719 |  |
| Adis16507 (ADIS16507) | gyro_noise_density | candidate (new) | 0.0008727 | n/a | 2 | 2 | 3.645e-05 | 7.827e-05 | below the vehicle threshold |
| Adis16507 (ADIS16507) | accel_noise_density | candidate (new) | 0.00637 | n/a | 2 | 2 | 0.0007787 | 0.001554 | below the vehicle threshold |
| Adis1650x (ADIS1650X) | gyro_noise_density | candidate (new) | 0.0008727 | n/a | 1 | 1 | 0.007622 | 0.007622 | below the vehicle threshold |
| Adis1650x (ADIS1650X) | accel_noise_density | candidate (new) | 0.00637 | n/a | 1 | 1 | 0.3748 | 0.3748 | below the vehicle threshold |
| Adis16607 (ADIS16607) | gyro_noise_density | candidate (new) | 0.0008727 | 0.0001231 | 20 | 10 | 7.826e-05 | 0.0001561 |  |
| Adis16607 (ADIS16607) | accel_noise_density | candidate (new) | 0.00637 | 0.003539 | 20 | 10 | 0.002065 | 0.003939 |  |
| Bmi055 (BMI055) | gyro_noise_density | candidate (new) | 0.0008727 | 0.0002762 | 185 | 131 | 0.0002627 | 0.0002975 |  |
| Bmi055 (BMI055) | accel_noise_density | candidate (new) | 0.00637 | 0.001395 | 185 | 131 | 0.001135 | 0.002303 |  |
| Bmi088 (BMI088) | gyro_noise_density | candidate (new) | 0.0008727 | 0.0002713 | 638 | 392 | 0.0002461 | 0.0003273 |  |
| Bmi088 (BMI088) | accel_noise_density | candidate (new) | 0.00637 | 0.001682 | 638 | 392 | 0.001338 | 0.002839 |  |
| Fxas21002c (FXAS21002C) | gyro_noise_density | candidate (new) | 0.0008727 | n/a | 2 | 1 | 0.0002295 | 0.0002295 | below the vehicle threshold |
| Fxas21002c (FXAS21002C) | accel_noise_density | candidate (new) | 0.00637 | n/a | 2 | 1 | 0.006489 | 0.006489 | below the vehicle threshold |
| Iam20680hp (IAM20680HP) | gyro_noise_density | candidate (new) | 0.0008727 | n/a | 4 | 2 | 3.37e-05 | 4.131e-05 | below the vehicle threshold |
| Iam20680hp (IAM20680HP) | accel_noise_density | candidate (new) | 0.00637 | n/a | 4 | 2 | 0.0003879 | 0.0009122 | below the vehicle threshold |
| Icm20602 (ICM20602) | gyro_noise_density | candidate (new) | 0.0008727 | 6.602e-05 | 871 | 681 | 5.533e-05 | 0.0001116 |  |
| Icm20602 (ICM20602) | accel_noise_density | candidate (new) | 0.00637 | 0.0007635 | 871 | 681 | 0.000661 | 0.001547 |  |
| Icm20608g (ICM20608G) | gyro_noise_density | candidate (new) | 0.0008727 | 9.93e-05 | 24 | 13 | 7.722e-05 | 0.0001156 |  |
| Icm20608g (ICM20608G) | accel_noise_density | candidate (new) | 0.00637 | 0.002125 | 24 | 13 | 0.001701 | 0.005207 |  |
| Icm20649 (ICM20649) | gyro_noise_density | candidate (new) | 0.0008727 | 0.000185 | 36 | 28 | 0.0001731 | 0.0002338 |  |
| Icm20649 (ICM20649) | accel_noise_density | candidate (new) | 0.00637 | 0.001699 | 36 | 28 | 0.001378 | 0.002619 |  |
| Icm20948 (ICM20948) | gyro_noise_density | candidate (new) | 0.0008727 | 0.0001125 | 30 | 22 | 8.499e-05 | 0.0001401 |  |
| Icm20948 (ICM20948) | accel_noise_density | candidate (new) | 0.00637 | 0.001322 | 30 | 22 | 0.001165 | 0.002077 |  |
| Icm40609d (ICM40609D) | gyro_noise_density | candidate (new) | 0.0008727 | n/a | 6 | 5 | 4.373e-05 | 7.391e-05 | below the vehicle threshold |
| Icm40609d (ICM40609D) | accel_noise_density | candidate (new) | 0.00637 | n/a | 6 | 5 | 0.0007151 | 0.001039 | below the vehicle threshold |
| Icm42605 (ICM42605) | gyro_noise_density | candidate (new) | 0.0008727 | n/a | 9 | 6 | 0.0001442 | 0.0003326 | below the vehicle threshold |
| Icm42605 (ICM42605) | accel_noise_density | candidate (new) | 0.00637 | n/a | 9 | 6 | 0.0009573 | 0.002157 | below the vehicle threshold |
| Icm42670p (ICM42670P) | gyro_noise_density | candidate (new) | 0.0008727 | 5.957e-05 | 89 | 79 | 3.279e-05 | 9.001e-05 |  |
| Icm42670p (ICM42670P) | accel_noise_density | candidate (new) | 0.00637 | 0.0006262 | 89 | 79 | 0.0003468 | 0.001343 |  |
| Icm42686p (ICM42686P) | gyro_noise_density | candidate (new) | 0.0008727 | 7.024e-05 | 34 | 19 | 6.127e-05 | 0.0001223 |  |
| Icm42686p (ICM42686P) | accel_noise_density | candidate (new) | 0.00637 | 0.001483 | 34 | 19 | 0.0006764 | 0.001891 |  |
| Icm45686 (ICM45686) | gyro_noise_density | candidate (new) | 0.0008727 | 5.727e-05 | 422 | 259 | 4.405e-05 | 0.0001025 |  |
| Icm45686 (ICM45686) | accel_noise_density | candidate (new) | 0.00637 | 0.001229 | 422 | 259 | 0.000778 | 0.0025 |  |
| Iim42652 (IIM42652) | gyro_noise_density | candidate (new) | 0.0008727 | 0.0001204 | 128 | 81 | 7.533e-05 | 0.0002332 |  |
| Iim42652 (IIM42652) | accel_noise_density | candidate (new) | 0.00637 | 0.001308 | 128 | 81 | 0.00061 | 0.002736 |  |
| Iim42653 (IIM42653) | gyro_noise_density | candidate (new) | 0.0008727 | 0.0001228 | 45 | 31 | 8.621e-05 | 0.0003988 |  |
| Iim42653 (IIM42653) | accel_noise_density | candidate (new) | 0.00637 | 0.002386 | 45 | 31 | 0.0009962 | 0.01167 |  |
| L3gd20 (L3GD20) | gyro_noise_density | candidate (new) | 0.0008727 | n/a | 10 | 8 | 0.0003754 | 0.0004379 | below the vehicle threshold |
| L3gd20 (L3GD20) | accel_noise_density | candidate (new) | 0.00637 | n/a | 10 | 8 | 0.005721 | 0.006817 | below the vehicle threshold |
| Lsm6dsk320x (LSM6DSK320X) | gyro_noise_density | candidate (new) | 0.0008727 | n/a | 2 | 2 | 4.94e-05 | 5.04e-05 | below the vehicle threshold |
| Lsm6dsk320x (LSM6DSK320X) | accel_noise_density | candidate (new) | 0.00637 | n/a | 2 | 2 | 0.0005079 | 0.000652 | below the vehicle threshold |
| Lsm6dsv (LSM6DSV) | gyro_noise_density | candidate (new) | 0.0008727 | n/a | 3 | 2 | 9.582e-05 | 0.0001308 | below the vehicle threshold |
| Lsm6dsv (LSM6DSV) | accel_noise_density | candidate (new) | 0.00637 | n/a | 3 | 2 | 0.0005811 | 0.0006042 | below the vehicle threshold |
| Lsm6dsv32x (LSM6DSV32X) | gyro_noise_density | candidate (new) | 0.0008727 | n/a | 1 | 1 | 4.71e-05 | 4.71e-05 | below the vehicle threshold |
| Lsm6dsv32x (LSM6DSV32X) | accel_noise_density | candidate (new) | 0.00637 | n/a | 1 | 1 | 0.0003397 | 0.0003397 | below the vehicle threshold |
| Lsm9ds1 (LSM9DS1) | gyro_noise_density | candidate (new) | 0.0008727 | n/a | 1 | 1 | 0.0001368 | 0.0001368 | below the vehicle threshold |
| Lsm9ds1 (LSM9DS1) | accel_noise_density | candidate (new) | 0.00637 | n/a | 1 | 1 | 0.0006372 | 0.0006372 | below the vehicle threshold |
| Mpu9250 (MPU9250) | gyro_noise_density | candidate (new) | 0.0008727 | 9.769e-05 | 44 | 31 | 8.181e-05 | 0.0001266 |  |
| Mpu9250 (MPU9250) | accel_noise_density | candidate (new) | 0.00637 | 0.0009651 | 44 | 31 | 0.0005563 | 0.001822 |  |
| Sch16t (SCH16T) | gyro_noise_density | candidate (new) | 0.0008727 | 2.835e-05 | 74 | 61 | 2.482e-05 | 3.226e-05 |  |
| Sch16t (SCH16T) | accel_noise_density | candidate (new) | 0.00637 | 0.0007964 | 74 | 61 | 0.0007297 | 0.0008962 |  |
| Other | gyro_noise_density | Other | 0.0008727 | 9.572e-05 | 3670 | 2434 | 5.663e-05 | 0.0002389 |  |
| Other | accel_noise_density | Other | 0.00637 | 0.001274 | 3670 | 2434 | 0.0007407 | 0.002287 |  |

### Barometer

| Part | Parameter | Entry | Current | Proposed | Logs | Vehicles | p25 | p75 | No proposal because | Logged rate (Hz) | Factor | Flag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Bmp280 (BMP280) | noise_sigma_m | existing | 0.16 | n/a | 1 | 1 | 1.647 | 1.647 | too few logs pass the white-noise check (19 failed the slope, 0 the resolution, of 20 checked) | 0.3305 | 1 |  |
| Bmp388 (BMP388, BMP390) | noise_sigma_m | existing | 0.08 | 0.1059 | 91 | 78 | 0.09898 | 0.1264 |  | 1.004 | 1 |  |
| Dps310 (DPS310) | noise_sigma_m | existing | 0.05 | n/a | 2 | 2 | 4.79 | 14.31 | too few logs pass the white-noise check (131 failed the slope, 3 the resolution, of 133 checked) | 5.072 | 1 |  |
| Spl06 (SPL06) | noise_sigma_m | existing | 0.1 | 0.02095 | 22 | 18 | 0.02008 | 0.02167 |  | 10.36 | 1 |  |
| Ms5611 (MS5611) | noise_sigma_m | existing | 0.1 | 0.2276 | 913 | 646 | 0.2079 | 0.2621 |  | 0.9994 | 1 |  |
| Bmp581 (BMP581) | noise_sigma_m | candidate (new) | 0.15 | 0.02184 | 16 | 15 | 0.01644 | 0.1369 |  | 0.9981 | 1 |  |
| Icp101xx (ICP101XX) | noise_sigma_m | candidate (new) | 0.15 | n/a | 2 | 2 | 0.04683 | 0.0607 | too few logs pass the white-noise check (16 failed the slope, 0 the resolution, of 18 checked) | 4.442 | 1 |  |
| Icp201xx (ICP201XX) | noise_sigma_m | candidate (new) | 0.15 | 0.1903 | 156 | 117 | 0.1622 | 0.2194 |  | 1 | 1 |  |
| Lps22hb (LPS22HB) | noise_sigma_m | candidate (new) | 0.15 | n/a | 4 | 2 | 0.1489 | 0.1584 | below the vehicle threshold (1 failed the slope, 0 the resolution, of 5 checked) | 0.9987 | 1 |  |
| Mpl3115a2 (MPL3115A2) | noise_sigma_m | candidate (new) | 0.15 | n/a | 2 | 1 | 0.8561 | 0.8561 | below the vehicle threshold (0 failed the slope, 0 the resolution, of 2 checked) | 0.9962 | 1 |  |
| Ms5607 (MS5607) | noise_sigma_m | candidate (new) | 0.15 | n/a | 3 | 3 | 0.321 | 0.556 | below the vehicle threshold (4 failed the slope, 0 the resolution, of 7 checked) | 1.002 | 1 |  |
| Spa06 (SPA06) | noise_sigma_m | candidate (new) | 0.15 | n/a | 2 | 2 | 0.01683 | 0.01689 | too few logs pass the white-noise check (29 failed the slope, 0 the resolution, of 31 checked) | 15.71 | 1 |  |
| Other | noise_sigma_m | Other | 0.15 | 0.2198 | 1253 | 915 | 0.1911 | 0.254 |  | 1 | 1 |  |

### Magnetometer

| Part | Parameter | Entry | Current | Proposed | Logs | Vehicles | p25 | p75 | No proposal because | Logged rate (Hz) | Factor | Flag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Hmc5883 (HMC5883) | noise_sigma_gauss | existing | 0.0006 | 0.003405 | 16 | 15 | 0.002338 | 0.005692 |  | 0.9992 | 1 |  |
| Qmc5883 (QMC5883L) | noise_sigma_gauss | existing | 0.0005 | 0.003216 | 152 | 98 | 0.002429 | 0.007436 |  | 1 | 1 |  |
| Ist8310 (IST8310) | noise_sigma_gauss | existing | 0.0003 | 0.002705 | 592 | 410 | 0.001785 | 0.00608 |  | 0.9975 | 1 |  |
| Rm3100 (RM3100) | noise_sigma_gauss | existing | 1.3e-05 | 0.001544 | 135 | 104 | 0.0008468 | 0.005576 |  | 0.999 | 1 |  |
| Lis3mdl (LIS3MDL) | noise_sigma_gauss | existing | 0.00032 | 0.008952 | 37 | 26 | 0.00438 | 0.01191 |  | 1.004 | 1 |  |
| Ak09916 (AK09916) | noise_sigma_gauss | candidate (new) | 0.0006 | 0.009422 | 171 | 113 | 0.008514 | 0.01107 |  | 1 | 1 |  |
| Ak8963 (AK8963) | noise_sigma_gauss | candidate (new) | 0.0006 | n/a | 3 | 3 | 0.0147 | 0.02029 | below the vehicle threshold (1 failed the slope, 0 the resolution, of 4 checked) | 1.001 | 1 |  |
| Bmm150 (BMM150) | noise_sigma_gauss | candidate (new) | 0.0006 | 0.003614 | 355 | 239 | 0.003095 | 0.004657 |  | 1 | 1 |  |
| Bmm350 (BMM350) | noise_sigma_gauss | candidate (new) | 0.0006 | 0.002767 | 15 | 10 | 0.002421 | 0.006115 |  | 1 | 1 |  |
| Iis2mdc (IIS2MDC) | noise_sigma_gauss | candidate (new) | 0.0006 | 0.003449 | 40 | 31 | 0.003019 | 0.005203 |  | 1.002 | 1 |  |
| Ist8308 (IST8308) | noise_sigma_gauss | candidate (new) | 0.0006 | n/a | 7 | 7 | 0.001923 | 0.09199 | too few logs pass the white-noise check (31 failed the slope, 59 the resolution, of 73 checked) | 1 | 1 |  |
| Lsm303agr (LSM303AGR) | noise_sigma_gauss | candidate (new) | 0.0006 | n/a | 8 | 4 | 0.005285 | 0.005749 | below the vehicle threshold (0 failed the slope, 0 the resolution, of 8 checked) | 0.997 | 1 |  |
| Lsm9ds1M (LSM9DS1_M) | noise_sigma_gauss | candidate (new) | 0.0006 | n/a | 0 | 0 | n/a | n/a | below the vehicle threshold (1 failed the slope, 0 the resolution, of 1 checked) | n/a | n/a |  |
| Mmc5983ma (MMC5983MA) | noise_sigma_gauss | candidate (new) | 0.0006 | n/a | 3 | 3 | 0.001962 | 0.003753 | below the vehicle threshold (4 failed the slope, 0 the resolution, of 7 checked) | 0.9966 | 1 |  |
| Qmc5883p (QMC5883P) | noise_sigma_gauss | candidate (new) | 0.0006 | 0.01207 | 22 | 19 | 0.00439 | 0.0147 |  | 1 | 1 |  |
| Other | noise_sigma_gauss | Other | 0.0006 | 0.003485 | 1618 | 1122 | 0.002262 | 0.008023 |  | 1 | 1 |  |

### GPS receiver

| Part | Parameter | Entry | Current | Proposed | Logs | Vehicles | p25 | p75 | No proposal because |
|---|---|---|---|---|---|---|---|---|---|
| UbloxM8N (UBX_8) | horizontal_noise_sigma_m (standalone_3d) | existing | 1.5 | 1.198 | 448 | 302 | 0.8768 | 1.953 |  |
| UbloxM8N (UBX_8) | altitude_noise_sigma_m (standalone_3d) | existing | 3 | 1.87 | 448 | 302 | 1.281 | 3.112 |  |
| UbloxM8N (UBX_8) | velocity_noise_sigma_mps (standalone_3d) | existing | 0.1 | 0.268 | 448 | 302 | 0.2051 | 0.4169 |  |
| UbloxM8N (UBX_8) | update_rate_hz (standalone_3d) | existing | 5 | 5 | 33 | 23 | 4.987 | 5 |  |
| UbloxM9N (UBX_9) | horizontal_noise_sigma_m (standalone_3d) | existing | 0.9 | 0.8705 | 1651 | 1100 | 0.6534 | 1.372 |  |
| UbloxM9N (UBX_9) | altitude_noise_sigma_m (standalone_3d) | existing | 1.5 | 1.43 | 1651 | 1100 | 1.074 | 2.337 |  |
| UbloxM9N (UBX_9) | velocity_noise_sigma_mps (standalone_3d) | existing | 0.05 | 0.2344 | 1651 | 1100 | 0.181 | 0.3331 |  |
| UbloxM9N (UBX_9) | update_rate_hz (standalone_3d) | existing | 10 | 8.001 | 113 | 78 | 8 | 10 |  |
| UbloxM10 (UBX_10) | horizontal_noise_sigma_m (standalone_3d) | existing | 1 | 2.074 | 63 | 47 | 1.468 | 2.739 |  |
| UbloxM10 (UBX_10) | altitude_noise_sigma_m (standalone_3d) | existing | 1.8 | 2.609 | 63 | 47 | 1.977 | 3.784 |  |
| UbloxM10 (UBX_10) | velocity_noise_sigma_mps (standalone_3d) | existing | 0.06 | 0.328 | 49 | 37 | 0.0825 | 0.58 |  |
| UbloxM10 (UBX_10) | update_rate_hz (standalone_3d) | existing | 10 | n/a | 8 | 6 | 2 | 4.25 | below the vehicle threshold |
| UbloxF9P (UBX_F9P) | horizontal_noise_sigma_m (rtk_fixed) | existing | 0.014 | 0.0141 | 535 | 359 | 0.0141 | 0.0141 |  |
| UbloxF9P (UBX_F9P) | altitude_noise_sigma_m (rtk_fixed) | existing | 0.02 | 0.01 | 535 | 359 | 0.01 | 0.01715 |  |
| UbloxF9P (UBX_F9P) | velocity_noise_sigma_mps (rtk_fixed) | existing | 0.015 | 0.094 | 535 | 359 | 0.072 | 0.116 |  |
| UbloxF9P (UBX_F9P) | update_rate_hz (rtk_fixed) | existing | 8 | 5.051 | 42 | 33 | 5 | 8 |  |
| SeptentrioMosaic (SBF) | horizontal_noise_sigma_m (rtk_fixed) | existing | 0.006 | 0.01 | 18 | 11 | 0.01 | 0.015 |  |
| SeptentrioMosaic (SBF) | altitude_noise_sigma_m (rtk_fixed) | existing | 0.012 | 0.02 | 18 | 11 | 0.015 | 0.025 |  |
| SeptentrioMosaic (SBF) | velocity_noise_sigma_mps (rtk_fixed) | existing | 0.01 | 0.01457 | 18 | 11 | 0.01106 | 0.02243 |  |
| SeptentrioMosaic | update_rate_hz (rtk_fixed) | existing | 100 | n/a | 0 | 0 | n/a | n/a | no eligible log |
| Here3 | horizontal_noise_sigma_m (standalone_3d) | existing | 0.9 | n/a | 0 | 0 | n/a | n/a | no eligible log |
| Here3 | altitude_noise_sigma_m (standalone_3d) | existing | 1.5 | n/a | 0 | 0 | n/a | n/a | no eligible log |
| Here3 | velocity_noise_sigma_mps (standalone_3d) | existing | 0.05 | n/a | 0 | 0 | n/a | n/a | no eligible log |
| Here3 | update_rate_hz (standalone_3d) | existing | 8 | n/a | 0 | 0 | n/a | n/a | no eligible log |
| Ubx20 (UBX_20) | horizontal_noise_sigma_m (standalone_3d) | candidate (new) | 1.5 | n/a | 4 | 3 | 0.2424 | 0.3838 | below the vehicle threshold |
| Ubx20 (UBX_20) | altitude_noise_sigma_m (standalone_3d) | candidate (new) | 3 | n/a | 4 | 3 | 0.4421 | 0.7695 | below the vehicle threshold |
| Ubx20 (UBX_20) | velocity_noise_sigma_mps (standalone_3d) | candidate (new) | 0.1 | n/a | 1 | 1 | 0.039 | 0.039 | below the vehicle threshold |
| Ubx20 | update_rate_hz (standalone_3d) | candidate (new) | 5 | n/a | 0 | 0 | n/a | n/a | no eligible log |
| Ubx6 (UBX_6) | horizontal_noise_sigma_m (standalone_3d) | candidate (new) | 1.5 | n/a | 2 | 2 | 1.776 | 2.633 | below the vehicle threshold |
| Ubx6 (UBX_6) | altitude_noise_sigma_m (standalone_3d) | candidate (new) | 3 | n/a | 2 | 2 | 2.888 | 4.034 | below the vehicle threshold |
| Ubx6 (UBX_6) | velocity_noise_sigma_mps (standalone_3d) | candidate (new) | 0.1 | n/a | 2 | 2 | 0.5 | 0.66 | below the vehicle threshold |
| Ubx6 | update_rate_hz (standalone_3d) | candidate (new) | 5 | n/a | 0 | 0 | n/a | n/a | no eligible log |
| Ubx7 (UBX_7) | horizontal_noise_sigma_m (standalone_3d) | candidate (new) | 1.5 | 2.042 | 21 | 16 | 1.519 | 3.259 |  |
| Ubx7 (UBX_7) | altitude_noise_sigma_m (standalone_3d) | candidate (new) | 3 | 3.373 | 21 | 16 | 2.195 | 6.737 |  |
| Ubx7 (UBX_7) | velocity_noise_sigma_mps (standalone_3d) | candidate (new) | 0.1 | 0.4266 | 21 | 16 | 0.3673 | 0.5003 |  |
| Ubx7 | update_rate_hz (standalone_3d) | candidate (new) | 5 | n/a | 0 | 0 | n/a | n/a | no eligible log |
| Other | horizontal_noise_sigma_m (standalone_3d) | Other | 1.5 | 0.812 | 9239 | 5855 | 0.5121 | 1.597 |  |
| Other | altitude_noise_sigma_m (standalone_3d) | Other | 3 | 1.25 | 9235 | 5854 | 0.81 | 2.287 |  |
| Other | velocity_noise_sigma_mps (standalone_3d) | Other | 0.1 | 0.1391 | 9048 | 5748 | 0.065 | 0.2435 |  |
| Other | update_rate_hz (standalone_3d) | Other | 5 | 5 | 960 | 700 | 4.989 | 9.91 |  |

### Boards

The 10 boards with the most vehicles, with the share of vehicles whose most common chip is each one.

| Board | Logs | Vehicles | IMU | Baro | Mag | IMU_GYRO_RATEMAX | IMU_GYRO_CUTOFF |
|---|---|---|---|---|---|---|---|
| PX4_FMU_V6C | 5778 | 2631 | BMI055 0.56, ICM42688P 0.25, BMI088 0.15, ICM20602 0.00, LSM6DSK320X 0.00, UNKNOWN_0xF1 0.00, ICM45686 0.00, LSM6DSV 0.00, MPU6000 0.00, UNKNOWN_0x0A 0.00, UNKNOWN_0x69 0.00, UNKNOWN_0xE5 0.00 | MS5611 0.96, BMP581 0.00, BMP388 0.00, ICP201XX 0.00, UNKNOWN_0x00 0.00 | IST8310 0.93, QMC5883L 0.01, QMC5883P 0.01, HMC5883 0.01, RM3100 0.00, IST8308 0.00, UAVCAN 0.00, IIS2MDC 0.00, UNKNOWN_0x21 0.00 | 800 | 40 |
| PX4_FMU_V6X | 4030 | 1819 | ICM45686 0.52, BMI088 0.25, ICM42688P 0.11, IIM42652 0.03, ICM42670P 0.03, ICM20649 0.03, ADIS16470 0.02, UNKNOWN_0x2F 0.00, ICM20602 0.00, UNKNOWN_0xE3 0.00, UNKNOWN_0xE9 0.00, ICM42686P 0.00, IIM42653 0.00, UNKNOWN_0x39 0.00, UNKNOWN_0xE1 0.00, UNKNOWN_0xEC 0.00 | ICP201XX 0.71, BMP388 0.20, BMP390 0.04, MS5611 0.03, BMP581 0.00, SPA06 0.00, UNKNOWN_0x53 0.00, MS5837 0.00, UAVCAN 0.00 | BMM150 0.74, RM3100 0.16, IST8310 0.07, UAVCAN 0.01, HMC5883 0.00, IST8308 0.00, BMM350 0.00, UNKNOWN_0x0C 0.00, LIS3MDL 0.00, MMC5983MA 0.00, QMC5883L 0.00 | 800 | 40 |
| PX4_FMU_V5 | 3223 | 1485 | ICM20602 0.57, ICM20689 0.22, BMI055 0.20, BMI088 0.00, ICM42670P 0.00, ICM42688P 0.00, ICM45686 0.00, ADIS16477 0.00, UNKNOWN_0x86 0.00 | MS5611 0.96, UAVCAN 0.00 | IST8310 0.97, HMC5883 0.01, UAVCAN 0.01, RM3100 0.00, LIS3MDL 0.00, QMC5883P 0.00 | 800 | 40 |
| MICOAIR_H743_V2 | 2503 | 1138 | BMI088 0.68, BMI270 0.31, ICM42688P 0.00 | SPL06 0.97, SPA06 0.02, BMP581 0.00, DPS310 0.00 | QMC5883L 0.68, IST8310 0.22, QMC5883P 0.02, HMC5883 0.01, RM3100 0.01, MMC5983MA 0.01 | 800 | 80 |
| MODALAI_VOXL2 | 1115 | 1115 | ICM42688P 0.98, BMI270 0.00 | ICP101XX 0.91, DPS310 0.01, MS5837 0.00 | QMC5883L 0.47, IIS2MDC 0.10, IST8310 0.09, RM3100 0.09, IST8308 0.04, LIS3MDL 0.02, BMM350 0.01, BMM150 0.01, UNKNOWN_0x0C 0.00 | 800 | 80 |
| HKUST_NXT_DUAL | 2294 | 1097 | BMI088 1.00, ICM42688P 0.00, ADIS16470 0.00, SCH16T 0.00 | SPL06 0.93, SPA06 0.02, DPS310 0.01 | IST8310 0.12, QMC5883L 0.07, RM3100 0.01, BMM150 0.00, UAVCAN 0.00, UNKNOWN_0x00 0.00 | 2000 | 40 |
| UAT_Q100 | 1109 | 754 | ICM20602 0.98, ICM42688P 0.02 | MS5611 1.00 | IIS2MDC 0.99, UAVCAN 0.00, IST8310 0.00 | 400 | 30 |
| PX4_FMU_V4 | 872 | 702 | ICM20602 0.70, ICM42688P 0.11, MPU6500 0.10, ICM20608G 0.08, MPU9250 0.01, ICM40609D 0.00 | MS5611 0.46, ICP201XX 0.00, MS5607 0.00 | QMC5883L 0.61, IST8310 0.24, LIS3MDL 0.10, HMC5883 0.04, AK8963 0.01, IST8308 0.00 | 800 | 90 |
| CUBEPILOT_CUBEORANGEPLUS | 1383 | 620 | ICM45686 0.85, ICM42688P 0.09, ICM20649 0.04, ICM20948 0.02, UNKNOWN_0xE4 0.00 | MS5611 0.99, ICP201XX 0.00 | AK09916 0.83, IST8310 0.12, RM3100 0.01, LIS3MDL 0.01, QMC5883L 0.01, HMC5883 0.00, IIS2MDC 0.00, MMC5983MA 0.00, UAVCAN 0.00 | 800 | 40 |
| CUAV_X7PRO | 1423 | 603 | ICM20689 0.56, ICM42688P 0.29, ADIS16470 0.12, BMI088 0.01, ICM20649 0.01, ICM42670P 0.00, UNKNOWN_0xE9 0.00 | MS5611 0.99, MS5837 0.00, UAVCAN 0.00 | RM3100 0.96, IST8310 0.02, UAVCAN 0.01 | 800 | 40 |

The full board table is in `board-table.json`.

## Battery

Internal resistance per cell, from current steps. Deployed is the value the simulator uses.

| Cells | Median mohm | p10 | p90 | Deployed mohm | Steps | Vehicles | Deployed from |
|---|---|---|---|---|---|---|---|
| 1S | 8.37 | 3.36 | 19.10 | 5.57 | 169 | 3 | pooled over vehicles: low-n (fewer than 200 step events) |
| 2S | 13.44 | 4.32 | 36.73 | 13.44 | 4732 | 299 | own fit |
| 3S | 8.74 | 2.40 | 30.95 | 8.74 | 13839 | 419 | own fit |
| 4S | 6.08 | 1.56 | 23.66 | 6.08 | 130805 | 1899 | own fit |
| 5S | 5.70 | 1.08 | 17.16 | 5.70 | 1046 | 35 | own fit |
| 6S | 3.21 | 1.21 | 11.05 | 3.21 | 441198 | 3004 | own fit |
| 7S | 3.37 | 1.00 | 14.68 | 3.37 | 1355 | 35 | own fit |
| 8S | 3.26 | 1.20 | 9.84 | 3.26 | 21839 | 99 | own fit |
| 9S | 9.80 | 4.21 | 36.87 | 5.57 | 7 | 1 | pooled over vehicles: low-n (fewer than 200 step events) |
| 10S | 3.89 | 1.59 | 9.00 | 3.89 | 487 | 10 | own fit |
| 12S | 2.15 | 0.92 | 4.86 | 2.15 | 195505 | 588 | own fit |
| 13S | 3.80 | 2.36 | 5.08 | 3.80 | 17009 | 39 | own fit |
| 14S | 2.39 | 0.88 | 9.21 | 2.39 | 14365 | 38 | own fit |

Pooled median over steps: 3.10 mohm per cell over 842356 steps.
Pooled median over vehicles: 5.57 mohm per cell over 6380 vehicles (each vehicle's median over all its step events, then the median over vehicles; the fallback).

Internal resistance per cell count and pack capacity (the log's battery capacity parameter). Spread only: the simulator table stays per cell count.

Capacity class bounds (mAh): 1000, 2500, 5000, 10000, 20000.

| Cells | Capacity (mAh) | Median mohm | p10 | p90 | Steps | Vehicles |
|---|---|---|---|---|---|---|
| 2S | 1000-2500 | 12.49 | 2.17 | 15.70 | 180 | 14 |
| 2S | 2500-5000 | 13.02 | 5.78 | 25.12 | 3142 | 113 |
| 2S | unknown capacity | 21.45 | 2.69 | 65.08 | 1393 | 168 |
| 3S | <1000 | 3.53 | 2.14 | 22.67 | 56 | 5 |
| 3S | 1000-2500 | 26.23 | 4.33 | 59.82 | 313 | 11 |
| 3S | 2500-5000 | 11.03 | 3.94 | 22.96 | 837 | 26 |
| 3S | 5000-10000 | 8.08 | 3.57 | 41.57 | 260 | 5 |
| 3S | unknown capacity | 8.29 | 2.22 | 30.51 | 12141 | 376 |
| 4S | <1000 | 7.34 | 3.49 | 14.09 | 709 | 13 |
| 4S | 1000-2500 | 7.08 | 1.41 | 29.13 | 430 | 15 |
| 4S | 2500-5000 | 9.67 | 2.03 | 31.15 | 6143 | 164 |
| 4S | 5000-10000 | 5.43 | 2.00 | 17.15 | 5230 | 135 |
| 4S | 10000-20000 | 4.57 | 2.60 | 13.67 | 7596 | 27 |
| 4S | >=20000 | 2.51 | 1.26 | 4.86 | 1620 | 9 |
| 4S | unknown capacity | 6.24 | 1.49 | 24.34 | 109077 | 1559 |
| 5S | unknown capacity | 5.94 | 0.95 | 20.45 | 775 | 28 |
| 6S | <1000 | 3.85 | 1.32 | 6.48 | 405 | 6 |
| 6S | 1000-2500 | 2.30 | 1.15 | 6.70 | 1134 | 21 |
| 6S | 2500-5000 | 3.10 | 1.40 | 14.25 | 5926 | 73 |
| 6S | 5000-10000 | 3.72 | 1.58 | 11.98 | 20979 | 237 |
| 6S | 10000-20000 | 3.98 | 1.14 | 10.63 | 16840 | 143 |
| 6S | >=20000 | 2.53 | 1.27 | 5.12 | 45071 | 146 |
| 6S | unknown capacity | 3.30 | 1.19 | 11.68 | 350843 | 2422 |
| 7S | unknown capacity | 3.15 | 0.98 | 15.29 | 1280 | 34 |
| 8S | unknown capacity | 3.32 | 1.21 | 10.01 | 21095 | 91 |
| 10S | unknown capacity | 3.81 | 1.55 | 8.19 | 469 | 8 |
| 12S | 2500-5000 | 2.97 | 1.25 | 7.19 | 1402 | 7 |
| 12S | 5000-10000 | 2.95 | 1.38 | 6.77 | 3187 | 18 |
| 12S | 10000-20000 | 1.91 | 1.06 | 3.85 | 4821 | 58 |
| 12S | >=20000 | 2.07 | 0.88 | 3.97 | 80119 | 147 |
| 12S | unknown capacity | 2.20 | 0.94 | 5.07 | 105976 | 365 |
| 13S | unknown capacity | 3.80 | 2.36 | 5.08 | 17009 | 39 |
| 14S | >=20000 | 2.78 | 1.03 | 9.90 | 7763 | 12 |
| 14S | unknown capacity | 2.13 | 0.77 | 8.29 | 6602 | 27 |

Held-out checks:

| Check | Value |
|---|---|
| Held-out logs with a voltage error (MAE) | 3276 of 7147 real held-out logs, usable or not |
| Voltage MAE, median (V per cell) | 0.08223 |
| Voltage MAE, p75 (V per cell) | 0.1429 |
| Target of 0.1 V per cell | met |
| Step check: steps (vehicles) | 402215 (1650) |
| Step check: median error (V per cell) | 0.02994 |
| Step check: p75 error (V per cell) | 0.06441 |

Held-out check per cell count: medians over the scored held-out logs of each cell count, next to the resistance deployed for it. The signed error is predicted minus measured voltage per cell; positive means the model predicts less sag than the packs show, or that the open-circuit curve sits above these packs.

| Cells | Scored logs | Deployed mohm | Median error (V per cell) | Median signed error (V per cell) |
|---|---|---|---|---|
| 2S | 147 | 13.44 | 0.1260 | 0.1240 |
| 3S | 235 | 8.74 | 0.0897 | -0.0173 |
| 4S | 1022 | 6.08 | 0.0789 | 0.0135 |
| 5S | 26 | 5.70 | 0.1049 | 0.0708 |
| 6S | 1497 | 3.21 | 0.0762 | 0.0153 |
| 7S | 11 | 3.37 | 0.1226 | 0.1226 |
| 8S | 45 | 3.26 | 0.0747 | -0.0188 |
| 10S | 6 | 3.89 | 0.3524 | 0.3524 |
| 12S | 242 | 2.15 | 0.0948 | 0.0208 |
| 13S | 15 | 3.80 | 0.0590 | -0.0476 |
| 14S | 30 | 2.39 | 0.1404 | -0.0110 |

## Vehicle response

Overall distributions over vehicle medians (each vehicle counts once). Effectiveness is in motor-command space; lag is the body-rate lag minus the log's filter delay, an upper bound on motor lag.

| Quantity | Logs | Vehicles | p5 | p25 | p50 | p75 | p95 |
|---|---|---|---|---|---|---|---|
| effectiveness.pitch | 398 | 251 | 66.44 | 135.3 | 195.8 | 325.4 | 586.3 |
| effectiveness.roll | 406 | 256 | 72.52 | 145.1 | 221.5 | 336.7 | 744.4 |
| effectiveness.yaw | 36 | 30 | 13.04 | 44.38 | 66.24 | 177 | 637.9 |
| hover.command | 993 | 513 | 0.2342 | 0.3647 | 0.4485 | 0.5398 | 0.7261 |
| inflight_gyro_noise.pitch | 987 | 510 | 0.005613 | 0.01281 | 0.025 | 0.05023 | 0.1482 |
| inflight_gyro_noise.roll | 987 | 510 | 0.006653 | 0.01524 | 0.02906 | 0.05706 | 0.1666 |
| inflight_gyro_noise.yaw | 987 | 510 | 0.002831 | 0.007266 | 0.01528 | 0.02641 | 0.1007 |
| lag_minus_filter_s.pitch | 419 | 263 | 0.007093 | 0.01737 | 0.02459 | 0.03987 | 0.06251 |
| lag_minus_filter_s.roll | 431 | 269 | 0.006068 | 0.01657 | 0.02307 | 0.03621 | 0.06156 |
| lag_minus_filter_s.yaw | 36 | 30 | 0.001598 | 0.005079 | 0.01332 | 0.02398 | 0.0591 |
| motor_lag_s | 92 | 50 | 0.0239 | 0.03931 | 0.05825 | 0.07094 | 0.09607 |
| rate.torque_setpoint_hz | 1355 | 636 | 399.4 | 665.8 | 751 | 800 | 990.1 |

## Reference airframes

A kit is compared when it has at least 5 vehicles. Each kit is compared on the logs of `log-ids/response.txt` whose airframe ID is the kit's. Ratio is the TH3SEUS prediction over the real median.

### DJI F450 (airframe 4011, 9 vehicles)

not compared: no source for motor_kv; no source for motor_weight_g; no source for esc_continuous_amps; no source for prop_diameter_in; no source for prop_pitch_in; no source for prop_blade_count; no source for battery_weight_g

### Holybro S500 (airframe 4015, 7 vehicles)

not compared: no source for frame_weight_g; no source for motor_kv; no source for motor_weight_g; no source for esc_continuous_amps; no source for prop_blade_count; no source for battery_cells; no source for battery_capacity_mah; no source for battery_c_rating; no source for battery_weight_g; no source for fc_weight_g

### Holybro X500 V2 (airframe 4019, 21 vehicles)

not compared: no source for frame_weight_g; no source for motor_weight_g; no source for prop_blade_count; no source for battery_capacity_mah; no source for battery_c_rating; no source for battery_weight_g

### BetaFPV Beta75X 2S (airframe 4041, 30 vehicles)

not compared: no source for motor_weight_g; no source for prop_pitch_in

Recommendations:

- thrust basis K_CT (reported, never changed) (hover.command): no change. 0 compared kits have data; a hover throttle discrepancy would be reported for the thrust basis K_CT, which is never recommended for change
- motor lag tau = 0.02 + 15/KV (lag): no change. no change recommended: 0 compared kits have data, at least 2 are needed
- inertia and arm-length formulas (effectiveness): no change. no change recommended: 0 compared kits have data, at least 2 are needed

## Randomization ranges

The full table, with the cell-count and airframe classes, is in `ranges.csv`. Overall rows:

| Quantity | Logs | Vehicles | p5 | p25 | p50 | p75 | p95 | Class rows |
|---|---|---|---|---|---|---|---|---|
| accel.accel_noise_density | 3671 | 2435 | 0.0004896 | 0.0007408 | 0.001274 | 0.00229 | 0.01012 | 44 |
| battery.r_mohm_per_cell | 11126 | 6380 | 1.538 | 3.029 | 5.611 | 11.74 | 28.36 | 11 |
| effectiveness.pitch | 398 | 251 | 66.44 | 135.3 | 195.8 | 325.4 | 586.3 | 11 |
| effectiveness.roll | 406 | 256 | 72.52 | 145.1 | 221.5 | 336.7 | 744.4 | 10 |
| effectiveness.yaw | 36 | 30 | 13.04 | 44.38 | 66.24 | 177 | 637.9 | 3 |
| gyro.gyro_noise_density | 3671 | 2435 | 3.338e-05 | 5.664e-05 | 9.577e-05 | 0.0002391 | 0.0004556 | 44 |
| hover.command | 993 | 513 | 0.2342 | 0.3647 | 0.4485 | 0.5398 | 0.7261 | 15 |
| inflight_gyro_noise.pitch | 987 | 510 | 0.005613 | 0.01281 | 0.025 | 0.05023 | 0.1482 | 15 |
| inflight_gyro_noise.roll | 987 | 510 | 0.006653 | 0.01524 | 0.02906 | 0.05706 | 0.1666 | 15 |
| inflight_gyro_noise.yaw | 987 | 510 | 0.002831 | 0.007266 | 0.01528 | 0.02641 | 0.1007 | 15 |
| lag_minus_filter_s.pitch | 419 | 263 | 0.007093 | 0.01737 | 0.02459 | 0.03987 | 0.06251 | 11 |
| lag_minus_filter_s.roll | 431 | 269 | 0.006068 | 0.01657 | 0.02307 | 0.03621 | 0.06156 | 10 |
| lag_minus_filter_s.yaw | 36 | 30 | 0.001598 | 0.005079 | 0.01332 | 0.02398 | 0.0591 | 3 |
| motor_lag_s | 92 | 50 | 0.0239 | 0.03931 | 0.05825 | 0.07094 | 0.09607 | 3 |
| rate.torque_setpoint_hz | 1355 | 636 | 399.4 | 665.8 | 751 | 800 | 990.1 | 15 |

## Gaps TH3SEUS does not model

- Vibration: not modelled. In flight, the median gyro noise (the angular velocity minus its moving average) is 0.02303 rad/s per sample at a median 789.9 Hz. On the density basis (per-sample std times sqrt(1 / rate), which treats the signal as white) that is 0.0008893 rad/s/sqrt(Hz), against 0.0001012 at rest: a ratio of 8.78 (987 flight logs, 3671 at-rest logs). The ratio is biased low: the logged in-flight signal is low-passed at IMU_GYRO_CUTOFF, usually well below half the logged rate, while it is divided by sqrt(logged rate). The two bases are not exactly comparable, so both are given.
- Wind: not modelled. These passes do not measure it.
- Ground effect: not modelled. These passes do not measure it.
- Motor-to-motor differences: not modelled. The motor lag is a median over the ESCs of a log, so the spread between motors is not kept.
- ESC dynamics: not modelled beyond a first-order lag with delay. Measured motor lag: median 0.05825 s, p5 to p95 0.0239 to 0.09607 s over 50 vehicles.

## Counts

### Inputs

| Input | Count |
|---|---|
| Manifest rows | 37323 |
| Manifest rows without a log | 0 |
| Failed downloads | 0 |
| candidates_noise_and_battery | 169883 |
| candidates_response | 4689 |
| capped_out_noise_and_battery | 74718 |
| capped_out_response | 2247 |
| copies_removed_noise_and_battery | 42595 |
| copies_removed_response | 1087 |
| errors_removed_noise_and_battery | 296 |
| logs_downloaded | 36474 |
| logs_in_both_sets | 849 |
| logs_only_in_noise_and_battery_set | 35119 |
| logs_only_in_response_set | 506 |
| selected_noise_and_battery | 35968 |
| selected_response | 1355 |
| simulation_removed_noise_and_battery | 16306 |

### Noise

Analyzed 3692, skipped 32250, failed 26.

| Reason (details removed, numbers as N) | Logs |
|---|---|
| imu.stationary_window: no disarmed quiet >=Ns run | 31145 |
| gps.eph_median_m: no GPS topic | 12349 |
| gps.eph_median_m: no ND fix in vehicle_gps_position | 2559 |
| gps.rest_spread_horizontal_m: at-rest window N s is shorter than N s | 1533 |
| gps.speed_accuracy_median_m_s: receiver reports no speed accuracy | 1140 |
| gps.horizontal_noise_sigma: "topic 'vehicle_gps_position' not in log." | 1102 |
| imu.stationary_window: neither 'sensor_combined' nor 'sensor_gyro' in log | 832 |
| mag.noise_sigma_gauss: "topic 'sensor_mag' not in log." | 815 |
| mag.rate_hz: "topic 'sensor_mag' not in log." | 815 |
| gps.epv_median_m: receiver reports no vertical accuracy | 743 |
| gps.eph_median_m: receiver reports no horizontal accuracy | 739 |
| gps.rest_spread_horizontal_m: no GPS topic | 567 |
| baro.chip_noise_sigma: "topic 'sensor_baro' not in log." | 531 |
| accel.legacy_noise_density: "topic 'sensor_accel' not in log." | 484 |
| gyro.legacy_noise_density: "topic 'sensor_gyro' not in log." | 484 |
| baro.chip_noise_sigma: only N distinct barometer samples in the still window | 317 |
| mag.allan_slope: only N distinct magnetometer samples in the still window | 267 |
| gps.eph_median_m: no ND fix in sensor_gps | 257 |
| imu.stationary_window: no arming state: neither 'actuator_armed' nor 'vehicle_status' in log | 224 |
| gps.rest_spread_horizontal_m: too few GPS samples with a fix in the at-rest window | 215 |
| baro.noise_sigma: "topic 'vehicle_air_data' not in log." | 121 |
| simulation log skipped | 49 |
| gps.horizontal_noise_sigma: too few GPS samples on ground | 29 |
| analysis failed: ValueError | 26 |
| accel.accel_noise_density: no integration interval: topic 'sensor_combined' not in log | 21 |
| accel.integration_interval_s: no integration interval: topic 'sensor_combined' not in log | 21 |
| gyro.gyro_noise_density: no integration interval: topic 'sensor_combined' not in log | 21 |
| gyro.integration_interval_s: no integration interval: topic 'sensor_combined' not in log | 21 |
| gps.eph_median_m: "topic 'sensor_gps' not in log." | 20 |
| gps.horizontal_noise_sigma: "topic 'sensor_gps' not in log." | 17 |
| mag.noise_sigma_gauss: need at least N samples | 9 |
| baro.chip_noise_sigma: constant barometer reading in the still window | 8 |
| gyro.gyro_bias_sigma: need at least N samples for Allan deviation | 8 |
| accel.legacy_noise_density: need at least N timestamps | 7 |
| mag.rate_hz: no magnetometer sample in the still window | 7 |
| gps.rest_spread_horizontal_m: "topic 'sensor_gps' not in log." | 6 |
| gps.speed_accuracy_median_m_s: "field 's_variance_m_s' not in topic 'vehicle_gps_position'." | 6 |
| gyro.legacy_noise_density: need at least N timestamps | 6 |
| baro.chip_noise_sigma: no valid pressure in the still window | 5 |
| gps.eph_median_m: "field 'eph' not in topic 'vehicle_gps_position'." | 5 |
| gps.epv_median_m: "field 'epv' not in topic 'vehicle_gps_position'." | 5 |
| gps.eph_median_m: "field 'eph' not in topic 'sensor_gps'." | 4 |
| gps.epv_median_m: "field 'epv' not in topic 'sensor_gps'." | 4 |
| gps.speed_accuracy_median_m_s: "field 's_variance_m_s' not in topic 'sensor_gps'." | 4 |
| mag.allan_slope: constant magnetometer reading in the still window | 4 |
| baro.noise_sigma: need at least N samples | 2 |
| mag.allan_slope: Allan deviation is not positive | 2 |
| mag.rate_hz: constant magnetometer reading in the still window | 2 |

### Battery

Analyzed 16573, skipped 19901, failed n/a. Failed logs are counted with skipped.

| Reason (details removed, numbers as N) | Logs |
|---|---|
| load error or no usable battery data | 15845 |
| cell count does not match the pack voltage | 2035 |
| fit log without a usable step or OCV sample | 1316 |
| log without a manifest row, left out | 506 |
| holdout log not scored | 150 |
| simulation log excluded | 49 |

### Response

Analyzed 1355, skipped 0, failed 0.

| Reason (details removed, numbers as N) | Logs |
|---|---|
| ESC RPM: no esc_status | 1113 |
| yaw: low fit quality | 679 |
| roll: low fit quality | 371 |
| no hover segment longer than N s | 362 |
| pitch: low fit quality | 335 |
| pitch: no in-air samples with all signals finite | 326 |
| roll: no in-air samples with all signals finite | 326 |
| yaw: no in-air samples with all signals finite | 326 |
| not logged at the rate controller's rate | 130 |
| yaw: lag below filter delay | 108 |
| ESC RPM: no ESC fitted | 105 |
| pitch: weak instrument | 96 |
| yaw: weak instrument | 62 |
| roll: weak instrument | 46 |
| ESC RPM: esc_status logged at N Hz, need N Hz | 43 |
| pitch: lag below filter delay | 27 |
| roll: lag below filter delay | 27 |
| roll: implausible gain | 20 |
| pitch: implausible gain | 17 |
| yaw: low excitation | 8 |
| hover segments too short for the in-flight noise at N Hz | 6 |
| pitch: low excitation | 4 |
| yaw: implausible gain | 4 |
| roll: low excitation | 3 |
| ESC RPM: all zero | 2 |
| yaw: fit failed | 2 |
| pitch: fit failed | 1 |
| roll: fit failed | 1 |
