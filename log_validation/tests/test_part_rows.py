import math

import pytest

from log_validation.part_rows import (
    DEFAULT_MIN_VEHICLES, REASON_BELOW_THRESHOLD, REASON_NO_LOGS, PartRow, RowRules, build_part_tables)

from .part_fakes import BARO_OK, noise_entry

G = "gyro.gyro_noise_density"
GPS_H = "gps.eph_median_m"


def rows(tables, part, parameter):
    return {r.variant: r for r in tables.rows if r.part == part and r.parameter == parameter}


def imu_rows(entries, rules=RowRules()):
    return rows(build_part_tables(entries, rules), "imu", "gyro_noise_density")


def tables_with(names):
    return build_part_tables([noise_entry(f"l{i}", f"v{i}", {G: 0.001}, imu=n)
                              for i, n in enumerate(names)])


def test_default_threshold_is_ten():
    assert DEFAULT_MIN_VEHICLES == 10 and RowRules().min_vehicles == 10


def test_each_vehicle_counts_once():
    es = [noise_entry(f"a{i}", "v1", {G: 0.001}, imu="BMI088") for i in range(3)]
    es += [noise_entry("b", "v2", {G: 0.002}, imu="BMI088"),
           noise_entry("c", "v3", {G: 0.003}, imu="BMI088")]
    r = imu_rows(es)["Bmi088"]
    assert (r.logs, r.vehicles, r.median) == (5, 3, pytest.approx(0.002))
    assert r.p25 == pytest.approx(0.0015) and r.p75 == pytest.approx(0.0025)


def test_log_without_vehicle_id_is_its_own_vehicle():
    es = [noise_entry("a", None, {G: 0.001}, imu="BMI088"),
          noise_entry("b", None, {G: 0.001}, imu="BMI088")]
    assert imu_rows(es)["Bmi088"].vehicles == 2


def test_two_log_names_pool_in_one_row():
    es = [noise_entry("a", "v1", {"baro.chip_noise_sigma": 0.1, **BARO_OK}, baro="BMP388"),
          noise_entry("b", "v2", {"baro.chip_noise_sigma": 0.2, **BARO_OK}, baro="BMP390")]
    r = rows(build_part_tables(es), "baro", "noise_sigma_m")["Bmp388"]
    assert (r.log_names, r.logs, r.status) == (("BMP388", "BMP390"), 2, "existing")


def test_candidate_row_has_the_other_value_as_current():
    r = imu_rows([noise_entry("a", "v1", {G: 0.001}, imu="BMI088")])["Bmi088"]
    assert (r.status, r.current) == ("candidate", 8.726646e-4)
    assert r.log_names == ("BMI088",)


def test_below_the_vehicle_threshold():
    nine = [noise_entry(f"l{i}", f"v{i}", {G: 0.001}, imu="BMI088") for i in range(9)]
    r = imu_rows(nine, RowRules(min_vehicles=10))["Bmi088"]
    assert r.proposed is None and r.reason == REASON_BELOW_THRESHOLD == "below the vehicle threshold"
    ten = nine + [noise_entry("l9", "v9", {G: 0.001}, imu="BMI088")]
    r10 = imu_rows(ten)["Bmi088"]
    assert r10.proposed == pytest.approx(0.001) and r10.reason is None


def test_other_pools_every_part():
    chips = ["MPU6000", "ICM20689", "BMI088", "ICM20602"]
    es = [noise_entry(f"l{i}", f"v{i}", {G: 1e-3 + i * 1e-5}, imu=chips[i % 4]) for i in range(30)]
    other = imu_rows(es)["Other"]
    assert (other.vehicles, other.status, other.log_names) == (30, "other", ())
    assert other.proposed is not None


def test_unidentified_and_generic_parts_count_only_toward_other():
    es = [noise_entry("a", "v1", {G: 0.001}, imu=None),
          noise_entry("b", "v2", {G: 0.001}, imu="UAVCAN")]
    r = imu_rows(es)
    assert set(r) >= {"Other"} and "Uavcan" not in r and "Unknown" not in r
    assert r["Other"].logs == 2
    assert all(r[v].logs == 0 for v in r if v != "Other")


def test_simulated_parts_count_nowhere():
    r = imu_rows([noise_entry("a", "v1", {G: 0.001}, imu="SIM")])
    assert r["Other"].logs == 0 and "Sim" not in r
    assert all(x.logs == 0 for x in r.values())


def gps_rows(entries):
    return rows(build_part_tables(entries), "gps", "horizontal_noise_sigma_m")


def test_f9p_flown_without_corrections():
    e = noise_entry("a", "v1", {GPS_H: 1.4, "gps.fix_type_mode": 3}, gps="UBX_F9P")
    t = gps_rows([e])
    assert t["UbloxF9P"].logs == 0 and t["Other"].logs == 1
    assert t["UbloxF9P"].fix_class == "rtk_fixed"


def test_rtk_fixed_log_counts_toward_the_f9p_row():
    e = noise_entry("a", "v1", {GPS_H: 0.014, "gps.fix_type_mode": 6}, gps="UBX_F9P")
    t = gps_rows([e])
    assert t["UbloxF9P"].logs == 1 and t["Other"].logs == 0


def test_generic_gps_counts_only_toward_other():
    e = noise_entry("a", "v1", {GPS_H: 1.4, "gps.fix_type_mode": 3}, gps="UBX")
    t = gps_rows([e])
    assert "Ubx" not in t and t["Other"].logs == 1
    assert all(r.logs == 0 for v, r in t.items() if v != "Other")


def test_gps_without_fix_mode_counts_nowhere():
    t = gps_rows([noise_entry("a", "v1", {GPS_H: 1.4}, gps="UBX_M8N")])
    assert all(r.logs == 0 for r in t.values())


def test_gps_candidate_uses_standalone_class():
    e = noise_entry("a", "v1", {GPS_H: 1.0, "gps.fix_type_mode": 3}, gps="EMLID_REACH")
    r = gps_rows([e])["EmlidReach"]
    assert (r.status, r.fix_class, r.logs) == ("candidate", "standalone_3d", 1)


def test_non_gps_rows_have_no_fix_class():
    assert all(r.fix_class is None for r in build_part_tables([]).rows if r.part != "gps")


def test_existing_entries_without_logs_have_rows():
    r = imu_rows([])["Lsm6dso"]
    assert (r.logs, r.proposed, r.current) == (0, None, 6.11e-5)
    assert r.median is None and r.p25 is None and r.p75 is None


def test_row_without_samples_has_no_nan():
    for r in build_part_tables([]).rows:
        for v in (r.median, r.p25, r.p75, r.proposed):
            assert v is None or not math.isnan(v)
        assert r.reason == REASON_NO_LOGS


def test_row_order():
    assert list(imu_rows_of(["ICM20602", "BMI088"])) == \
        ["Mpu6000", "Mpu6500", "Icm20689", "Icm42688p", "Bmi270", "Lsm6dso", "Bmi088", "Icm20602", "Other"]


def imu_rows_of(names):
    return rows(tables_with(names), "imu", "gyro_noise_density")


def test_rows_are_ordered_by_part_then_variant_then_parameter():
    t = build_part_tables([])
    parts = [r.part for r in t.rows]
    assert parts == sorted(parts, key=("imu", "baro", "mag", "gps").index)
    imu = [(r.variant, r.parameter) for r in t.rows if r.part == "imu"]
    assert imu[:2] == [("Mpu6000", "gyro_noise_density"), ("Mpu6000", "accel_noise_density")]


def test_mag_none_has_no_row():
    t = build_part_tables([noise_entry("a", "v1", {"mag.noise_sigma_gauss": 1e-3}, mag="None")])
    mag = rows(t, "mag", "noise_sigma_gauss")
    assert "None" not in mag and mag["Other"].logs == 1


def test_log_ids_are_the_logs_behind_proposed_values():
    es = [noise_entry(f"id{9 - i}", f"v{i}", {G: 0.001}, imu="BMI088") for i in range(10)]
    es.append(noise_entry("lonely", "vx", {G: 0.001}, imu="SIM"))
    t = build_part_tables(es)
    assert t.log_ids == tuple(sorted(f"id{i}" for i in range(10)))


def test_log_ids_empty_without_proposed_values():
    assert build_part_tables([noise_entry("a", "v1", {G: 0.001}, imu="BMI088")]).log_ids == ()


def test_non_finite_values_are_ignored():
    es = [noise_entry("a", "v1", {G: math.nan}, imu="BMI088"),
          noise_entry("b", "v2", {G: math.inf}, imu="BMI088"),
          noise_entry("c", "v3", {G: 0.001}, imu="BMI088")]
    r = imu_rows(es)["Bmi088"]
    assert (r.logs, r.vehicles) == (1, 1) and r.log_names == ("BMI088",)
    assert imu_rows(es)["Other"].logs == 1


def test_entry_without_parts_reads_as_unnamed_and_inputs_are_not_mutated():
    e = noise_entry("a", "v1", {G: 0.001})
    del e["parts"]
    before = repr(e)
    assert imu_rows([e])["Other"].logs == 1
    assert repr(e) == before


def test_task6_fields_have_neutral_defaults():
    r = imu_rows([])["Other"]
    assert isinstance(r, PartRow)
    assert (r.checked, r.failed_slope, r.failed_resolution) == (0, 0, 0)
    assert r.median_rate_hz is None and r.median_factor is None and r.factor_flagged is False
