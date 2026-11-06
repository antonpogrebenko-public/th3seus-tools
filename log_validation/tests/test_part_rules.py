import math

import numpy as np
import pytest

from log_validation.noise_stats import allan_slope, resolution, white_noise_std
from log_validation.part_rows import (
    FACTOR_FLAG, REASON_BELOW_THRESHOLD, REASON_NO_LOGS, REASON_WHITE_NOISE, WHITE_NOISE_SLOPE,
    RowRules, build_part_tables, rate_factor, white_noise_failures)

from .part_fakes import BARO_OK, MAG_OK, noise_entry

G = "gyro.gyro_noise_density"
MAG_VALUE = "mag.noise_sigma_gauss"
BARO_VALUE = "baro.chip_noise_sigma"
FAIL_SLOPE = {"mag.allan_slope": -0.2}  # |-0.2 + 0.5| = 0.3 > 0.15


def table_rows(tables, part, parameter):
    return {r.variant: r for r in tables.rows if r.part == part and r.parameter == parameter}


def mag_rows(entries, rules=RowRules()):
    return table_rows(build_part_tables(entries, rules), "mag", "noise_sigma_gauss")


def baro_rows(entries, rules=RowRules()):
    return table_rows(build_part_tables(entries, rules), "baro", "noise_sigma_m")


def by(tables, part, variant):
    return next(r for r in tables.rows if r.part == part and r.variant == variant)


def mag_entry(i, evidence=None, value=0.01, name="RM3100", vehicle=None):
    m = {MAG_VALUE: value, **MAG_OK, **(evidence or {})}
    return noise_entry(f"m{i}", vehicle or f"v{i}", m, mag=name)


def mag_fleet(vehicles, failing):
    """One RM3100 log per vehicle; the first `failing` of them fail the slope check."""
    return [mag_entry(i, evidence=FAIL_SLOPE if i < failing else None) for i in range(vehicles)]


def test_constants():
    assert WHITE_NOISE_SLOPE == -0.5 and FACTOR_FLAG == 3.0


def test_white_noise_passes():
    x = np.random.default_rng(5).normal(0, 0.1, 3000)
    value = white_noise_std(x)
    assert white_noise_failures(allan_slope(x), value, resolution(x), 0.15) == (False, False)
    assert abs(value - 0.1) / 0.1 < 0.10


def test_random_walk_fails():
    x = np.cumsum(np.random.default_rng(6).normal(0, 0.1, 3000))
    assert white_noise_failures(allan_slope(x), white_noise_std(x), resolution(x), 0.15)[0]
    e = noise_entry("a", "v1", {MAG_VALUE: 0.01, "mag.allan_slope": allan_slope(x),
                                "mag.resolution_gauss": 1e-6, "mag.rate_hz": 50.0}, mag="RM3100")
    r = mag_rows([e])["Rm3100"]
    assert (r.logs, r.failed_slope, r.checked) == (0, 1, 1)


def test_quantization_fails():
    x = np.round(np.random.default_rng(7).normal(0, 0.1, 3000) / 0.5) * 0.5
    assert white_noise_failures(allan_slope(x), white_noise_std(x), resolution(x), 0.15)[1]


def test_a_log_failing_only_the_resolution_counts_nowhere():
    r = mag_rows([mag_entry(0, {"mag.resolution_gauss": 0.02}, value=0.01)])
    for name in ("Rm3100", "Other"):
        assert (r[name].checked, r[name].failed_slope, r[name].failed_resolution,
                r[name].logs) == (1, 0, 1, 0)


def test_both_conditions_can_fail_for_one_log():
    assert white_noise_failures(0.5, 1e-7, 1e-3, 0.15) == (True, True)
    e = mag_entry(0, {"mag.allan_slope": 0.5, "mag.resolution_gauss": 1.0})
    r = mag_rows([e])["Rm3100"]
    assert (r.checked, r.failed_slope, r.failed_resolution, r.logs) == (1, 1, 1, 0)


def test_slope_boundary_is_inclusive():
    assert white_noise_failures(-0.375, 1.0, 0.1, 0.125) == (False, False)
    assert white_noise_failures(-0.37, 1.0, 0.1, 0.125) == (True, False)


def test_value_equal_to_resolution_passes():
    assert white_noise_failures(-0.5, 0.1, 0.1, 0.15) == (False, False)


def test_rate_factor_is_the_square_root_of_the_ratio_at_or_above_the_simulator_rate():
    assert rate_factor(100.0, 50.0) == pytest.approx(math.sqrt(0.5))
    assert rate_factor(200.0, 50.0) == pytest.approx(0.5)
    assert rate_factor(50.0, 50.0) == 1.0


@pytest.mark.parametrize("log_rate", [1.0, 25.0, 49.9])
def test_rate_factor_is_one_below_the_simulator_rate(log_rate):
    assert rate_factor(log_rate, 50.0) == 1.0


def test_100_hz_log_to_a_50_hz_simulator():
    e = mag_entry(0, {"mag.rate_hz": 100.0})
    r = mag_rows([e], RowRules(sim_rate_mag_hz=50.0))["Rm3100"]
    assert r.median == pytest.approx(0.00707107, rel=1e-4)
    assert (r.median_rate_hz, r.median_factor) == (100.0, pytest.approx(math.sqrt(0.5)))


def test_1_hz_log_to_a_50_hz_simulator():
    e = noise_entry("a", "v1", {BARO_VALUE: 0.2, **BARO_OK, "baro.chip_rate_hz": 1.0},
                    baro="BMP388")
    r = baro_rows([e], RowRules(sim_rate_baro_hz=50.0))["Bmp388"]
    assert r.median == pytest.approx(0.2) and r.median_factor == 1.0
    assert r.median_rate_hz == 1.0 and not r.factor_flagged


def test_each_log_is_converted_with_its_own_rate_before_aggregation():
    slow = mag_entry(0, {"mag.rate_hz": 1.0}, name="RM3100")
    fast = mag_entry(1, {"mag.rate_hz": 100.0}, name="IST8310")
    t = mag_rows([slow, fast])
    assert (t["Rm3100"].median, t["Rm3100"].median_factor) == (pytest.approx(0.01), 1.0)
    assert t["Ist8310"].median == pytest.approx(0.01 * math.sqrt(0.5))
    assert t["Ist8310"].median_factor == pytest.approx(math.sqrt(0.5))
    other = t["Other"]
    assert other.median == pytest.approx((0.01 + 0.01 * math.sqrt(0.5)) / 2)
    assert other.median_factor == pytest.approx((1.0 + math.sqrt(0.5)) / 2)
    assert other.median_rate_hz == pytest.approx(50.5)


def test_barometer_uses_the_barometer_simulator_rate():
    e = noise_entry("a", "v1", {BARO_VALUE: 0.1, **BARO_OK, "baro.chip_rate_hz": 200.0},
                    baro="BMP388")
    r = baro_rows([e], RowRules(sim_rate_baro_hz=50.0, sim_rate_mag_hz=200.0))["Bmp388"]
    assert r.median == pytest.approx(0.05) and r.median_factor == pytest.approx(0.5)


def test_magnetometer_uses_the_magnetometer_simulator_rate():
    e = mag_entry(0, {"mag.rate_hz": 200.0})
    r = mag_rows([e], RowRules(sim_rate_baro_hz=200.0, sim_rate_mag_hz=50.0))["Rm3100"]
    assert r.median == pytest.approx(0.005) and r.median_factor == pytest.approx(0.5)


def test_missing_proposal_names_the_white_noise_check():
    r = mag_rows(mag_fleet(12, 5))["Rm3100"]
    assert (r.proposed, r.reason, r.failed_slope, r.checked) == (
        None, "too few logs pass the white-noise check", 5, 12)
    assert (r.logs, r.vehicles) == (7, 7)


def test_threshold_applies_to_the_passing_logs():
    r = mag_rows(mag_fleet(11, 1))["Rm3100"]
    assert (r.checked, r.failed_slope, r.vehicles) == (11, 1, 10)
    assert r.proposed == pytest.approx(0.01) and r.reason is None


def test_factor_beyond_3x_is_flagged():
    def baro(rate):
        e = noise_entry("a", "v1", {BARO_VALUE: 0.1, **BARO_OK, "baro.chip_rate_hz": rate},
                        baro="BMP388")
        return baro_rows([e])["Bmp388"]
    fast, same = baro(500.0), baro(50.0)
    assert fast.median_factor == pytest.approx(math.sqrt(0.1)) and fast.factor_flagged
    assert not same.factor_flagged
    assert not baro(150.0).factor_flagged  # factor 0.577


def test_slow_log_is_not_flagged():
    # A 5 Hz log gave factor sqrt(10) = 3.16 under the old rule; below the simulator rate
    # the factor is 1.
    e = noise_entry("a", "v1", {BARO_VALUE: 0.1, **BARO_OK, "baro.chip_rate_hz": 5.0},
                    baro="BMP388")
    r = baro_rows([e])["Bmp388"]
    assert (r.median_factor, r.factor_flagged) == (1.0, False)


def test_factor_of_exactly_one_third_is_not_flagged():
    e = noise_entry("a", "v1", {BARO_VALUE: 0.1, **BARO_OK, "baro.chip_rate_hz": 450.0},
                    baro="BMP388")
    r = baro_rows([e], RowRules(sim_rate_baro_hz=50.0))["Bmp388"]
    assert r.median_factor == 1 / FACTOR_FLAG and not r.factor_flagged


def test_value_equal_to_resolution_counts_toward_the_row():
    r = mag_rows([mag_entry(0, {"mag.resolution_gauss": 0.01}, value=0.01)])["Rm3100"]
    assert (r.checked, r.failed_resolution, r.logs) == (1, 0, 1)


def test_tolerance_option():
    es = [mag_entry(0, {"mag.allan_slope": -0.3})]
    assert mag_rows(es)["Rm3100"].failed_slope == 1
    r = mag_rows(es, RowRules(allan_slope_tolerance=0.25))["Rm3100"]
    assert (r.failed_slope, r.logs) == (0, 1)


def test_first_run_entries_count_only_where_they_can():
    old = {"log_id": "o1", "vehicle_id": "v1", "measurements": {
        G: 1e-3, MAG_VALUE: 0.01, "gps.eph_median_m": 1.0}, "errors": []}
    t = build_part_tables([old])
    assert by(t, "imu", "Other").logs == 1
    assert by(t, "mag", "Other").logs == 0 and by(t, "gps", "Other").logs == 0
    assert by(t, "mag", "Other").checked == 0


@pytest.mark.parametrize("evidence", [
    {"mag.allan_slope": math.nan}, {"mag.resolution_gauss": math.inf},
    {"mag.rate_hz": math.nan}, {"mag.rate_hz": 0.0}, {"mag.rate_hz": -50.0}])
def test_log_without_usable_evidence_is_not_eligible_anywhere(evidence):
    t = mag_rows([mag_entry(0, evidence)])
    for r in t.values():
        assert (r.checked, r.logs, r.failed_slope, r.failed_resolution) == (0, 0, 0, 0)


def test_log_missing_an_evidence_key_is_not_eligible():
    m = {MAG_VALUE: 0.01, "mag.allan_slope": -0.5, "mag.rate_hz": 50.0}
    t = mag_rows([noise_entry("a", "v1", m, mag="RM3100")])
    assert all(r.checked == 0 and r.logs == 0 for r in t.values())


def test_a_failing_log_enters_neither_its_row_nor_other():
    t = mag_rows([mag_entry(0, FAIL_SLOPE), mag_entry(1)])
    assert (t["Rm3100"].checked, t["Rm3100"].logs, t["Rm3100"].failed_slope) == (2, 1, 1)
    assert (t["Other"].checked, t["Other"].logs, t["Other"].failed_slope) == (2, 1, 1)


def test_checked_is_counted_per_row():
    es = [mag_entry(0, FAIL_SLOPE, name="RM3100"), mag_entry(1, name="RM3100"),
          mag_entry(2, name="IST8310")]
    t = mag_rows(es)
    assert t["Rm3100"].checked == 2 and t["Ist8310"].checked == 1 and t["Other"].checked == 3


def test_median_rate_and_factor_are_over_the_passing_logs():
    # Over all three logs the medians would be 200 Hz and factor 0.5.
    es = [mag_entry(0, {"mag.rate_hz": 100.0}), mag_entry(1, {"mag.rate_hz": 200.0}),
          mag_entry(2, {"mag.rate_hz": 1000.0, **FAIL_SLOPE})]
    r = mag_rows(es)["Rm3100"]
    assert r.median_rate_hz == pytest.approx(150.0)
    assert r.median_factor == pytest.approx((math.sqrt(0.5) + 0.5) / 2)


def test_imu_and_gps_rows_are_not_checked():
    es = [noise_entry("a", "v1", {G: 0.001, "gps.eph_median_m": 1.0, "gps.fix_type_mode": 3},
                      imu="BMI088", gps="UBX_M8N")]
    for r in build_part_tables(es).rows:
        if r.part in ("imu", "gps"):
            assert (r.checked, r.failed_slope, r.failed_resolution) == (0, 0, 0)
            assert r.median_rate_hz is None and r.median_factor is None and not r.factor_flagged


def test_all_logs_failing_leaves_no_nan_and_no_rate():
    r = mag_rows([mag_entry(i, FAIL_SLOPE) for i in range(3)])["Rm3100"]
    assert r.median is None and r.median_rate_hz is None and r.median_factor is None
    assert not r.factor_flagged
    for v in (r.median, r.p25, r.p75, r.proposed, r.median_rate_hz, r.median_factor):
        assert v is None or not math.isnan(v)


def test_reason_no_logs_when_nothing_was_eligible():
    r = mag_rows([mag_entry(0, {"mag.rate_hz": 0.0})])["Rm3100"]
    assert (r.proposed, r.reason, r.checked) == (None, REASON_NO_LOGS, 0)


def test_reason_when_all_eligible_logs_fail_with_3_vehicles():
    r = mag_rows(mag_fleet(3, 3))["Rm3100"]
    assert (r.checked, r.logs, r.proposed, r.reason) == (3, 0, None, REASON_BELOW_THRESHOLD)


def test_reason_when_all_eligible_logs_fail_with_12_vehicles():
    r = mag_rows(mag_fleet(12, 12))["Rm3100"]
    assert (r.checked, r.logs, r.proposed, r.reason) == (12, 0, None, REASON_WHITE_NOISE)


def test_reason_below_threshold_when_few_eligible_and_some_pass():
    r = mag_rows(mag_fleet(5, 2))["Rm3100"]
    assert (r.logs, r.reason) == (3, REASON_BELOW_THRESHOLD)


def test_failing_log_is_not_behind_a_proposed_value():
    es = mag_fleet(10, 0)
    es.append(mag_entry(99, FAIL_SLOPE))
    t = build_part_tables(es)
    r = table_rows(t, "mag", "noise_sigma_gauss")["Rm3100"]
    assert r.proposed is not None and r.vehicles == 10 and r.checked == 11
    assert "m99" not in t.log_ids
    assert {f"m{i}" for i in range(10)} <= set(t.log_ids)


def test_inputs_are_not_mutated():
    es = mag_fleet(3, 1)
    before = repr(es)
    build_part_tables(es)
    assert repr(es) == before


def test_reason_names_a_row_without_an_eligible_log():
    # A finite value without the white-noise evidence is measured, but not eligible.
    assert REASON_NO_LOGS == "no eligible log"
    m = {MAG_VALUE: 0.01, "mag.rate_hz": 50.0}
    r = mag_rows([noise_entry("a", "v1", m, mag="RM3100")])["Rm3100"]
    assert (r.reason, r.checked, r.logs) == ("no eligible log", 0, 0)
