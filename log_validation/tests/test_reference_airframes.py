"""Tests for the reference-airframe comparison."""

from __future__ import annotations

import dataclasses
import json
import math
from pathlib import Path

import pytest

from log_validation.reference_airframes import (
    FORMULA_OF,
    Kit,
    compare_kits,
    load_kits,
    log_effectiveness,
    main,
    recommendations,
    to_command_space,
)
from log_validation.th3seus_model import REFERENCE_BUILDS, predict

BUILD = REFERENCE_BUILDS["impulse_apex_5"]
PRED = predict(BUILD)


def kit(name: str = "kit_a", airframe_id: int = 4001, build=BUILD) -> Kit:
    return Kit(name=name, airframe_id=airframe_id, build=build, sources={}, missing_sources=())


def entry(log_id: str, vehicle_id, airframe_id: int = 4001, **measurements: float) -> dict:
    return {
        "log_id": log_id,
        "vehicle_id": vehicle_id,
        "airframe_id": airframe_id,
        "hardware": {},
        "measurements": dict(measurements),
        "errors": [],
    }


def vehicles(n: int, airframe_id: int = 4001, **measurements: float) -> list[dict]:
    return [entry(f"{airframe_id}-{i}", f"v{airframe_id}-{i}", airframe_id, **measurements)
            for i in range(n)]


def kits_json(tmp_path: Path, items: list) -> Path:
    path = tmp_path / "kits.json"
    path.write_text(json.dumps(items), encoding="utf-8")
    return path


def kit_item(**overrides) -> dict:
    fields = {k: v for k, v in dataclasses.asdict(BUILD).items() if v is not None}
    item = {"name": "kit_a", "airframe_id": 4001, "build": fields,
            "sources": {k: f"https://example.com/{k}" for k in fields}}
    item.update(overrides)
    return item


def row(comparison, quantity):
    return next(r for r in comparison.rows if r.quantity == quantity)


# ---- the brief's six tests ----

def test_unsourced_value_is_not_compared(tmp_path):
    item = kit_item()
    del item["sources"]["motor_kv"]
    kits = load_kits(kits_json(tmp_path, [item]))
    assert kits[0].build is None and kits[0].missing_sources == ("motor_kv",)
    result = compare_kits(kits, vehicles(6, **{"hover.command": 0.4}))[0]
    assert not result.compared and "motor_kv" in result.note and result.rows == ()


def test_too_few_vehicles():
    result = compare_kits([kit()], vehicles(3, **{"hover.command": 0.4}), min_vehicles=5)[0]
    assert result.n_vehicles == 3 and not result.compared and result.rows == ()
    assert "3 vehicles, fewer than 5" in result.note


def test_one_kit_recommends_nothing():
    comparisons = compare_kits([kit()], vehicles(6, **{"lag_minus_filter_s.roll": 0.01}))
    assert comparisons[0].compared
    assert not any(r.recommended for r in recommendations(comparisons))


def _three_kits_response(**measurements: float):
    kits = [kit(f"kit_{i}", 4001 + i) for i in range(3)]
    response = [e for i in range(3) for e in vehicles(6, 4001 + i, **measurements)]
    return kits, response


def test_consistent_lag_error_is_recommended():
    kits, response = _three_kits_response(**{"lag_minus_filter_s.roll": 0.010})
    recs = {r.quantity: r for r in recommendations(compare_kits(kits, response))}
    assert recs["lag"].recommended
    assert recs["lag"].kits == ("kit_0", "kit_1", "kit_2")
    assert recs["lag"].formula == FORMULA_OF["lag"]


LAG_LABEL = "body-rate lag minus filter delay (upper bound on motor lag)"


def test_lag_below_in_all_kits_is_not_recommended():
    kits, response = _three_kits_response(**{"lag_minus_filter_s.roll": 0.200})
    comparisons = compare_kits(kits, response)
    assert all(row(c, "lag").real_p25 > PRED.tau_motor_s for c in comparisons)
    rec = {r.quantity: r for r in recommendations(comparisons)}["lag"]
    assert not rec.recommended and rec.kits == ("kit_0", "kit_1", "kit_2")
    assert "upper bound" in rec.note


def test_lag_row_is_labelled_as_an_upper_bound():
    r = row(compare_kits([kit()], vehicles(5, **{"lag_minus_filter_s.roll": 0.01}))[0], "lag")
    assert r.label == LAG_LABEL


def test_hover_is_never_recommended():
    kits, response = _three_kits_response(**{"hover.command": 0.1})
    recs = {r.quantity: r for r in recommendations(compare_kits(kits, response))}
    assert PRED.hover_command > 0.1
    assert not recs["hover.command"].recommended
    assert "K_CT" in recs["hover.command"].note


def test_vehicle_weighting():
    response = [entry(f"a{i}", "heavy", **{"hover.command": 0.3}) for i in range(4)]
    response += [entry(f"b{i}", f"v{i}", **{"hover.command": 0.5}) for i in range(4)]
    result = compare_kits([kit()], response, min_vehicles=5)[0]
    r = row(result, "hover.command")
    assert r.real_median == pytest.approx(0.5) and r.n_vehicles == 5
    assert r.ratio == pytest.approx(PRED.hover_command / 0.5)
    assert r.predicted == pytest.approx(PRED.hover_command)


# ---- Ruling 27 ----

def test_to_command_space_f_zero_is_identity():
    assert to_command_space(100.0, 0.0, 0.4) == pytest.approx(100.0)


def test_to_command_space_f_one_scales_by_twice_hover():
    assert to_command_space(100.0, 1.0, 0.4) == pytest.approx(100.0 * 2 * 0.4)


def test_to_command_space_mid_f():
    assert to_command_space(10.0, 0.3, 0.5) == pytest.approx(10.0 * (0.7 + 2 * 0.3 * 0.5))


def test_to_command_space_missing_is_none():
    assert to_command_space(10.0, None, 0.5) is None
    assert to_command_space(10.0, 0.3, None) is None
    assert to_command_space(10.0, math.nan, 0.5) is None


def test_effectiveness_is_converted_before_comparison():
    m = {"effectiveness.roll": 100.0, "hover.thr_mdl_fac": 1.0, "hover.command": 0.5}
    result = compare_kits([kit()], vehicles(5, **m))[0]
    assert row(result, "effectiveness.roll").real_median == pytest.approx(100.0)
    m["hover.command"] = 0.4
    result = compare_kits([kit()], vehicles(5, **m))[0]
    assert row(result, "effectiveness.roll").real_median == pytest.approx(80.0)


def test_effectiveness_left_out_without_conversion_inputs():
    result = compare_kits([kit()], vehicles(5, **{"effectiveness.roll": 100.0}))[0]
    assert not any(r.quantity.startswith("effectiveness") for r in result.rows)


# ---- Ruling 26 ----

def test_bat_scale_en_one_excludes_effectiveness():
    base = {"effectiveness.roll": 50.0, "hover.thr_mdl_fac": 0.0, "hover.command": 0.4}
    response = vehicles(5, bat_scale_en=1.0, **base)
    result = compare_kits([kit()], response)[0]
    assert not any(r.quantity.startswith("effectiveness") for r in result.rows)


def test_bat_scale_en_zero_or_absent_keeps_effectiveness():
    base = {"effectiveness.roll": 50.0, "hover.thr_mdl_fac": 0.0, "hover.command": 0.4}
    for extra in ({"bat_scale_en": 0.0}, {}):
        result = compare_kits([kit()], vehicles(5, **base, **extra))[0]
        assert row(result, "effectiveness.roll").real_median == pytest.approx(50.0)


def test_bat_scale_en_does_not_exclude_other_quantities():
    result = compare_kits([kit()], vehicles(
        5, bat_scale_en=1.0, **{"hover.command": 0.4, "lag_minus_filter_s.roll": 0.01}))[0]
    assert {r.quantity for r in result.rows} == {"hover.command", "lag"}


# ---- Ruling 4 ----

def test_lag_is_mean_of_roll_and_pitch():
    m = {"lag_minus_filter_s.roll": 0.01, "lag_minus_filter_s.pitch": 0.03}
    r = row(compare_kits([kit()], vehicles(5, **m))[0], "lag")
    assert r.real_median == pytest.approx(0.02) and r.predicted == pytest.approx(PRED.tau_motor_s)


def test_lag_with_one_axis_uses_it():
    only_pitch = vehicles(5, **{"lag_minus_filter_s.pitch": 0.03})
    r = row(compare_kits([kit()], only_pitch)[0], "lag")
    assert r.real_median == pytest.approx(0.03)


def test_lag_skipped_when_neither_axis():
    result = compare_kits([kit()], vehicles(5, **{"hover.command": 0.4}))[0]
    assert {r.quantity for r in result.rows} == {"hover.command"}


# ---- matching and weighting details ----

def test_logs_match_kit_by_airframe_id():
    response = vehicles(5, 4001, **{"hover.command": 0.4}) + vehicles(7, 9999, **{"hover.command": 0.4})
    result = compare_kits([kit()], response)[0]
    assert result.n_vehicles == 5


def test_log_without_vehicle_id_is_its_own_vehicle():
    response = [entry(f"l{i}", None, **{"hover.command": 0.4}) for i in range(5)]
    result = compare_kits([kit()], response)[0]
    assert result.n_vehicles == 5 and result.compared


def test_log_key_does_not_collide_with_vehicle_id():
    response = [entry("x", None, **{"hover.command": 0.4}),
                entry("y", "x", **{"hover.command": 0.4})]
    assert compare_kits([kit()], response, min_vehicles=1)[0].n_vehicles == 2


def test_percentiles_over_vehicles():
    response = [entry(f"l{i}", f"v{i}", **{"hover.command": 0.1 * (i + 1)}) for i in range(5)]
    r = row(compare_kits([kit()], response)[0], "hover.command")
    assert (r.real_p25, r.real_median, r.real_p75) == pytest.approx((0.2, 0.3, 0.4))


def test_n_vehicles_counts_contributors_per_quantity():
    response = vehicles(5, **{"hover.command": 0.4}) + [entry("extra", "vx")]
    result = compare_kits([kit()], response)[0]
    assert result.n_vehicles == 6 and row(result, "hover.command").n_vehicles == 5


def test_inputs_are_not_mutated():
    response = vehicles(5, **{"hover.command": 0.4})
    snapshot = json.dumps(response, sort_keys=True)
    compare_kits([kit()], response)
    assert json.dumps(response, sort_keys=True) == snapshot


# ---- recommendations ----

def test_mixed_directions_not_recommended():
    kits = [kit("kit_0", 4001), kit("kit_1", 4002)]
    response = (vehicles(6, 4001, **{"lag_minus_filter_s.roll": 0.010})
                + vehicles(6, 4002, **{"lag_minus_filter_s.roll": 0.080}))
    recs = {r.quantity: r for r in recommendations(compare_kits(kits, response))}
    assert not recs["lag"].recommended and "no change" in recs["lag"].note.lower()


def test_prediction_inside_iqr_not_recommended():
    kits, response = _three_kits_response(**{"lag_minus_filter_s.roll": PRED.tau_motor_s})
    recs = {r.quantity: r for r in recommendations(compare_kits(kits, response))}
    assert not recs["lag"].recommended


def test_effectiveness_recommended_when_all_axes_consistent():
    m = {f"effectiveness.{a}": 1.0 for a in ("roll", "pitch", "yaw")}
    kits, response = _three_kits_response(**m, **{"hover.thr_mdl_fac": 0.0, "hover.command": 0.4})
    recs = {r.quantity: r for r in recommendations(compare_kits(kits, response))}
    assert recs["effectiveness"].recommended and len(recs["effectiveness"].kits) == 3


def test_uncompared_kits_are_ignored_by_recommendations():
    kits, response = _three_kits_response(**{"lag_minus_filter_s.roll": 0.010})
    comparisons = compare_kits(kits, response[: 6 * 2] + response[12:15], min_vehicles=5)
    recs = {r.quantity: r for r in recommendations(comparisons)}
    assert recs["lag"].recommended and recs["lag"].kits == ("kit_0", "kit_1")


# ---- load_kits validation ----

def test_load_kits_empty_bundled_table():
    bundled = Path(__file__).parent.parent / "reference_kits.json"
    assert load_kits(bundled) == []


def test_load_kits_good_kit(tmp_path):
    k = load_kits(kits_json(tmp_path, [kit_item()]))[0]
    assert k.build == BUILD and k.missing_sources == () and k.airframe_id == 4001


def test_load_kits_malformed_json(tmp_path):
    path = tmp_path / "kits.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON"):
        load_kits(path)


def test_load_kits_not_a_list(tmp_path):
    with pytest.raises(ValueError, match="list"):
        load_kits(kits_json(tmp_path, {"name": "x"}))


def test_load_kits_unknown_build_field(tmp_path):
    item = kit_item()
    item["build"]["wingspan_m"] = 1.0
    with pytest.raises(ValueError, match=r"kit_a.*wingspan_m"):
        load_kits(kits_json(tmp_path, [item]))


def test_load_kits_bad_value_names_kit_and_field(tmp_path):
    item = kit_item()
    item["build"]["motor_kv"] = -5
    with pytest.raises(ValueError, match=r"kit_a.*motor_kv"):
        load_kits(kits_json(tmp_path, [item]))


def test_load_kits_non_numeric_value(tmp_path):
    item = kit_item()
    item["build"]["motor_kv"] = "fast"
    with pytest.raises(ValueError, match=r"kit_a.*motor_kv"):
        load_kits(kits_json(tmp_path, [item]))


def test_load_kits_missing_required_field(tmp_path):
    item = kit_item()
    del item["build"]["frame_weight_g"]
    with pytest.raises(ValueError, match=r"kit_a.*frame_weight_g"):
        load_kits(kits_json(tmp_path, [item]))


def test_load_kits_source_for_unknown_field(tmp_path):
    item = kit_item()
    item["sources"]["wingspan_m"] = "https://example.com"
    with pytest.raises(ValueError, match=r"kit_a.*wingspan_m"):
        load_kits(kits_json(tmp_path, [item]))


def test_load_kits_bad_entry_shape(tmp_path):
    with pytest.raises(ValueError, match="name"):
        load_kits(kits_json(tmp_path, [{"airframe_id": 1, "build": {}, "sources": {}}]))
    with pytest.raises(ValueError, match="airframe_id"):
        load_kits(kits_json(tmp_path, [kit_item(airframe_id="x")]))


# ---- CLI ----

def test_cli_round_trip_has_no_private_paths(tmp_path, capsys):
    response_path = tmp_path / "response.json"
    response_path.write_text(json.dumps(vehicles(6, **{"hover.command": 0.3})), encoding="utf-8")
    kits_path = kits_json(tmp_path, [kit_item()])
    out = tmp_path / "out" / "comparison.json"
    code = main([str(response_path), "--kits", str(kits_path), "-o", str(out),
                 "--min-vehicles", "5"])
    assert code == 0
    text = out.read_text(encoding="utf-8")
    assert str(tmp_path) not in text and str(Path.home()) not in text
    data = json.loads(text)
    assert data["comparisons"][0]["kit"] == "kit_a" and data["comparisons"][0]["compared"]
    assert {r["quantity"] for r in data["recommendations"]} == set(FORMULA_OF)


def test_cli_missing_input_exits_2(tmp_path, capsys):
    code = main([str(tmp_path / "nope.json"), "--kits", str(kits_json(tmp_path, [])),
                 "-o", str(tmp_path / "o.json")])
    assert code == 2 and capsys.readouterr().err.startswith("Error:")


def test_cli_bad_kits_exits_2(tmp_path, capsys):
    response_path = tmp_path / "response.json"
    response_path.write_text("[]", encoding="utf-8")
    bad = tmp_path / "kits.json"
    bad.write_text("{", encoding="utf-8")
    code = main([str(response_path), "--kits", str(bad), "-o", str(tmp_path / "o.json")])
    assert code == 2 and "Error:" in capsys.readouterr().err


# ---- fix round 1 ----

def _lag_by_vehicle(airframe_id: int, lags: list[float]) -> list[dict]:
    return [entry(f"{airframe_id}-{i}", f"v{airframe_id}-{i}", airframe_id,
                  **{"lag_minus_filter_s.roll": lag}) for i, lag in enumerate(lags)]


def test_note_names_compared_kit_without_data():
    kits = [kit("kit_0", 4001), kit("kit_1", 4002)]
    response = vehicles(6, 4001, **{"lag_minus_filter_s.roll": 0.010}) + vehicles(
        6, 4002, **{"hover.command": 0.3})
    recs = {r.quantity: r for r in recommendations(compare_kits(kits, response))}
    assert not recs["lag"].recommended
    assert "no change recommended" in recs["lag"].note and "kit_1" in recs["lag"].note
    assert "kit_0" in recs["hover.command"].note and "kit_1" not in recs["hover.command"].note
    assert "kit_1" in recs["effectiveness"].note and "kit_0" in recs["effectiveness"].note
    assert "1 compared kit has data" in recs["lag"].note


def test_consistent_note_names_kit_without_data():
    kits = [kit(f"kit_{i}", 4001 + i) for i in range(3)]
    response = (vehicles(6, 4001, **{"lag_minus_filter_s.roll": 0.010})
                + vehicles(6, 4002, **{"lag_minus_filter_s.roll": 0.010})
                + vehicles(6, 4003, **{"hover.command": 0.3}))
    recs = {r.quantity: r for r in recommendations(compare_kits(kits, response))}
    assert recs["lag"].recommended and "kit_2" in recs["lag"].note


def test_load_kits_refuses_shared_airframe_id(tmp_path):
    items = [kit_item(name="kit_a"), kit_item(name="kit_b")]
    with pytest.raises(ValueError, match=r"kit_a.*kit_b.*airframe_id"):
        load_kits(kits_json(tmp_path, items))


def test_load_kits_refuses_shared_name(tmp_path):
    items = [kit_item(), kit_item(airframe_id=4002)]
    with pytest.raises(ValueError, match=r"kit_a.*kit_a.*name"):
        load_kits(kits_json(tmp_path, items))


def _three_kit_lags(lags: list[float]):
    kits = [kit(f"kit_{i}", 4001 + i) for i in range(3)]
    response = [e for i in range(3) for e in _lag_by_vehicle(4001 + i, lags)]
    return kits, response


def test_prediction_exactly_on_p75_is_inside():
    p = PRED.tau_motor_s
    kits, response = _three_kit_lags([0.001, 0.002, 0.003, p, 0.2])
    comparisons = compare_kits(kits, response)
    assert row(comparisons[0], "lag").real_p75 == p
    assert not {r.quantity: r for r in recommendations(comparisons)}["lag"].recommended


def test_prediction_exactly_on_p25_is_inside():
    p = PRED.tau_motor_s
    kits, response = _three_kit_lags([0.001, p, 0.3, 0.4, 0.5])
    comparisons = compare_kits(kits, response)
    assert row(comparisons[0], "lag").real_p25 == p
    assert not {r.quantity: r for r in recommendations(comparisons)}["lag"].recommended


def test_effectiveness_recommended_with_roll_and_pitch_only():
    m = {"effectiveness.roll": 1.0, "effectiveness.pitch": 1.0,
         "hover.thr_mdl_fac": 0.0, "hover.command": 0.4}
    kits, response = _three_kits_response(**m)
    comparisons = compare_kits(kits, response)
    assert not any(r.quantity.endswith("yaw") for r in comparisons[0].rows)
    recs = {r.quantity: r for r in recommendations(comparisons)}
    assert recs["effectiveness"].recommended


def test_effectiveness_with_one_axis_inside_is_not_recommended():
    m = {"effectiveness.roll": 1.0, "effectiveness.pitch": PRED.effectiveness_pitch,
         "hover.thr_mdl_fac": 0.0, "hover.command": 0.4}
    kits, response = _three_kits_response(**m)
    recs = {r.quantity: r for r in recommendations(compare_kits(kits, response))}
    assert not recs["effectiveness"].recommended


def test_to_command_space_refuses_non_positive_hover():
    assert to_command_space(10.0, 0.3, 0.0) is None
    assert to_command_space(10.0, 0.3, -0.2) is None


def test_effectiveness_dropped_for_zero_hover_command():
    m = {"effectiveness.roll": 50.0, "hover.thr_mdl_fac": 0.3, "hover.command": 0.0}
    result = compare_kits([kit()], vehicles(5, **m))[0]
    assert not any(r.quantity.startswith("effectiveness") for r in result.rows)


def test_f_zero_keeps_effectiveness_without_hover_command():
    assert log_effectiveness({"effectiveness.roll": 200, "hover.thr_mdl_fac": 0.0}, "roll") == 200
    assert to_command_space(10.0, 0.0, None) == 10.0
    assert to_command_space(10.0, 0.0, 0.0) == 10.0


def test_missing_thr_mdl_fac_still_drops_effectiveness():
    assert log_effectiveness({"effectiveness.roll": 200, "hover.command": 0.4}, "roll") is None


def test_non_positive_effectiveness_is_dropped():
    for value in (0.0, -5.0):
        m = {"effectiveness.roll": value, "hover.thr_mdl_fac": 0.0, "hover.command": 0.4}
        result = compare_kits([kit()], vehicles(5, **m))[0]
        assert not any(r.quantity.startswith("effectiveness") for r in result.rows)


def test_converted_non_positive_effectiveness_is_dropped():
    m = {"effectiveness.roll": 50.0, "hover.thr_mdl_fac": 3.0, "hover.command": 0.1}
    result = compare_kits([kit()], vehicles(5, **m))[0]
    assert not any(r.quantity.startswith("effectiveness") for r in result.rows)
