import pytest

from log_validation import part_notes
from log_validation.part_notes import COMPARISONS, NOTES, existing_arm_comment
from log_validation.th3seus_parts import OTHER, PARTS, variant

G, A = "gyro_noise_density", "accel_noise_density"
H = "horizontal_noise_sigma_m"


def test_every_text_is_mirror_text_in_comment_order():
    for part, by_name in NOTES.items():
        for name, notes in by_name.items():
            comment = variant(part, name).comment
            starts = [comment.index(n.text) for n in notes]
            assert starts == sorted(starts), (part, name)
    for part, by_name in COMPARISONS.items():
        for name, texts in by_name.items():
            assert all(t in variant(part, name).comment for t in texts), (part, name)


def test_note_fields_are_profile_fields():
    for part, by_name in NOTES.items():
        for name, notes in by_name.items():
            fields = {f for f, _ in variant(part, name).fields}
            assert all(set(n.fields) <= fields for n in notes), (part, name)


def test_every_existing_comment_is_classified():
    for part, spec in PARTS.items():
        for v in spec.variants:
            if v.name != OTHER:
                existing_arm_comment(part, v.name, frozenset())  # must not raise


def test_measured_entry_with_a_comparison():
    assert existing_arm_comment("imu", "Icm42688p", frozenset({G, A})) == "modern flagship"


def test_unmeasured_entry_with_a_comparison_has_no_comment():
    assert existing_arm_comment("baro", "Dps310", frozenset()) == ""
    assert existing_arm_comment("imu", "Mpu6500", frozenset()) == ""


def test_unmeasured_entry_without_a_comparison_is_unchanged():
    assert existing_arm_comment("imu", "Lsm6dso", frozenset()) == \
        "0.0035 deg/s/sqrt(Hz), 60 ug/sqrt(Hz)"
    here3 = variant("gps", "Here3").comment
    assert existing_arm_comment("gps", "Here3", frozenset()) == here3
    assert existing_arm_comment("gps", "Here3", frozenset({H})) == here3


def test_a_figure_for_a_field_that_keeps_its_value():
    assert existing_arm_comment("gps", "UbloxM8N", frozenset({H})) == \
        "u-blox NEO-M8N; 18 Hz max (5 Hz default); GPS+GLONASS"


def test_a_note_on_two_fields_drops_when_either_is_measured():
    assert existing_arm_comment("imu", "Mpu6000", frozenset({G})) == \
        "650 ug/sqrt(Hz); used in tests"
    assert existing_arm_comment("imu", "Mpu6000", frozenset({A})) == \
        "0.05 deg/s/sqrt(Hz); used in tests"
    assert existing_arm_comment("imu", "Mpu6000", frozenset({G, A})) == "used in tests"


def test_an_unclassified_comment_fails_naming_the_entry(monkeypatch):
    imu = {k: v for k, v in NOTES["imu"].items() if k != "Bmi270"}
    monkeypatch.setattr(part_notes, "NOTES", {**NOTES, "imu": imu})
    with pytest.raises(ValueError, match="Bmi270"):
        existing_arm_comment("imu", "Bmi270", frozenset())


def test_a_comment_with_unread_text_fails_naming_the_entry(monkeypatch):
    short = tuple(n for n in NOTES["imu"]["Mpu6000"] if n.text != "used in tests")
    monkeypatch.setattr(part_notes, "NOTES", {**NOTES, "imu": {**NOTES["imu"], "Mpu6000": short}})
    with pytest.raises(ValueError, match="Mpu6000"):
        existing_arm_comment("imu", "Mpu6000", frozenset())
