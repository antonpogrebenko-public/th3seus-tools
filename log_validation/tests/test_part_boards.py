"""Board table: chip shares over vehicles and the gyro rate parameter medians."""

import copy
import json

import pytest

from log_validation.batch_analyze import SIMULATION_ERROR
from log_validation.part_boards import board_table

from .part_fakes import noise_entry

BOARD = "PX4_FMU_V6C"


def _board(entries, board=BOARD):
    return board_table(entries)["boards"][board]


def test_board_chip_shares():
    es = [noise_entry(f"l{i}", f"v{i}", {}, board=BOARD,
                      imu="BMI088" if i < 7 else "ICM42688P") for i in range(10)]
    b = _board(es)
    assert b["imu"] == {"BMI088": pytest.approx(0.7), "ICM42688P": pytest.approx(0.3)}
    assert (b["logs"], b["vehicles"]) == (10, 10)


def test_vehicle_counts_once_with_its_most_common_chip():
    es = [noise_entry("a", "v1", {}, board=BOARD, baro="MS5611"),
          noise_entry("b", "v1", {}, board=BOARD, baro="MS5611"),
          noise_entry("c", "v1", {}, board=BOARD, baro="BMP388")]
    b = _board(es)
    assert b["baro"] == {"MS5611": 1.0}
    assert (b["logs"], b["vehicles"]) == (3, 1)


def test_chip_tie_goes_to_the_alphabetical_first():
    es = [noise_entry("a", "v1", {}, board=BOARD, baro="MS5611"),
          noise_entry("b", "v1", {}, board=BOARD, baro="BMP388")]
    assert _board(es)["baro"] == {"BMP388": 1.0}


def test_vehicle_without_the_part_is_not_a_key():
    es = [noise_entry("a", "v1", {}, board=BOARD, mag="RM3100"),
          noise_entry("b", "v2", {}, board=BOARD, mag=None)]
    assert _board(es)["mag"] == {"RM3100": 0.5}


def test_log_without_a_vehicle_is_its_own_vehicle():
    es = [noise_entry("a", None, {}, board=BOARD), noise_entry("b", None, {}, board=BOARD)]
    assert _board(es)["vehicles"] == 2


def test_gyro_parameter_medians():
    es = [noise_entry(f"l{i}", f"v{i}", {"param.IMU_GYRO_RATEMAX": r, "param.IMU_GYRO_CUTOFF": c},
                      board=BOARD) for i, (r, c) in enumerate([(400, 30), (800, 40), (800, 40)])]
    b = _board(es)
    assert b["imu_gyro_ratemax_median"] == 800.0
    assert b["imu_gyro_cutoff_median"] == 40.0
    bare = _board([noise_entry("x", "v", {}, board=BOARD)])
    assert bare["imu_gyro_ratemax_median"] is None
    assert bare["imu_gyro_cutoff_median"] is None
    json.dumps(bare, allow_nan=False)


def test_gyro_parameter_median_is_over_vehicle_medians():
    es = [noise_entry("a", "v1", {"param.IMU_GYRO_RATEMAX": 400}, board=BOARD),
          noise_entry("b", "v1", {"param.IMU_GYRO_RATEMAX": 400}, board=BOARD),
          noise_entry("c", "v1", {"param.IMU_GYRO_RATEMAX": 400}, board=BOARD),
          noise_entry("d", "v2", {"param.IMU_GYRO_RATEMAX": 800}, board=BOARD)]
    assert _board(es)["imu_gyro_ratemax_median"] == 600.0


def test_simulation_and_unreadable_entries_are_left_out():
    sim = noise_entry("s", "vs", {}, board=BOARD)
    sim["errors"] = [SIMULATION_ERROR]
    no_hw = noise_entry("h", "vh", {}, board=BOARD)
    no_hw["hardware"] = {}
    for bad in (None, "", 5):
        e = noise_entry("b", "vb", {}, board=BOARD)
        e["hardware"] = {"fc_board": bad}
        no_hw_list = [e]
        assert board_table(no_hw_list) == {"boards": {}}
    del_hw = noise_entry("m", "vm", {}, board=BOARD)
    del del_hw["hardware"]
    ok = noise_entry("o", "vo", {}, board=BOARD)
    t = board_table([sim, no_hw, del_hw, ok])
    assert t["boards"][BOARD]["logs"] == 1
    assert t["boards"][BOARD]["vehicles"] == 1


def test_sparse_entry_does_not_raise():
    e = {"hardware": {"fc_board": BOARD}}
    b = _board([e])
    assert (b["logs"], b["imu"], b["baro"], b["mag"]) == (1, {}, {}, {})
    e2 = noise_entry("p", "v", {}, board=BOARD)
    del e2["parts"]["imu"]
    assert _board([e2])["imu"] == {}


def test_boards_and_chips_are_sorted_and_input_untouched():
    es = [noise_entry("a", "v1", {}, board="B_BOARD", imu="Z"),
          noise_entry("b", "v2", {}, board="A_BOARD", imu="Y"),
          noise_entry("c", "v3", {}, board="A_BOARD", imu="X")]
    before = copy.deepcopy(es)
    t = board_table(es)
    assert es == before
    assert list(t["boards"]) == ["A_BOARD", "B_BOARD"]
    assert list(t["boards"]["A_BOARD"]["imu"]) == ["X", "Y"]


def test_document_shape():
    es = [noise_entry("a", "v", {}, board=BOARD, imu="BMI088")]
    assert set(board_table(es)) == {"boards"}
    assert set(_board(es)) == {"logs", "vehicles", "imu", "baro", "mag",
                               "imu_gyro_ratemax_median", "imu_gyro_cutoff_median"}
