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


@pytest.mark.parametrize("order", [("MS5611", "BMP388"), ("BMP388", "MS5611")])
def test_chip_tie_goes_to_the_alphabetical_first(order):
    es = [noise_entry(f"l{i}", "v1", {}, board=BOARD, baro=chip) for i, chip in enumerate(order)]
    assert _board(es)["baro"] == {"BMP388": 1.0}


def test_a_vehicle_on_two_boards_counts_once_on_each():
    es = [noise_entry("a", "v1", {}, board="A_BOARD", imu="BMI088"),
          noise_entry("b", "v1", {}, board="B_BOARD", imu="ICM42688P")]
    t = board_table(es)["boards"]
    assert (t["A_BOARD"]["vehicles"], t["A_BOARD"]["imu"]) == (1, {"BMI088": 1.0})
    assert (t["B_BOARD"]["vehicles"], t["B_BOARD"]["imu"]) == (1, {"ICM42688P": 1.0})


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


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), None, "800"])
def test_a_gyro_parameter_that_is_not_a_finite_number_is_left_out(bad):
    es = [noise_entry("a", "v1", {"param.IMU_GYRO_RATEMAX": bad}, board=BOARD),
          noise_entry("b", "v2", {"param.IMU_GYRO_RATEMAX": 800}, board=BOARD)]
    assert _board(es)["imu_gyro_ratemax_median"] == 800.0
    assert _board(es[:1])["imu_gyro_ratemax_median"] is None


def test_gyro_parameter_median_is_over_vehicle_medians():
    es = [noise_entry("a", "v1", {"param.IMU_GYRO_RATEMAX": 400}, board=BOARD),
          noise_entry("b", "v1", {"param.IMU_GYRO_RATEMAX": 400}, board=BOARD),
          noise_entry("c", "v1", {"param.IMU_GYRO_RATEMAX": 400}, board=BOARD),
          noise_entry("d", "v2", {"param.IMU_GYRO_RATEMAX": 800}, board=BOARD)]
    assert _board(es)["imu_gyro_ratemax_median"] == 600.0


@pytest.mark.parametrize("board", [None, "", 5])
def test_an_entry_without_a_readable_board_is_left_out(board):
    entry = {**noise_entry("b", "vb", {}), "hardware": {"fc_board": board}}
    assert board_table([entry]) == {"boards": {}}


def test_simulation_and_boardless_entries_are_left_out():
    simulated = {**noise_entry("s", "vs", {}, board=BOARD), "errors": [SIMULATION_ERROR]}
    empty_hardware = {**noise_entry("h", "vh", {}, board=BOARD), "hardware": {}}
    no_hardware = {k: v for k, v in noise_entry("m", "vm", {}).items() if k != "hardware"}
    counted = noise_entry("o", "vo", {}, board=BOARD)
    row = board_table([simulated, empty_hardware, no_hardware, counted])["boards"][BOARD]
    assert (row["logs"], row["vehicles"]) == (1, 1)


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
