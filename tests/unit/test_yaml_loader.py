"""C4: HSM reads YAML 1.2 numbers (field report #16)."""

from __future__ import annotations

import math

import pytest
import yaml

from hpc_sweep_manager.core.common.config import HSMConfig, SweepConfig
from hpc_sweep_manager.core.common.param_generator import ParameterGenerator
from hpc_sweep_manager.core.common.yaml_loader import dump_yaml, load_yaml


@pytest.mark.parametrize(
    "text,expected",
    [
        ("1e-1", 0.1),  # YAML 1.1: the string "1e-1"
        ("5e-2", 0.05),
        ("1E3", 1000.0),
        (".5", 0.5),
        ("-.inf", -math.inf),
        ("010", 10),  # YAML 1.1: octal 8
        ("007", 7),
        ("-3", -3),
        ("1_000", 1000),  # as YAML 1.1 and Python read it
        ("1__0", "1__0"),
        ("12:00:00", "12:00:00"),  # YAML 1.1: base-60 int 43200
        ("0x1F", "0x1F"),  # YAML 1.1: hex 31
        ("0b101", "0b101"),
        ("3.14", 3.14),
        ("yes", True),  # 1.1 bools kept for compatibility
        ("no", False),
        ("~", None),
    ],
)
def test_scalar_table(text, expected):
    assert load_yaml(f"v: {text}")["v"] == expected


def test_nan_and_plain_safe_load_untouched():
    assert math.isnan(load_yaml("v: .nan")["v"])
    assert yaml.safe_load("v: 010")["v"] == 8  # the global SafeLoader is not modified


def test_sweep_grid_yields_floats_and_decimal_ints(tmp_path):
    path = tmp_path / "sweep.yaml"
    path.write_text("sweep:\n  grid:\n    lr: [1e-1, 5e-2]\n    seed: [007, 010]\n")
    combos = ParameterGenerator(SweepConfig.from_yaml(path)).generate_combinations(None)
    assert sorted({c["lr"] for c in combos}) == [0.05, 0.1]
    assert sorted({c["seed"] for c in combos}) == [7, 10]


def test_unquoted_walltime_in_hsm_config_stays_a_string(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("slurm:\n  walltime: 12:00:00\n  gpus: 1\n")
    cfg = HSMConfig.load(config_path=path, machine_config_path=tmp_path / "none.yaml")
    assert cfg.get_slurm_spec().walltime == "12:00:00"


@pytest.mark.parametrize("value", ["1e-3", "1e3", "12:00:00", "010", "0x1f", "yes", "~", "1_000"])
def test_dumped_strings_read_back_as_strings_under_both_grammars(value):
    # A rewrite of .hsm/config.yaml must not turn a quoted string into a number, here
    # (YAML 1.2) or in an older HSM (YAML 1.1, where 12:00:00 is 43200).
    text = dump_yaml({"k": value})
    assert load_yaml(text) == {"k": value}
    assert yaml.safe_load(text) == {"k": value}


def test_dump_keeps_key_order_and_plain_values():
    data = {"z": 1, "a": {"walltime": "04:00:00", "lr": 0.001, "on": True}}
    assert load_yaml(dump_yaml(data)) == data
    assert dump_yaml(data).startswith("z: 1\na:\n")


def test_an_explicit_int_tag_keeps_its_base():
    assert load_yaml("v: !!int 0x10")["v"] == 16


@pytest.mark.parametrize(
    ("walltime", "seconds"),
    [("90", 5400), ("30:00", 1800), ("12:00:00", 43200), ("2-00", 172800), ("1-00:30", 88200)],
)
def test_parse_walltime_reads_every_slurm_form(walltime, seconds):
    from hpc_sweep_manager.core.common.utils import parse_walltime

    assert parse_walltime(walltime) == seconds


def test_an_unquoted_signal_grace_says_what_it_needs():
    from hpc_sweep_manager.core.common.resumable import ResumableConfig

    with pytest.raises(ValueError, match="signal_grace must be a whole number"):
        ResumableConfig.from_dict({"signal_grace": "5:00"})
