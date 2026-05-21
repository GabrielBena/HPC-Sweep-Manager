"""Unit tests for templating utilities."""

from __future__ import annotations

import pytest

from hpc_sweep_manager.core.common.templating import params_to_hydra_args


class TestParamsToHydraArgs:
    def test_empty_dict(self):
        assert params_to_hydra_args({}) == ""

    def test_simple_string(self):
        assert params_to_hydra_args({"name": "alice"}) == '"name=alice"'

    def test_integer(self):
        assert params_to_hydra_args({"seed": 42}) == '"seed=42"'

    def test_float(self):
        assert params_to_hydra_args({"lr": 0.001}) == '"lr=0.001"'

    def test_none_becomes_null(self):
        assert params_to_hydra_args({"checkpoint": None}) == '"checkpoint=null"'

    def test_bool_lowercased(self):
        assert params_to_hydra_args({"shuffle": True}) == '"shuffle=true"'
        assert params_to_hydra_args({"shuffle": False}) == '"shuffle=false"'

    def test_list_becomes_bracket_form(self):
        result = params_to_hydra_args({"layers": [128, 256, 512]})
        assert result == '"layers=[128, 256, 512]"'

    def test_tuple_treated_as_list(self):
        result = params_to_hydra_args({"layers": (128, 256)})
        assert result == '"layers=[128, 256]"'

    def test_string_with_space_quoted(self):
        # Already quoted by the outer "" — Hydra parses inner spaces
        assert params_to_hydra_args({"msg": "hello world"}) == '"msg=hello world"'

    def test_string_with_comma_quoted(self):
        assert params_to_hydra_args({"tags": "a,b,c"}) == '"tags=a,b,c"'

    def test_multiple_params(self):
        result = params_to_hydra_args({"lr": 0.01, "batch_size": 16})
        # Dict iteration order in Python 3.7+ is insertion order
        assert result == '"lr=0.01" "batch_size=16"'

    def test_dotted_keys_for_nested_hydra_configs(self):
        result = params_to_hydra_args({"model.hidden_size": 128})
        assert result == '"model.hidden_size=128"'

    @pytest.mark.parametrize(
        "value,expected_suffix",
        [
            (0, "=0"),
            (-1, "=-1"),
            (1e10, "=10000000000.0"),
            ("", "="),
        ],
    )
    def test_edge_value_types(self, value, expected_suffix):
        result = params_to_hydra_args({"x": value})
        assert result == f'"x{expected_suffix}"'
