"""Unit tests for :mod:`core.hpc.gpu_planner` — heterogeneous GPU scheduling.

The fixtures mirror the live use case from issue #7: a 22-task sweep with
bimodal costs (T=5 arms ≈ 7h, T=16 arms ≈ 23h on A100) split across
``{a100: 1.0, h200: 0.4}``.
"""

from __future__ import annotations

import pytest

from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.common.utils import parse_walltime
from hpc_sweep_manager.core.hpc.gpu_planner import (
    GpuTypePlan,
    build_array_submissions,
    jobs_manifest_entries,
    plan_gpu_split,
    task_costs,
)


# ------------------------------------------------------------------ task_costs


class TestTaskCosts:
    def test_no_cost_param_is_uniform(self):
        assert task_costs([{"a": 1}, {"a": 2}], None) == [1.0, 1.0]

    def test_numeric_param_values_used_directly(self):
        params = [{"T": 5}, {"T": 16}, {"T": 5}]
        assert task_costs(params, "T") == [5.0, 16.0, 5.0]

    def test_cost_map_translates_values(self):
        # The live calibration: T=5 → 7h, T=16 → 23h (ratios matter).
        params = [{"T": 5}, {"T": 16}]
        assert task_costs(params, "T", {5: 7.0, 16: 23.0}) == [7.0, 23.0]

    def test_cost_map_string_key_mismatch_tolerated(self):
        # YAML round-trips can stringify keys.
        assert task_costs([{"T": 5}], "T", {"5": 7.0}) == [7.0]

    def test_missing_param_defaults_to_max_usable_with_warning(self, caplog):
        # Unknown cost = treated as EXPENSIVE (max usable) — its bin's
        # walltime must never be scaled down by it (TIMEOUT bias).
        with caplog.at_level("WARNING"):
            costs = task_costs([{"T": 5}, {"other": 1}], "T")
        assert costs == [5.0, 5.0]
        assert any("Task(s): 2" in r.message for r in caplog.records)
        assert any("TIMEOUT" in r.message for r in caplog.records)

    def test_all_defaulted_costs_are_one(self, caplog):
        with caplog.at_level("WARNING"):
            assert task_costs([{"a": 1}, {"a": 2}], "T") == [1.0, 1.0]

    def test_defaulted_cost_cannot_collapse_walltime(self):
        # The review's EXP B: a defaulted task alone in a bin used to get
        # walltime = base × (1/global_max). Now it carries max cost.
        costs = task_costs(
            [{"T": 16}, {"T": 16}, {"T": 5}], "T", {16: 23.0}  # 5 missing!
        )
        assert costs == [23.0, 23.0, 23.0]
        plans = plan_gpu_split(
            costs=costs,
            gpu_types=["A100", "H200"],
            speed_factors={"a100": 1.0, "h200": 0.4},
            base_walltime="23:00:00",
        )
        for p in plans:
            assert parse_walltime(p.walltime) >= parse_walltime("09:12:00") * 0.999

    def test_non_numeric_without_map_defaults(self, caplog):
        with caplog.at_level("WARNING"):
            assert task_costs([{"T": "big"}], "T") == [1.0]
        assert len(caplog.records) == 1

    def test_value_absent_from_map_defaults(self, caplog):
        with caplog.at_level("WARNING"):
            assert task_costs([{"T": 99}], "T", {5: 7.0}) == [1.0]

    def test_bool_is_not_a_cost(self, caplog):
        with caplog.at_level("WARNING"):
            assert task_costs([{"T": True}], "T") == [1.0]

    def test_non_positive_defaults(self, caplog):
        with caplog.at_level("WARNING"):
            assert task_costs([{"T": 0}, {"T": -3}], "T") == [1.0, 1.0]


# -------------------------------------------------------------- plan_gpu_split


class TestPlanGpuSplitUniform:
    def test_counts_inverse_to_factor(self):
        # The live shape: 22 uniform tasks over a100 (1.0) + h200 (0.4).
        # Balanced completion ⇒ counts ∝ 1/factor ⇒ ~6 on a100, ~16 on h200.
        plans = plan_gpu_split(
            costs=[1.0] * 22,
            gpu_types=["A100", "H200"],
            speed_factors={"a100": 1.0, "h200": 0.4},
        )
        by_type = {p.gpu_type: p for p in plans}
        assert set(by_type) == {"A100", "H200"}
        assert len(by_type["H200"].indices) > len(by_type["A100"].indices)
        assert 5 <= len(by_type["A100"].indices) <= 8
        # Every task assigned exactly once.
        all_indices = sorted(i for p in plans for i in p.indices)
        assert all_indices == list(range(22))

    def test_equal_factors_split_evenly(self):
        plans = plan_gpu_split(costs=[1.0] * 10, gpu_types=["a", "b"])
        sizes = sorted(len(p.indices) for p in plans)
        assert sizes == [5, 5]

    def test_deterministic(self):
        kwargs = dict(
            costs=[3.0, 1.0, 2.0, 1.0, 5.0],
            gpu_types=["x", "y"],
            speed_factors={"x": 1.0, "y": 0.5},
        )
        assert plan_gpu_split(**kwargs) == plan_gpu_split(**kwargs)

    def test_more_types_than_tasks_drops_empty_bins(self):
        plans = plan_gpu_split(costs=[1.0], gpu_types=["a", "b", "c"])
        assert len(plans) == 1
        assert plans[0].indices == (0,)

    def test_partial_factor_map_is_a_hard_error(self):
        # The review's EXP I: a slow type missing from a PROVIDED map would
        # default to 1.0 → over-assigned work + under-provisioned walltime
        # → mass TIMEOUT. A partial map is a config bug: refuse.
        with pytest.raises(ValueError, match="no entry for gpu type.*TIMEOUT"):
            plan_gpu_split(
                costs=[1.0, 1.0], gpu_types=["a", "b"], speed_factors={"a": 1.0}
            )

    def test_no_factor_map_warns_naming_timeout(self, caplog):
        # No map at all = "all types equally fast" — allowed, but the
        # warning must name the consequence.
        with caplog.at_level("WARNING"):
            plans = plan_gpu_split(costs=[1.0, 1.0], gpu_types=["a", "b"])
        assert any("TIMEOUT" in r.message for r in caplog.records)
        assert {p.speed_factor for p in plans} == {1.0}

    def test_invalid_factor_raises(self):
        with pytest.raises(ValueError, match="must be > 0"):
            plan_gpu_split(
                costs=[1.0], gpu_types=["a"], speed_factors={"a": 0}
            )

    def test_empty_types_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            plan_gpu_split(costs=[1.0], gpu_types=[])

    def test_factor_keys_case_insensitive_type_case_preserved(self):
        # Config says lowercase, GRES needs the cased form (gotcha #6).
        plans = plan_gpu_split(
            costs=[1.0], gpu_types=["A100"], speed_factors={"a100": 2.0}
        )
        assert plans[0].gpu_type == "A100"
        assert plans[0].speed_factor == 2.0


class TestPlanGpuSplitLPT:
    # Live bimodal sweep: 7 long arms (23h) + 15 short arms (7h).
    _COSTS = [23.0] * 7 + [7.0] * 15

    def test_long_arms_prefer_fast_type(self):
        plans = plan_gpu_split(
            costs=self._COSTS,
            gpu_types=["A100", "H200"],
            speed_factors={"a100": 1.0, "h200": 0.4},
        )
        by_type = {p.gpu_type: p for p in plans}
        h200_costs = [self._COSTS[i] for i in by_type["H200"].indices]
        # The very first (costliest) task must land on the fastest bin, and
        # the fast bin carries the majority of the long arms.
        assert 0 in by_type["H200"].indices
        assert sum(1 for c in h200_costs if c == 23.0) >= 5

    def test_makespan_no_worse_than_single_type(self):
        plans = plan_gpu_split(
            costs=self._COSTS,
            gpu_types=["A100", "H200"],
            speed_factors={"a100": 1.0, "h200": 0.4},
        )
        makespan = max(
            sum(self._COSTS[i] for i in p.indices) * p.speed_factor for p in plans
        )
        all_on_a100 = sum(self._COSTS) * 1.0
        assert makespan < all_on_a100

    def test_indices_ascending_within_plan(self):
        plans = plan_gpu_split(costs=self._COSTS, gpu_types=["a", "b"])
        for p in plans:
            assert list(p.indices) == sorted(p.indices)


class TestWalltimeScaling:
    def test_factor_scales_base(self):
        # base 18h on the factor-1.0 reference; h200 at 0.4 → 07:12:00.
        plans = plan_gpu_split(
            costs=[1.0] * 4,
            gpu_types=["H200"],
            speed_factors={"h200": 0.4},
            base_walltime="18:00:00",
        )
        assert plans[0].walltime == "07:12:00"

    def test_bin_max_cost_ratio_scales(self):
        # Two types; the slow-arm bin (max 7 of global max 23) on factor 1.0:
        # 18h × 1.0 × 7/23 = 19721.7s → ceil to minute = 19740s = 05:29:00.
        plans = plan_gpu_split(
            costs=[23.0, 7.0],
            gpu_types=["fast", "slow"],
            speed_factors={"fast": 0.4, "slow": 1.0},
            base_walltime="18:00:00",
        )
        by_type = {p.gpu_type: p for p in plans}
        # LPT: 23 → fast; 7 → whichever minimizes; (0+7)*1.0=7 < (23+7)*0.4=12 → slow.
        assert by_type["slow"].walltime == "05:29:00"
        # fast bin holds the global max cost → pure factor scaling.
        assert by_type["fast"].walltime == "07:12:00"

    def test_floor_ten_minutes(self):
        plans = plan_gpu_split(
            costs=[1.0],
            gpu_types=["t"],
            speed_factors={"t": 0.001},
            base_walltime="01:00:00",
        )
        assert parse_walltime(plans[0].walltime) == 600

    def test_slower_than_base_allowed(self):
        # l4-style factor 3.0 → walltime LONGER than base, deliberately uncapped.
        plans = plan_gpu_split(
            costs=[1.0],
            gpu_types=["L4"],
            speed_factors={"l4": 3.0},
            base_walltime="02:00:00",
        )
        assert plans[0].walltime == "06:00:00"

    def test_no_base_walltime_stays_none(self):
        plans = plan_gpu_split(costs=[1.0], gpu_types=["t"])
        assert plans[0].walltime is None


# ----------------------------------------------------- build_array_submissions


class TestBuildArraySubmissions:
    _PARAMS = [{"seed": i} for i in range(4)]

    def test_single_type_is_legacy_shape(self):
        spec = ResourceSpec(gpus=1, gpu_type="H100", walltime="02:00:00")
        subs = build_array_submissions(
            params_list=self._PARAMS, effective_spec=spec, prefix="sw"
        )
        assert len(subs) == 1
        sub = subs[0]
        assert sub.job_name == "sw_array"
        assert sub.params_filename == "parameter_combinations.json"
        assert sub.gpu_type is None
        assert sub.spec is spec  # untouched — byte-identical scalar path
        assert [e["index"] for e in sub.entries] == [1, 2, 3, 4]
        assert [e["global_index"] for e in sub.entries] == [1, 2, 3, 4]

    def test_multi_type_partitions_with_local_and_global_indices(self):
        spec = ResourceSpec(gpus=1, gpu_type=("A100", "H200"), walltime="10:00:00")
        subs = build_array_submissions(
            params_list=self._PARAMS,
            effective_spec=spec,
            prefix="sw",
            speed_factors={"a100": 1.0, "h200": 1.0},
        )
        assert {s.job_name for s in subs} == {"sw_array_A100", "sw_array_H200"}
        assert {s.params_filename for s in subs} == {
            "parameter_combinations_A100.json",
            "parameter_combinations_H200.json",
        }
        for sub in subs:
            # Scalarized spec, renderable directly.
            assert isinstance(sub.spec.gpu_type, str)
            # index is array-local 1..k; global_index keeps original position.
            assert [e["index"] for e in sub.entries] == list(
                range(1, len(sub.entries) + 1)
            )
            for e in sub.entries:
                assert self._PARAMS[e["global_index"] - 1] == e["params"]
        # Union of global indices covers every task exactly once.
        all_globals = sorted(
            e["global_index"] for s in subs for e in s.entries
        )
        assert all_globals == [1, 2, 3, 4]

    def test_multi_type_scales_walltime_per_sub_spec(self):
        spec = ResourceSpec(gpus=1, gpu_type=("A100", "H200"), walltime="10:00:00")
        subs = build_array_submissions(
            params_list=self._PARAMS,
            effective_spec=spec,
            prefix="sw",
            speed_factors={"a100": 1.0, "h200": 0.5},
        )
        by_type = {s.gpu_type: s for s in subs}
        assert by_type["A100"].spec.walltime == "10:00:00"
        assert by_type["H200"].spec.walltime == "05:00:00"

    def test_costs_length_mismatch_raises(self):
        spec = ResourceSpec(gpus=1, gpu_type=("a", "b"))
        with pytest.raises(ValueError, match="costs"):
            build_array_submissions(
                params_list=self._PARAMS,
                effective_spec=spec,
                prefix="sw",
                costs=[1.0],
            )

    def test_unsafe_type_token_sanitized(self):
        spec = ResourceSpec(gpus=1, gpu_type=("weird type!", "ok"))
        subs = build_array_submissions(
            params_list=self._PARAMS, effective_spec=spec, prefix="sw"
        )
        names = {s.job_name for s in subs}
        assert "sw_array_weirdtype" in names
        # The SPEC keeps the original string (rendered into --gres verbatim).
        assert {s.spec.gpu_type for s in subs} == {"weird type!", "ok"}


class TestJobsManifestEntries:
    def test_entries_from_jobinfo_params(self):
        entries = jobs_manifest_entries(
            ["1", "2"],
            {
                "1": {"_array_size": 6, "_gpu_type": "A100"},
                "2": {"_array_size": 16, "_gpu_type": "H200"},
            },
        )
        assert entries == [
            {"job_id": "1", "gpu_type": "A100", "num_tasks": 6},
            {"job_id": "2", "gpu_type": "H200", "num_tasks": 16},
        ]

    def test_individual_jobs_default_to_one_task(self):
        assert jobs_manifest_entries(["7"], {"7": {"seed": 1}}) == [
            {"job_id": "7", "gpu_type": None, "num_tasks": 1}
        ]


class TestHardenings:
    """Findings from the post-merge cold review of PR #10."""

    def test_bool_param_never_matches_int_cost_map_key(self):
        # True == 1 in Python; the map path must not exploit that.
        assert task_costs([{"T": True}], "T", {1: 99.0}) == [1.0]

    def test_token_collision_raises_not_clobbers(self):
        # Two types sanitizing to one token would share a params file —
        # the second write clobbers the first → silent wrong-config.
        spec = ResourceSpec(gpus=1, gpu_type=("h100!", "h100"))
        with pytest.raises(ValueError, match="collide after"):
            build_array_submissions(
                params_list=[{"s": 1}, {"s": 2}], effective_spec=spec, prefix="sw"
            )

    def test_two_part_base_walltime_rejected_in_multi_type(self):
        # parse_walltime reads "48:00" as 48 MINUTES — scaled across
        # sub-arrays that's a silent 60x under-provision.
        with pytest.raises(ValueError, match="HH:MM:SS"):
            plan_gpu_split(
                costs=[1.0], gpu_types=["a"], base_walltime="48:00"
            )

    def test_all_zero_costs_no_zerodivision(self):
        # Pure-API guard: task_costs never emits zeros, but direct callers can.
        plans = plan_gpu_split(
            costs=[0.0, 0.0], gpu_types=["a"], base_walltime="01:00:00"
        )
        assert plans[0].walltime is not None  # didn't raise

    def test_submission_carries_planner_speed_factor(self):
        # Display layers read THIS — never re-derive from config.
        spec = ResourceSpec(gpus=1, gpu_type=("A100", "H200"), walltime="10:00:00")
        subs = build_array_submissions(
            params_list=[{"s": i} for i in range(4)],
            effective_spec=spec,
            prefix="sw",
            speed_factors={"a100": 1.0, "h200": 0.5},
        )
        by_type = {s.gpu_type: s for s in subs}
        assert by_type["A100"].speed_factor == 1.0
        assert by_type["H200"].speed_factor == 0.5


class TestNormalizeSpeedFactors:
    def test_normalizes_keys_and_values(self):
        from hpc_sweep_manager.core.hpc.gpu_planner import normalize_speed_factors

        assert normalize_speed_factors({"A100": "1.0", "h200": 0.4}) == {
            "a100": 1.0,
            "h200": 0.4,
        }

    def test_drops_nonpositive_and_nonfinite(self, caplog):
        from hpc_sweep_manager.core.hpc.gpu_planner import normalize_speed_factors

        with caplog.at_level("WARNING"):
            out = normalize_speed_factors(
                {"a": 0, "b": -1, "c": float("inf"), "d": float("nan"), "e": 2.0}
            )
        assert out == {"e": 2.0}
        assert len(caplog.records) == 4

    def test_non_mapping_and_empty_are_none(self):
        from hpc_sweep_manager.core.hpc.gpu_planner import normalize_speed_factors

        assert normalize_speed_factors(None) is None
        assert normalize_speed_factors({}) is None
        assert normalize_speed_factors([("a", 1)]) is None
        assert normalize_speed_factors({"a": "fast"}) is None


class TestSweepConfigCostParamValidation:
    def test_cost_param_must_be_swept(self):
        from hpc_sweep_manager.core.common.config import SweepConfig

        cfg = SweepConfig.from_dict(
            {"sweep": {"grid": {"a": [1, 2]}, "cost_param": "typo.T"}}
        )
        errors = cfg.validate()
        assert any("cost_param" in e for e in errors)

    def test_cost_param_in_grid_ok(self):
        from hpc_sweep_manager.core.common.config import SweepConfig

        cfg = SweepConfig.from_dict(
            {"sweep": {"grid": {"T": [1, 2]}, "cost_param": "T"}}
        )
        assert cfg.validate() == []

    def test_cost_param_in_paired_ok(self):
        from hpc_sweep_manager.core.common.config import SweepConfig

        cfg = SweepConfig.from_dict(
            {
                "sweep": {
                    "grid": {},
                    "paired": [{"g": {"T": [1, 2], "alpha": [0.1, 0.2]}}],
                    "cost_param": "T",
                }
            }
        )
        assert cfg.validate() == []


class TestSweepConfigCostFields:
    def test_from_dict_parses_cost_fields(self):
        from hpc_sweep_manager.core.common.config import SweepConfig

        cfg = SweepConfig.from_dict(
            {"sweep": {"grid": {"T": [5, 16]}, "cost_param": "T", "cost_map": {5: 7.0}}}
        )
        assert cfg.cost_param == "T"
        assert cfg.cost_map == {5: 7.0}

    def test_absent_cost_fields_default_empty(self):
        from hpc_sweep_manager.core.common.config import SweepConfig

        cfg = SweepConfig.from_dict({"sweep": {"grid": {"a": [1]}}})
        assert cfg.cost_param is None
        assert cfg.cost_map == {}
