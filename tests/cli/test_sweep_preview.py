"""Unit tests for the dry-run / placement preview helpers in cli/sweep.py.

These back the P1b legibility fix and the GPU-placement preview: value
formatting must not truncate short lists, paired groups must render as explicit
zipped rows, and the allowlist description must read correctly for each shape.
"""

from __future__ import annotations

import io

from rich.console import Console

from hpc_sweep_manager.cli.sweep import (
    _describe_gpu_allowlist,
    _format_param_values,
    _render_paired_groups,
)


def _capture(fn) -> str:
    buf = io.StringIO()
    console = Console(file=buf, width=200, no_color=True)
    fn(console)
    return buf.getvalue()


class TestDescribeGpuAllowlist:
    def test_none_is_all(self):
        assert _describe_gpu_allowlist(None) == "all detected GPUs"

    def test_zero_is_cpu(self):
        assert _describe_gpu_allowlist(0) == "CPU only"

    def test_int_n(self):
        assert _describe_gpu_allowlist(3) == "first 3 GPU(s)"

    def test_index_list(self):
        assert _describe_gpu_allowlist([1, 2, 3]) == "GPUs [1, 2, 3]"

    def test_empty_list_is_cpu(self):
        assert _describe_gpu_allowlist([]) == "CPU only"

    def test_bool_true_is_all_not_one(self):
        # bool is an int subclass; True must not read as "first 1 GPU".
        assert _describe_gpu_allowlist(True) == "all detected GPUs"
        assert _describe_gpu_allowlist(False) == "CPU only"


class TestFormatParamValues:
    def test_short_list_not_truncated(self):
        out = _format_param_values([0.001, 0.01, 0.1])
        assert out == "[0.001, 0.01, 0.1]"
        assert "…" not in out

    def test_long_list_is_ellipsized(self):
        out = _format_param_values(list(range(200)), limit=40)
        assert out.endswith("…")
        assert len(out) <= 40


class TestRenderPairedGroups:
    def test_renders_zipped_rows(self):
        paired = {
            "model.rec_steps": {
                "type": "paired",
                "group": 0,
                "group_name": "dilation",
                "values": [1, 2, 4],
                "count": 3,
            },
            "model.alpha": {
                "type": "paired",
                "group": 0,
                "group_name": "dilation",
                "values": [1.0, 0.75, 0.5],
                "count": 3,
            },
        }
        out = _capture(lambda c: _render_paired_groups(paired, c))
        assert "Paired groups (zipped):" in out
        assert "dilation" in out and "3 pair(s)" in out
        # The whole point: each row shows the params that zip together.
        assert "model.rec_steps=1" in out and "model.alpha=1.0" in out
        assert "model.rec_steps=4" in out and "model.alpha=0.5" in out

    def test_empty_is_silent(self):
        assert _capture(lambda c: _render_paired_groups({}, c)) == ""
