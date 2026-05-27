"""Unit tests for the nvidia-smi CSV parser + probe helper."""

from __future__ import annotations

import pytest

from hpc_sweep_manager.core.remote import gpu_probe
from hpc_sweep_manager.core.remote.gpu_probe import GpuInfo, parse_nvidia_smi_csv


SAMPLE = (
    "0, NVIDIA A100-SXM4-40GB, 38200, 40960, 97\n"
    "1, NVIDIA A100-SXM4-40GB, 12, 40960, 0\n"
)


class TestParse:
    def test_parses_rows(self):
        gpus = parse_nvidia_smi_csv(SAMPLE)
        assert len(gpus) == 2
        assert gpus[0] == GpuInfo(0, "NVIDIA A100-SXM4-40GB", 38200, 40960, 97)
        assert gpus[1].index == 1

    def test_free_busy_heuristic(self):
        gpus = parse_nvidia_smi_csv(SAMPLE)
        assert gpus[0].is_free is False  # 97% util
        assert gpus[1].is_free is True  # 0% util, 12 MB used

    def test_mem_gb_properties(self):
        gpu = parse_nvidia_smi_csv(SAMPLE)[0]
        assert round(gpu.mem_total_gb) == 40
        assert 37 < gpu.mem_used_gb < 38

    def test_empty_output_returns_empty(self):
        assert parse_nvidia_smi_csv("") == []
        assert parse_nvidia_smi_csv("\n  \n") == []

    def test_skips_malformed_lines(self):
        text = "0, GoodGPU, 100, 1000, 5\ngarbage line\n1, BadMem, NaNmb, 1000, 5\n"
        gpus = parse_nvidia_smi_csv(text)
        # Only the first row is well-formed + numeric.
        assert len(gpus) == 1
        assert gpus[0].name == "GoodGPU"

    def test_free_threshold_boundary(self):
        # 4% util, 499 MB used → free; bump either past threshold → busy.
        assert parse_nvidia_smi_csv("0, G, 499, 8000, 4")[0].is_free is True
        assert parse_nvidia_smi_csv("0, G, 501, 8000, 4")[0].is_free is False
        assert parse_nvidia_smi_csv("0, G, 100, 8000, 6")[0].is_free is False


class TestProbe:
    pytestmark = pytest.mark.asyncio

    async def test_probe_returns_parsed_gpus(self, monkeypatch):
        class FakeResult:
            returncode = 0
            stdout = SAMPLE
            stderr = ""

        class FakeConn:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def run(self, cmd, check=False):
                assert "nvidia-smi" in cmd
                return FakeResult()

        async def fake_connect(*a, **k):
            return FakeConn()

        monkeypatch.setattr(gpu_probe, "create_ssh_connection", fake_connect)
        gpus = await gpu_probe.probe_gpus("anahita")
        assert len(gpus) == 2

    async def test_probe_no_nvidia_smi_returns_empty(self, monkeypatch):
        class FakeResult:
            returncode = 127
            stdout = ""
            stderr = "nvidia-smi: command not found"

        class FakeConn:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def run(self, cmd, check=False):
                return FakeResult()

        async def fake_connect(*a, **k):
            return FakeConn()

        monkeypatch.setattr(gpu_probe, "create_ssh_connection", fake_connect)
        assert await gpu_probe.probe_gpus("cpubox") == []
