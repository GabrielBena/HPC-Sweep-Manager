# API reference

Python entry points if you want to embed HSM in your own scripts (or
contribute to it). For end-user CLI usage see the
[user guides](../user_guide/).

## Live tier (use these for new code)

- **[ComputeSource + JobInfo](compute_sources.md)** — the unified async
  interface implemented by `LocalComputeSource`, `SlurmComputeSource`,
  `SSHComputeSource`, `DistributedComputeSource`. This is the
  canonical place new backends and sweep drivers live.
- **`core/common/sweep_orchestrator.py`** —
  `build_compute_source(mode, ...)`, `run_sweep_async(...)`,
  `spec_from_cli(...)`. The async sweep lifecycle driver every CLI
  mode (except completion) routes through.
- **`core/common/resource_spec.py`** — typed `ResourceSpec` dataclass
  (walltime, cpus, mem, gpus, gpu_type, modules, qos, account,
  pre_script, extra_directives). Replaces the legacy opaque
  `resources: str` field.

## Design principles

1. **Async by default below the CLI.** Every `ComputeSource` method is
   async; the CLI is the only sync boundary
   (`asyncio.run(run_sweep_async(...))`).
2. **Typed resource specs.** Backends never receive an opaque string;
   they get a `ResourceSpec` and emit their own native syntax.
3. **Push-model SSH.** Remote machines are dumb compute targets: bash +
   rsync + optionally nvidia-smi. No HSM install, no remote project
   discovery.
4. **One backend interface.** Every execution mode (`local`, `array`,
   `individual`, `remote`, `distributed`, `auto`) routes through the
   `ComputeSource` ABC. The pre-async `BaseJobManager` hierarchy was
   deleted in Pass B-heavy.

## Where to look in the code

```
src/hpc_sweep_manager/
├── cli/                              # Click command tree
├── core/
│   ├── common/
│   │   ├── compute_source.py         # ABC + JobInfo + SweepResult
│   │   ├── sweep_orchestrator.py     # build_compute_source + run_sweep_async
│   │   ├── resource_spec.py          # typed ResourceSpec
│   │   ├── config.py                 # HSMConfig + SweepConfig
│   │   ├── templating.py             # params_to_hydra_args + render_template
│   │   ├── param_generator.py        # grid + paired → param combinations
│   │   └── sweep_analysis.py         # SweepCompletionAnalyzer (read-only)
│   ├── local/
│   │   └── local_compute_source.py
│   ├── hpc/
│   │   └── slurm_compute_source.py
│   ├── remote/
│   │   ├── ssh_compute_source.py     # push-model + factory + parse_gpus_arg
│   │   ├── push_exec.py              # pure helpers (rsync cmds, slot logic)
│   │   ├── gpu_probe.py              # nvidia-smi parser
│   │   └── discovery.py              # just create_ssh_connection
│   └── distributed/
│       ├── distributed_compute_source.py  # fan-out wrapper
│       └── distributed_manager.py    # interior fan-out engine
└── templates/
    ├── slurm_single.sh.j2            # one task per sbatch
    ├── slurm_array.sh.j2             # array submission
    ├── local_compute_source.sh.j2    # local wrapper
    └── ssh_compute_source.sh.j2      # push-SSH wrapper
```

For deep design context see [../../ARCHITECTURE.md](../../ARCHITECTURE.md).
For the agent on-boarding shortlist see [../../CLAUDE.md](../../CLAUDE.md).
