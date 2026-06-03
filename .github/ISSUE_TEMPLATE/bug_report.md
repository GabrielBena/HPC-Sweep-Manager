---
name: Bug report
about: Something HSM did wrong (especially silent / wrong-result bugs)
labels: bug
---

**What happened vs. what you expected**


**Backend** (delete others): `local` / `array` / `individual` / `remote` (SSH) / SSH-Slurm (`backend: slurm`) / `distributed`

**Command**
```bash
hsm sweep run ...
```

**Evidence** (paste the useful bits)
- `tasks/<task>/task_info.txt` (Status line) and `logs/*.err`
- For Slurm: `sacct -j <id> -X -o State,ExitCode`
- `hsm sweep report <id> --scan-tasks` if relevant

**Env**: cluster/host, conda env, HSM version (`hsm --version` / commit), Python.

> Did a sweep *look* fine but produce wrong/missing results? Say so loudly — those
> (FAILED-reported-COMPLETED, silent CPU fallback, lost results) are the highest
> priority. See `docs/dev/field-reports/` for the kind of report that's gold.
