"""How loaded a shared Slurm account is, and how much of that is us (tracker S-4, field report #10).

Slurm computes fair share per account: when one member runs hundreds of tasks, every job on the
account drops in priority, co-workers' included (a 1,320-task replay once pushed a lab account to
~17x its share and a co-worker waited for days). One round trip answers it: ``sshare`` for the
account's usage against its share, ``squeue -A`` for who runs and who waits, ``sinfo`` for the
partition's GPU nodes (which CPU-only jobs should leave alone).
"""

from __future__ import annotations

import asyncio
import math
import shlex
from dataclasses import dataclass, field
from typing import Any

HOT_RATIO = 2.0  # usage above this multiple of the account's share is "hot"
DEFAULT_THROTTLE = 50  # tasks at once (about 200 CPUs at 4 each) when the account is hot

# $1 account, $2 partition (may be empty). Every section prints even when its command fails.
PROBE = r"""A="$1"; P="$2"
echo @@ME; id -un
echo @@SSHARE; sshare -n -P -A "$A" -a -o Account,User,NormShares,RawUsage,EffectvUsage 2>/dev/null
echo @@RUN; squeue -h -A "$A" -t R -o '%u|%C|%N' 2>/dev/null
echo @@PEND; squeue -h -A "$A" -t PD -o '%u|%r' 2>/dev/null
echo @@GPUN; sinfo -h ${P:+-p "$P"} -N -o '%N %G' 2>/dev/null | awk '$2!="(null)"{print $1}'
"""


@dataclass(frozen=True)
class Share:
    account: str
    me: str
    ratio: float  # the account's effective usage / its normalised share (nan: unknown)
    my_usage: float  # our fraction of the account's recorded usage
    running: dict[str, tuple[int, int]] = field(default_factory=dict)  # user -> (jobs, cpus)
    waiting: dict[str, set[str]] = field(default_factory=dict)  # co-worker -> pending reasons
    gpu_nodes: tuple[str, ...] = ()
    mine_on_gpu: int = 0  # our running jobs on GPU nodes

    @property
    def known(self) -> bool:
        """The probe found the account's share (a failed or empty ``sshare`` leaves it nan)."""
        return not math.isnan(self.ratio)

    @property
    def hot(self) -> bool:
        """Over ``HOT_RATIO`` its share, or a co-worker waiting on priority."""
        return self.ratio > HOT_RATIO or any("Priority" in r for r in self.waiting.values())

    def summary(self) -> str:
        jobs, cpus = self.running.get(self.me, (0, 0))
        total = sum(c for _, c in self.running.values())
        pending = ", ".join(f"{u} ({'/'.join(sorted(r))})" for u, r in sorted(self.waiting.items()))
        usage = f"{self.ratio:.1f}x" if self.known else "unknown (sshare gave nothing)"
        return (
            f"{self.account}: usage {usage} its fair share; {self.me} = "
            f"{100 * self.my_usage:.0f}% of its recorded usage and {cpus} of {total} running "
            f"CPUs ({jobs} jobs, {self.mine_on_gpu} on GPU nodes); co-workers pending: "
            f"{pending or 'none'}"
        )


def parse_share(out: str, account: str) -> Share:
    """Parse :data:`PROBE`'s output for ``account``."""
    sec: dict[str, list[str]] = {}
    cur = None
    for line in out.splitlines():
        if line.startswith("@@"):
            cur = sec.setdefault(line[2:].strip(), [])
        elif cur is not None and line.strip():
            cur.append(line.strip())
    me = (sec.get("ME") or ["?"])[0]

    def num(x: str) -> float:
        try:
            return float(x)
        except ValueError:
            return 0.0

    norm = eff = 0.0
    usage: dict[str, float] = {}
    for row in sec.get("SSHARE", []):
        acct, user, ns, raw, ev = (row.split("|") + [""] * 5)[:5]
        if acct.strip() != account:
            continue
        if user.strip():
            usage[user.strip()] = usage.get(user.strip(), 0.0) + num(raw)
        else:
            norm, eff = num(ns), num(ev)
    gpu_nodes = tuple(dict.fromkeys(sec.get("GPUN", [])))  # one row per node and partition
    running: dict[str, tuple[int, int]] = {}
    mine_on_gpu = 0
    for row in sec.get("RUN", []):
        user, cpus, node = (row.split("|") + [""] * 3)[:3]
        jobs, total = running.get(user, (0, 0))
        running[user] = (jobs + 1, total + int(num(cpus)))
        mine_on_gpu += user == me and node in gpu_nodes
    waiting: dict[str, set[str]] = {}
    for row in sec.get("PEND", []):
        user, reason = (row.split("|") + [""])[:2]
        if user != me:
            waiting.setdefault(user, set()).add(reason)
    return Share(
        account=account,
        me=me,
        ratio=eff / norm if norm else math.nan,
        my_usage=usage.get(me, 0.0) / (sum(usage.values()) or 1.0),
        running=running,
        waiting=waiting,
        gpu_nodes=gpu_nodes,
        mine_on_gpu=mine_on_gpu,
    )


async def probe_share(account: str, partition: str = "", conn: Any = None) -> Share:
    """Run :data:`PROBE` over ``conn`` (asyncssh), or locally when ``conn`` is None."""
    args = f"{shlex.quote(account)} {shlex.quote(partition)}"
    if conn is not None:
        out = (await conn.run(f"bash -s -- {args}", input=PROBE, check=False)).stdout or ""
    else:
        proc = await asyncio.create_subprocess_shell(
            f"bash -s -- {args}", stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE
        )
        out = (await proc.communicate(PROBE.encode()))[0].decode()
    return parse_share(out, account)
