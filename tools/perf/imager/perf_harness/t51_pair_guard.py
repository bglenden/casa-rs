# SPDX-License-Identifier: LGPL-3.0-or-later
"""Outer, single-attempt guard for the entire T51 paired diagnostic pipeline.

The child command must include setup, builds, both engines and validation.
Nothing in this module grants permission to run that command. RSS is sampled
across the group and observed descendants; the receipt does not claim a kernel
aggregate-RSS limit or an unsampled instantaneous peak.
"""

import os
import signal
import subprocess
import time


WALL_SECONDS = 900
RSS_BYTES = 32 << 30


def process_scope(listing, root_pid, retained):
    """Track debugger children even if they leave the initial process group."""
    rows = {}
    for line in listing.splitlines():
        pid, parent, group, rss = map(int, line.split())
        if pid <= 0 or parent < 0 or group < 0 or rss < 0 or pid in rows:
            raise ValueError("invalid process census")
        rows[pid] = (parent, group, rss * 1024)
    owned = {pid for pid in retained if pid in rows}
    owned.update(pid for pid, (_, group, _) in rows.items() if group == root_pid)
    if root_pid in rows:
        owned.add(root_pid)
    while True:
        children = {pid for pid, (parent, _, _) in rows.items() if parent in owned}
        if children <= owned:
            break
        owned.update(children)
    return owned, sum(rows[pid][2] for pid in owned)


def run_pair_pipeline(argv, *, cwd, environment, log, wall_seconds=WALL_SECONDS):
    """Run one already-authorized pipeline under one nonrenewable deadline.

    The timer begins before process launch. Errors in the guardian fail closed;
    all observed descendants and the original process group are terminated.
    The caller persists the small returned receipt without additional workload
    setup, validation, or hashing outside this boundary.
    """
    if isinstance(wall_seconds, bool) or not isinstance(wall_seconds, int) or wall_seconds <= 0:
        raise ValueError("wall_seconds must be a positive integer")
    started = time.monotonic()
    deadline = started + wall_seconds
    peak = 0
    owned = set()
    reason = None
    proc = subprocess.Popen(argv, cwd=cwd, env=environment, stdout=log,
                            stderr=subprocess.STDOUT, start_new_session=True)

    def kill_scope():
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        for pid in owned:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                reason = "wall_cap"
                break
            listing = subprocess.check_output(
                ["ps", "-axo", "pid=,ppid=,pgid=,rss="], text=True,
                timeout=min(2.0, remaining),
            )
            owned, rss = process_scope(listing, proc.pid, owned)
            peak = max(peak, rss)
            if rss >= RSS_BYTES:
                reason = "rss_cap"
                break
            if proc.poll() is not None:
                if owned - {proc.pid}:
                    reason = "pipeline_exited_with_live_descendants"
                break
            time.sleep(max(0.0, min(0.1, deadline - time.monotonic())))
    except BaseException:
        kill_scope()
        raise
    finally:
        if reason is not None or proc.poll() is None:
            kill_scope()
        proc.wait()
    elapsed = time.monotonic() - started
    if elapsed >= wall_seconds and reason is None:
        reason = "wall_cap"
    return {"exit_code": proc.returncode, "stop_reason": reason,
            "total_seconds": elapsed, "wall_cap_seconds": wall_seconds,
            "rss_cap_bytes": RSS_BYTES, "sampled_scope_peak_rss_bytes": peak,
            "rss_scope": "initial process group plus observed descendants",
            "complete": reason is None and proc.returncode == 0,
            "attempts": 1}
