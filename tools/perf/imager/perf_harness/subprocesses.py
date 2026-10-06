# SPDX-License-Identifier: LGPL-3.0-or-later
"""Canonical subprocess boundary for imaging evidence tools."""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import threading
import time
from typing import Callable


def run_with_terminal_usage(argv: list[str]) -> int:
    """Run one command and report its kernel high-water RSS after it exits."""
    if sys.platform not in {"darwin", "linux"} or not hasattr(os, "wait4"):
        raise RuntimeError("terminal command RSS requires Darwin or Linux wait4")
    process = subprocess.Popen(argv)
    try:
        _, status, usage = os.wait4(process.pid, 0)
        process.returncode = os.waitstatus_to_exitcode(status)
    except BaseException:
        process.kill()
        process.wait()
        raise
    peak_rss_bytes = int(usage.ru_maxrss) * (1 if sys.platform == "darwin" else 1024)
    print(
        "imager_bench_process_resource "
        f"command={pathlib.Path(argv[0]).name} pid={process.pid} "
        f"exit_code={process.returncode} peak_rss_bytes={peak_rss_bytes} "
        "source=wait4 scope=terminal_child",
        file=sys.stderr,
        flush=True,
    )
    return process.returncode if process.returncode >= 0 else 128 - process.returncode


def run_command(
    argv: list[str],
    *,
    cwd: pathlib.Path | None = None,
    environment: dict[str, str] | None = None,
    input_text: str | None = None,
    timeout_seconds: float | None = None,
    merge_stderr: bool = True,
    check: bool = False,
    stream_stdout: bool = False,
    incremental_output_path: pathlib.Path | None = None,
    on_spawn: Callable[[subprocess.Popen[str]], None] | None = None,
    before_reap: Callable[[subprocess.Popen[str]], None] | None = None,
) -> subprocess.CompletedProcess[str]:
    if (
        stream_stdout
        or incremental_output_path is not None
        or on_spawn is not None
        or before_reap is not None
    ):
        if input_text is not None or not merge_stderr:
            raise ValueError(
                "incremental output capture requires merged stderr and does not "
                "accept stdin"
            )
        output_handle = None
        if incremental_output_path is not None:
            incremental_output_path.parent.mkdir(parents=True, exist_ok=True)
            output_handle = incremental_output_path.open("w", encoding="utf-8")
        process = subprocess.Popen(
            argv,
            cwd=cwd,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
        )
        output_chunks: list[str] = []
        assert process.stdout is not None

        def drain_stdout() -> None:
            assert process.stdout is not None
            for line in process.stdout:
                output_chunks.append(line)
                if output_handle is not None:
                    output_handle.write(line)
                    output_handle.flush()
                if stream_stdout:
                    sys.stdout.write(line)
                    sys.stdout.flush()

        reader = threading.Thread(target=drain_stdout, daemon=True)
        reader.start()
        if on_spawn is not None:
            try:
                on_spawn(process)
            except BaseException:
                process.terminate()
                try:
                    process.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                reader.join()
                process.stdout.close()
                if output_handle is not None:
                    output_handle.close()
                raise
        try:
            return_code = _wait_with_before_reap(
                process,
                timeout_seconds=timeout_seconds,
                before_reap=before_reap,
            )
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            reader.join()
            process.stdout.close()
            if output_handle is not None:
                output_handle.close()
            raise
        except BaseException:
            # An operator interrupt must not orphan a long-running CASA worker.
            # Preserve the exception after synchronously closing the process and
            # its output-draining thread so the caller can write a typed receipt.
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            reader.join()
            process.stdout.close()
            if output_handle is not None:
                output_handle.close()
            raise
        reader.join()
        process.stdout.close()
        if output_handle is not None:
            output_handle.close()
        completed = subprocess.CompletedProcess(
            argv, return_code, "".join(output_chunks), None
        )
        if check and return_code != 0:
            raise subprocess.CalledProcessError(
                return_code, argv, output=completed.stdout
            )
        return completed
    return subprocess.run(
        argv,
        cwd=cwd,
        env=environment,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT if merge_stderr else subprocess.PIPE,
        timeout=timeout_seconds,
        check=check,
    )


def _wait_with_before_reap(
    process: subprocess.Popen[str],
    *,
    timeout_seconds: float | None,
    before_reap: Callable[[subprocess.Popen[str]], None] | None,
) -> int:
    if before_reap is None:
        return process.wait(timeout=timeout_seconds)
    if _wait_for_posix_exit_without_reap(process, timeout_seconds=timeout_seconds):
        before_reap(process)
        return process.wait()
    return_code = process.wait(timeout=timeout_seconds)
    before_reap(process)
    return return_code


def _wait_for_posix_exit_without_reap(
    process: subprocess.Popen[str],
    *,
    timeout_seconds: float | None,
) -> bool:
    required = ("P_PID", "WEXITED", "WNOWAIT", "WNOHANG", "waitid")
    if os.name != "posix" or any(not hasattr(os, name) for name in required):
        return False
    deadline = None if timeout_seconds is None else time.monotonic() + timeout_seconds
    options = os.WEXITED | os.WNOWAIT | os.WNOHANG
    while True:
        try:
            status = os.waitid(os.P_PID, process.pid, options)
        except ChildProcessError:
            return False
        if status is not None:
            return True
        if deadline is not None and time.monotonic() >= deadline:
            raise subprocess.TimeoutExpired(process.args, timeout_seconds)
        time.sleep(0.02)


if __name__ == "__main__":
    if len(sys.argv) < 3 or sys.argv[1] != "--":
        raise SystemExit("usage: subprocesses.py -- command [arguments...]")
    raise SystemExit(run_with_terminal_usage(sys.argv[2:]))
