# SPDX-License-Identifier: LGPL-3.0-or-later
"""Canonical subprocess boundary for imaging evidence tools."""

from __future__ import annotations

import pathlib
import subprocess
import sys
import threading


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
) -> subprocess.CompletedProcess[str]:
    if stream_stdout:
        if input_text is not None or not merge_stderr:
            raise ValueError(
                "stream_stdout requires merged stderr and does not accept stdin"
            )
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
                sys.stdout.write(line)
                sys.stdout.flush()

        reader = threading.Thread(target=drain_stdout, daemon=True)
        reader.start()
        try:
            return_code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            reader.join()
            process.stdout.close()
            raise
        reader.join()
        process.stdout.close()
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
