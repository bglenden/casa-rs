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
    incremental_output_path: pathlib.Path | None = None,
) -> subprocess.CompletedProcess[str]:
    if stream_stdout or incremental_output_path is not None:
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
        try:
            return_code = process.wait(timeout=timeout_seconds)
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
