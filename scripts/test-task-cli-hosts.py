#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Black-box conformance corpus for every one-shot task binary.

Every task binary runs on the shared task front end (#931): it answers
`--json-run` with one typed reply on stdout whose exit status agrees with it,
and offers the non-executing `--template` and `--describe`.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


# Keep this inventory explicit: a new hosted binary must make a visible choice
# to join the common process contract. Browser-session JSONL hosts are excluded.
# Each entry is (executable, prefix arguments naming a hosted surface).
HOSTED_TASKS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("calibrate", ()),
    ("calibrate", ("gaincal",)),
    ("importvla", ()),
    ("msexplore", ()),
    ("msexplore", ("plotms",)),
    ("simobserve", ()),
    ("casars-imager", ()),
    ("mstransform", ()),
    ("mstransform", ("split",)),
    ("flagdata", ()),
    ("flagmanager", ()),
    ("exportfits", ()),
    ("feather", ()),
    ("immath", ()),
    ("immoments", ()),
    ("impbcor", ()),
    ("importfits", ()),
    ("impv", ()),
    ("imregrid", ()),
    ("imsubimage", ()),
    ("imexplore", ("imstat",)),
    ("casars-plotcal", ()),
)


def run(command: list[str], *, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        input=stdin,
        text=True,
        capture_output=True,
        check=False,
    )


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def refusal(executable: str, result: subprocess.CompletedProcess[str], case: str) -> dict:
    """The validation reply a front-end binary gives for a refused request."""

    require(result.returncode == 2, f"{executable}: {case} exit {result.returncode}")
    reply = json.loads(result.stdout)
    require(
        reply["status"] == "failed" and reply["kind"] == "validation",
        f"{executable}: {case} reply {reply!r}",
    )
    return reply


def check_front_end(executable: str, base: list[str]) -> None:
    """A front-end binary refuses a request it cannot read with a typed reply,
    and describes its surface without running anything."""

    reply = refusal(executable, run([*base, "--json-run", "-"], stdin="{"), "malformed")
    require("failed to parse task request" in reply["message"], f"{executable}: {reply!r}")
    reply = refusal(
        executable,
        run([*base, "--json-run", "/definitely/missing/casa-rs-task-request.json"]),
        "missing-file",
    )
    require("failed to read JSON request" in reply["message"], f"{executable}: {reply!r}")
    refusal(
        executable,
        run([*base, "--json-run", "-"], stdin='{"surface": "no-such-surface"}'),
        "unknown-surface",
    )

    # imregrid's own --template names its template image; --profile-template
    # reaches the profile on every surface.
    spellings = ["--profile-template"] + ([] if executable == "imregrid" else ["--template"])
    for spelling in spellings:
        template = run([*base, spelling])
        require(template.returncode == 0, f"{executable}: {spelling}: {template.stderr}")
        require("[casars]" in template.stdout, f"{executable}: {spelling} is not a profile")
    describe = run([*base, "--describe"])
    require(describe.returncode == 0, f"{executable}: describe: {describe.stderr}")
    require(describe.stdout.startswith("task "), f"{executable}: describe drift")


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    target = Path(os.environ.get("CARGO_TARGET_DIR", repo / "target"))
    if not target.is_absolute():
        target = repo / target

    build_env = os.environ.copy()
    build_env["CARGO_INCREMENTAL"] = "0"
    subprocess.run(
        ["cargo", "build", "--workspace", "--bins", "--quiet"],
        cwd=repo,
        env=build_env,
        check=True,
    )

    for executable, prefix in HOSTED_TASKS:
        binary = target / "debug" / executable
        require(binary.is_file(), f"{executable}: built binary is missing at {binary}")
        base = [str(binary), *prefix]

        protocol = run([*base, "--protocol-info"])
        require(protocol.returncode == 0, f"{executable}: protocol-info: {protocol.stderr}")
        descriptor = json.loads(protocol.stdout)
        require(descriptor["surface_kind"] == "task", f"{executable}: non-task protocol")
        require(descriptor["protocol_version"] > 0, f"{executable}: invalid protocol version")

        schema = run([*base, "--json-schema"])
        require(schema.returncode == 0, f"{executable}: json-schema: {schema.stderr}")
        bundle = json.loads(schema.stdout)
        require(bundle["protocol"] == descriptor, f"{executable}: discovery descriptor drift")
        require(bundle["parameter_surfaces"], f"{executable}: no parameter surfaces")
        require("request_schema" in bundle, f"{executable}: no request schema")
        require("result_schema" in bundle, f"{executable}: no result schema")

        help_result = run([*base, "--help"])
        require(help_result.returncode == 0, f"{executable}: help: {help_result.stderr}")
        for action in ("--protocol-info", "--json-schema", "--json-run <SOURCE>"):
            require(action in help_result.stdout, f"{executable}: help omits {action}")

        check_front_end(executable, base)

    print(f"task CLI host conformance passed for {len(HOSTED_TASKS)} binaries")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AssertionError, KeyError, json.JSONDecodeError) as error:
        print(f"task CLI host conformance failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
