#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Manifest-driven native macOS GUI acceptance harness."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import plistlib
import shutil
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


SCHEMA_VERSION = 1
APP_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = APP_ROOT.parent.parent
MANIFEST_PATH = APP_ROOT / "gui-journeys.json"
GATE_ROOT = APP_ROOT / ".gui-test"


class HarnessError(RuntimeError):
    pass


def load_manifest(path: Path = MANIFEST_PATH) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    validate_manifest(manifest)
    return manifest


def validate_manifest(manifest: dict[str, Any]) -> None:
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise HarnessError("unsupported GUI journey manifest schema")
    timeout_classes = manifest.get("timeout_classes")
    journeys = manifest.get("journeys")
    if not isinstance(timeout_classes, dict) or not isinstance(journeys, list):
        raise HarnessError("manifest must define timeout_classes and journeys")
    ids: set[str] = set()
    for journey in journeys:
        required = {
            "id", "classification", "selector", "timeout_class", "result_bundle",
            "rust_binaries", "python_packages", "requires_codex_account",
            "requires_network", "project", "gate", "artifacts", "remote_supported",
        }
        missing = sorted(required - journey.keys())
        if missing:
            raise HarnessError(f"journey is missing fields: {', '.join(missing)}")
        journey_id = journey["id"]
        if not isinstance(journey_id, str) or not journey_id or journey_id in ids:
            raise HarnessError(f"invalid or duplicate journey id: {journey_id!r}")
        ids.add(journey_id)
        if journey["timeout_class"] not in timeout_classes:
            raise HarnessError(f"{journey_id}: unknown timeout class")
        if journey["classification"] not in {"deterministic", "opt_in_live"}:
            raise HarnessError(f"{journey_id}: invalid classification")
        if journey["classification"] == "deterministic":
            if journey["requires_network"] or journey["requires_codex_account"]:
                raise HarnessError(f"{journey_id}: deterministic journey must remain offline")
        artifact_names = [item.get("name") for item in journey["artifacts"]]
        if len(artifact_names) != len(set(artifact_names)) or any(not name for name in artifact_names):
            raise HarnessError(f"{journey_id}: artifact names must be unique and non-empty")
        gate = journey["gate"]
        if gate is not None:
            if not journey["selector"] or not isinstance(gate.get("required_fields"), list):
                raise HarnessError(f"{journey_id}: live gate contract is incomplete")


def select_journey(manifest: dict[str, Any], journey_id: str) -> dict[str, Any]:
    for journey in manifest["journeys"]:
        if journey["id"] == journey_id:
            return journey
    raise HarnessError(f"unknown GUI journey: {journey_id}")


def require_commands(
    journey: dict[str, Any],
    which: Callable[[str], str | None] = shutil.which,
) -> None:
    commands = ["xcodebuild", "cargo"]
    if journey["requires_codex_account"]:
        commands.append(os.environ.get("CASA_RS_CODEX_COMMAND", "codex"))
    if journey.get("network_preflight"):
        commands.append("curl")
    missing = [command for command in commands if which(command) is None]
    if missing:
        raise HarnessError(f"missing required command(s): {', '.join(missing)}")


def run_command(
    command: list[str],
    *,
    cwd: Path = REPO_ROOT,
    env: dict[str, str] | None = None,
    timeout: int | None = None,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    print("==>", " ".join(command))
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        timeout=timeout,
        check=True,
        text=True,
        capture_output=capture,
    )


def git_revision() -> str:
    return run_command(
        ["git", "rev-parse", "HEAD"], capture=True
    ).stdout.strip()


def resolve_python() -> str:
    configured = os.environ.get("CASA_RS_GUI_TEST_PYTHON")
    if configured:
        path = shutil.which(configured) if not Path(configured).is_absolute() else configured
        if path and Path(path).is_file():
            return str(Path(path).resolve())
        raise HarnessError(f"configured GUI test Python is unavailable: {configured}")
    result = run_command(
        [str(REPO_ROOT / "scripts/resolve-python.sh"), "3.10"], capture=True
    )
    return result.stdout.strip()


def target_directory() -> Path:
    configured = Path(os.environ.get("CARGO_TARGET_DIR", str(REPO_ROOT / "target")))
    return configured if configured.is_absolute() else REPO_ROOT / configured


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass
class JourneyPaths:
    artifact_root: Path
    result_bundle: Path
    summary: Path
    gate: Path | None
    project: Path | None
    receipt: Path | None
    test_report: Path | None
    published_report: Path | None


def make_paths(journey: dict[str, Any]) -> JourneyPaths:
    artifact_root = Path(
        os.environ.get("CASA_RS_GUI_TEST_ARTIFACT_ROOT", str(GATE_ROOT))
    ).expanduser()
    project_base = Path(
        os.environ.get("CASA_RS_GUI_TEST_PROJECT_BASE", str(Path.home() / ".casa-rs-gui-tests"))
    ).expanduser()
    project_policy = journey["project"]["policy"]
    project: Path | None
    if project_policy == "none":
        project = None
    elif project_policy == "fixed":
        project = project_base / journey["project"]["name"]
    elif project_policy == "timestamped":
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        project = project_base / f"{journey['project']['name']}-{stamp}-{os.getpid()}"
    else:
        raise HarnessError(f"{journey['id']}: unknown project policy {project_policy}")
    gate = journey["gate"]
    gate_path = GATE_ROOT / gate["file"] if gate else None
    receipt = project / gate["receipt"] if gate and project else None
    test_report = project / gate["report"] if gate and project and gate.get("report") else None
    published_report = (
        artifact_root / gate["published_report"] if gate and gate.get("published_report") else None
    )
    return JourneyPaths(
        artifact_root=artifact_root,
        result_bundle=artifact_root / journey["result_bundle"],
        summary=artifact_root / f"{journey['id']}.evidence.json",
        gate=gate_path,
        project=project,
        receipt=receipt,
        test_report=test_report,
        published_report=published_report,
    )


def preflight_live(journey: dict[str, Any], python: str) -> None:
    if journey["requires_codex_account"]:
        codex = os.environ.get("CASA_RS_CODEX_COMMAND", "codex")
        run_command([codex, "--version"])
        run_command([codex, "login", "status"])
    run_command([python, "--version"])
    packages = journey["python_packages"]
    if packages:
        imports = ", ".join(packages)
        run_command([python, "-c", f"import {imports}"])

    network = journey.get("network_preflight")
    if network:
        template = REPO_ROOT / network["template"]
        manifest = template / "tutorial.toml"
        markdown = template / "tutorial.md"
        if not manifest.is_file() or not markdown.is_file():
            raise HarnessError(f"tutorial template is incomplete: {template}")
        source = manifest.read_text(encoding="utf-8")
        expected_fragments = [
            network["url"],
            f"expected_size_bytes = {network['expected_size']}",
            f"sha256 = \"{network['expected_sha256']}\"",
        ]
        if any(fragment not in source for fragment in expected_fragments):
            raise HarnessError("tutorial manifest does not match journey contract")
        headers = run_command(
            ["curl", "-fsSIL", "--max-time", "30", network["url"]], capture=True
        ).stdout.lower()
        if " 200" not in headers:
            raise HarnessError("tutorial source preflight did not return HTTP 200")
        expected_length = f"content-length: {network['expected_size']}"
        if expected_length not in headers:
            raise HarnessError("tutorial source size differs from the manifest")


def build_rust(journey: dict[str, Any], env: dict[str, str]) -> None:
    for build in journey["rust_binaries"]:
        command = ["cargo", "build", "-p", build["package"]]
        for binary in build["bins"]:
            command.extend(["--bin", binary])
        run_command(command, env=env)


def prepare_project(journey: dict[str, Any], paths: JourneyPaths) -> bool:
    if paths.project is None:
        return False
    paths.project.parent.mkdir(parents=True, exist_ok=True)
    resume_variable = journey["project"].get("resume_environment")
    resume = bool(resume_variable and os.environ.get(resume_variable) == "1")
    if resume:
        if not paths.project.is_dir():
            raise HarnessError(f"cannot resume; retained project is missing: {paths.project}")
    elif paths.project.exists():
        shutil.rmtree(paths.project)
    return resume


def gate_values(
    journey: dict[str, Any], paths: JourneyPaths, python: str, revision: str, resume: bool
) -> dict[str, str]:
    if not journey["gate"] or not paths.project or not paths.receipt:
        return {}
    codex = os.environ.get("CASA_RS_CODEX_COMMAND", "codex")
    codex_path = shutil.which(codex) or codex
    values = {
        "agentCommand": codex_path,
        "home": str(Path.home()),
        "codexHome": os.environ.get("CODEX_HOME", str(Path.home() / ".codex")),
        "path": os.environ.get("PATH", "/usr/bin:/bin"),
        "pythonCommand": python,
        "projectRoot": str(paths.project),
        "passReceipt": str(paths.receipt),
        "repoRoot": str(REPO_ROOT),
        "repoRevision": revision,
        "resumeAfterTask": "true" if resume else "false",
    }
    if paths.test_report:
        values["evidenceReport"] = str(paths.test_report)
    target = target_directory() / "debug"
    values.update({
        "simobserveCommand": str(target / "simobserve"),
        "msexploreCommand": str(target / "msexplore"),
        "imagerCommand": str(target / "casars-imager"),
    })
    network = journey.get("network_preflight")
    if network:
        template = REPO_ROOT / network["template"]
        values.update({
            "templateRoot": str(template),
            "templateManifestSha256": sha256(template / "tutorial.toml"),
            "templateMarkdownSha256": sha256(template / "tutorial.md"),
            "sourceUri": network["url"],
            "expectedSize": str(network["expected_size"]),
            "expectedSha256": network["expected_sha256"],
        })
    required = journey["gate"]["required_fields"]
    missing = [key for key in required if not values.get(key)]
    if missing:
        raise HarnessError(f"{journey['id']}: missing gate fields: {', '.join(missing)}")
    return {key: values[key] for key in required}


def prepare_gate(journey: dict[str, Any], paths: JourneyPaths, values: dict[str, str]) -> None:
    if not paths.gate:
        return
    GATE_ROOT.mkdir(parents=True, exist_ok=True)
    paths.gate.unlink(missing_ok=True)
    if paths.receipt:
        paths.receipt.unlink(missing_ok=True)
    if paths.test_report:
        paths.test_report.unlink(missing_ok=True)
    if paths.published_report:
        paths.published_report.unlink(missing_ok=True)
    with paths.gate.open("wb") as handle:
        plistlib.dump(values, handle, fmt=plistlib.FMT_XML, sort_keys=True)


def xcode_arguments(paths: JourneyPaths) -> tuple[list[str], list[str]]:
    derived = Path(
        os.environ.get(
            "CASA_RS_GUI_TEST_DERIVED_DATA", str(paths.artifact_root / "DerivedData")
        )
    ).expanduser()
    destination = os.environ.get(
        "CASA_RS_GUI_TEST_DESTINATION", f"platform=macOS,arch={platform.machine()}"
    )
    shared = [
        "-project", str(APP_ROOT / "CasarsMac.xcodeproj"),
        "-scheme", "CasarsMacGUI",
        "-configuration", "Debug",
        "-destination", destination,
        "-derivedDataPath", str(derived),
    ]
    signing: list[str] = []
    identity = os.environ.get("CASA_RS_GUI_TEST_CODE_SIGN_IDENTITY")
    keychain = os.environ.get("CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN")
    if identity:
        signing.extend(["CODE_SIGN_STYLE=Manual", f"CODE_SIGN_IDENTITY={identity}"])
        if keychain:
            signing.append(f"OTHER_CODE_SIGN_FLAGS=--keychain {keychain}")
    return shared, signing


def build_xcode(paths: JourneyPaths) -> None:
    shared, signing = xcode_arguments(paths)
    derived = Path(shared[shared.index("-derivedDataPath") + 1])
    if os.environ.get("CASA_RS_GUI_TEST_REUSE_BUILD") == "1":
        runner = derived / "Build/Products/Debug/CasarsMacUITests-Runner.app"
        if not runner.is_dir():
            raise HarnessError("build reuse requested but UI test products are missing")
        print("==> Reusing unchanged CasarsMacUITests build products")
        return
    run_command(["xcodebuild", "build-for-testing", *shared, *signing])
    identity = os.environ.get("CASA_RS_GUI_TEST_CODE_SIGN_IDENTITY")
    if identity:
        app = derived / "Build/Products/Debug/casars-mac.app"
        run_command(["/usr/bin/codesign", "--verify", "--deep", "--strict", str(app)])
        requirement = run_command(
            ["/usr/bin/codesign", "-dr", "-", str(app)], capture=True
        ).stderr
        if "cdhash" in requirement:
            raise HarnessError("stable GUI signing produced a build-specific requirement")


def notify(message: str) -> None:
    if os.environ.get("CI") or os.environ.get("CASA_RS_GUI_TEST_EXCLUSIVE_NOTICE", "1") == "0":
        return
    subprocess.run(
        ["/usr/bin/osascript", "-e", f'display notification "{message}" with title "casa-rs GUI tests"'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


class CodexHider:
    def __init__(self) -> None:
        self.stop = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self) -> None:
        if os.environ.get("CI"):
            return
        script = (
            'tell application "System Events" to set visible of '
            '(first application process whose bundle identifier is "com.openai.codex") to false'
        )

        def hide() -> None:
            while not self.stop.is_set():
                subprocess.run(
                    ["/usr/bin/osascript", "-e", script],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
                self.stop.wait(0.5)

        self.thread = threading.Thread(target=hide, daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=2)
            subprocess.run(
                ["/usr/bin/osascript", "-e", 'tell application id "com.openai.codex" to activate'],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )


def run_xcode(
    journey: dict[str, Any], paths: JourneyPaths, timeout_seconds: int
) -> int:
    countdown = os.environ.get("CASA_RS_GUI_TEST_COUNTDOWN_SECONDS", "10")
    if not countdown.isdigit():
        raise HarnessError("CASA_RS_GUI_TEST_COUNTDOWN_SECONDS must be non-negative")
    if not os.environ.get("CI") and os.environ.get("CASA_RS_GUI_TEST_EXCLUSIVE_NOTICE", "1") != "0":
        print("\n==> EXCLUSIVE GUI TEST WINDOW")
        notify("Starting an exclusive foreground test window. Please leave the Mac idle.")
        for remaining in range(int(countdown), 0, -1):
            print(f"\r==> Starting GUI journey in {remaining:2d} second(s)...", end="", flush=True)
            time.sleep(1)
        print("\r==> Starting GUI journey now.                 ")
    shared, _ = xcode_arguments(paths)
    command = [
        "xcodebuild", "test-without-building", *shared,
        "-resultBundlePath", str(paths.result_bundle),
    ]
    if journey["selector"]:
        command.extend(["-only-testing", journey["selector"]])
    env = os.environ.copy()
    env["NSUnbufferedIO"] = "YES"
    hider = CodexHider()
    hider.start()
    try:
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            env=env,
            timeout=timeout_seconds,
            check=False,
        )
        return completed.returncode
    finally:
        hider.close()


def artifact_paths(journey: dict[str, Any], paths: JourneyPaths, transport_only: bool = False) -> list[Path]:
    artifacts = journey["artifacts"]
    if transport_only:
        artifacts = [artifact for artifact in artifacts if artifact["transport"]]
    return [paths.artifact_root / artifact["name"] for artifact in artifacts]


def write_summary(
    journey: dict[str, Any], paths: JourneyPaths, revision: str, status: str
) -> None:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "journey_id": journey["id"],
        "classification": journey["classification"],
        "repository_revision": revision,
        "selector": journey["selector"],
        "status": status,
        "artifacts": [artifact["name"] for artifact in journey["artifacts"]],
        "project_retained": bool(status != "passed" and paths.project and paths.project.exists()),
    }
    paths.summary.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def finalize_run(
    journey: dict[str, Any], paths: JourneyPaths, revision: str, test_status: int
) -> None:
    def fail(message: str) -> None:
        write_summary(journey, paths, revision, "failed")
        raise HarnessError(message)

    if test_status != 0:
        write_summary(journey, paths, revision, "failed")
        return
    if paths.receipt and not paths.receipt.is_file():
        fail(f"{journey['id']} did not write its success receipt")
    if paths.test_report:
        if not paths.test_report.is_file() or not paths.published_report:
            fail(f"{journey['id']} did not write its evidence report")
        try:
            with paths.test_report.open(encoding="utf-8") as handle:
                json.load(handle)
        except (OSError, json.JSONDecodeError) as error:
            fail(f"{journey['id']} wrote an invalid evidence report: {error}")
        shutil.copy2(paths.test_report, paths.published_report)
    summary_name = paths.summary.name
    missing = [
        path.name
        for path in artifact_paths(journey, paths)
        if path.name != summary_name and not path.exists()
    ]
    if missing:
        fail(f"declared artifacts are missing: {', '.join(missing)}")
    if paths.project and paths.project.exists():
        shutil.rmtree(paths.project)
    write_summary(journey, paths, revision, "passed")


def run_journey(journey_id: str) -> int:
    manifest = load_manifest()
    journey = select_journey(manifest, journey_id)
    if platform.system() != "Darwin":
        raise HarnessError("GUI acceptance requires macOS with an interactive GUI session")
    require_commands(journey)
    revision = git_revision()
    python = resolve_python()
    paths = make_paths(journey)
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    if paths.result_bundle.exists():
        shutil.rmtree(paths.result_bundle)
    preflight_live(journey, python)
    env = os.environ.copy()
    env["CARGO_INCREMENTAL"] = "0"
    env["CASA_RS_GUI_TEST_PYTHON"] = python
    for secret in ("OPENAI_API_KEY", "AZURE_OPENAI_API_KEY", "OPENAI_BASE_URL"):
        env.pop(secret, None)
        os.environ.pop(secret, None)
    build_rust(journey, env)
    resume = prepare_project(journey, paths)
    values = gate_values(journey, paths, python, revision, resume)
    prepare_gate(journey, paths, values)
    try:
        build_xcode(paths)
        timeout = manifest["timeout_classes"][journey["timeout_class"]]
        status = run_xcode(journey, paths, timeout)
        finalize_run(journey, paths, revision, status)
        if status:
            if paths.project and journey["project"]["retain_on_failure"]:
                print(f"==> Failed project retained for diagnosis: {paths.project}", file=sys.stderr)
            notify("GUI tests failed. You can use the Mac again; inspect retained evidence.")
            return status
        notify("GUI tests passed. You can use the Mac again.")
        print(f"==> GUI journey complete: PASS ({journey_id})")
        return 0
    finally:
        if paths.gate:
            paths.gate.unlink(missing_ok=True)
        if paths.receipt:
            paths.receipt.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("journey")
    subparsers.add_parser("validate")
    describe_parser = subparsers.add_parser("describe")
    describe_parser.add_argument("journey")
    list_parser = subparsers.add_parser("artifacts")
    list_parser.add_argument("journey")
    list_parser.add_argument("--transport", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "validate":
            manifest = load_manifest()
            print(f"validated {len(manifest['journeys'])} GUI journeys")
            return 0
        if args.command == "artifacts":
            manifest = load_manifest()
            journey = select_journey(manifest, args.journey)
            paths = make_paths(journey)
            for path in artifact_paths(journey, paths, args.transport):
                print(path.name)
            return 0
        if args.command == "describe":
            manifest = load_manifest()
            print(json.dumps(select_journey(manifest, args.journey), sort_keys=True))
            return 0
        return run_journey(args.journey)
    except (HarnessError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        print(f"gui_acceptance.py: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
