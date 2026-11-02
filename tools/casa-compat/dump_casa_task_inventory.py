#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record the installed CASA's task contracts as versioned fixtures (#931 4a).

Run with CASA's own Python, which has `casatasks`, for example

    "$CASA_PYTHON" -I tools/casa-compat/dump_casa_task_inventory.py \
        --out resources/casa-compat/casa-6.7.6.14

For each CASA task a casa-rs surface mimics, the fixture records what the
task's generated module (`casatasks/<task>.py`, written by xml-casa from the
task's XML) and its implementation (`casatasks/private/task_<task>.py`) say:

- the call signature: every parameter in order, its kind and the `repr` of
  its default;
- the validator schema each parameter is checked against: its CASA type
  (`cReqPath`, `cStr`, ...), its coercions, allowed values and bounds, read
  from the `schema = {...}` literal in the generated `__call__`;
- the one-line description of each parameter from the task's docstring;
- the implementation's `return` and `raise` statements, with their lines,
  which are its return branches and the exceptions it raises itself;
- what a call with the task's defaults does in an empty directory, measured
  in a child process: the generated validator refuses a missing input with
  `AssertionError`, and a task that gets past it raises or returns.

`--check DIR` compares the installed CASA's records with the fixtures in
DIR and exits 1 when they differ, naming each difference. The script exits 2
when it is not run with CASA's Python: a missing CASA is an error, never a
skipped comparison. Nothing here is imported by casa-rs at run time; CASA is
the evidence oracle only.
"""

from __future__ import annotations

import argparse
import ast
import inspect
import json
import os
import subprocess
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any

PINNED_VERSION = "6.7.6.14"

# The CASA tasks casa-rs mimics: each CASA-named surface, and tclean for
# casa-rs's imager (#931). plotms lives in casaplotms; its surface waits on
# its own decision, so its contract is recorded beside the others.
TASKS: tuple[tuple[str, str], ...] = (
    ("applycal", "casatasks"),
    ("bandpass", "casatasks"),
    ("exportfits", "casatasks"),
    ("feather", "casatasks"),
    ("flagdata", "casatasks"),
    ("flagmanager", "casatasks"),
    ("fluxscale", "casatasks"),
    ("gaincal", "casatasks"),
    ("gencal", "casatasks"),
    ("imhead", "casatasks"),
    ("immath", "casatasks"),
    ("immoments", "casatasks"),
    ("impbcor", "casatasks"),
    ("importfits", "casatasks"),
    ("importvla", "casatasks"),
    ("impv", "casatasks"),
    ("imregrid", "casatasks"),
    ("imstat", "casatasks"),
    ("imsubimage", "casatasks"),
    ("mstransform", "casatasks"),
    ("plotms", "casaplotms"),
    ("simobserve", "casatasks"),
    ("split", "casatasks"),
    ("tclean", "casatasks"),
)

PROBE_TIMEOUT_SECONDS = 120


class NotCasa(RuntimeError):
    """The script is not running under CASA's Python."""


def casa_version() -> str:
    try:
        import casatasks
    except ImportError as error:
        raise NotCasa(f"casatasks is not importable ({error}); run with CASA's Python") from None
    return str(casatasks.version_string())


def task_object(name: str, package: str) -> Any:
    module = __import__(package, fromlist=[name])
    return getattr(module, name)


def source_path(path: str) -> str:
    """`path` relative to the site-packages directory that holds it."""
    parts = Path(path).parts
    for index in range(len(parts) - 1, -1, -1):
        if parts[index] == "site-packages":
            return "/".join(parts[index + 1 :])
    return Path(path).name


def literal(node: ast.AST) -> Any:
    """A schema value as JSON: literals as themselves, names (coercion
    functions) as their dotted text, calls such as `float(-1)` as text."""
    if isinstance(node, ast.Dict):
        return {literal(key): literal(value) for key, value in zip(node.keys, node.values)}
    if isinstance(node, (ast.List, ast.Tuple)):
        return [literal(item) for item in node.elts]
    if isinstance(node, ast.Constant):
        return node.value
    text = ast.unparse(node)
    return text.removeprefix("_coerce.")


def call_function(tree: ast.Module) -> ast.FunctionDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "__call__":
            return node
    raise ValueError("no __call__ in the generated task module")


def schema_of(function: ast.FunctionDef) -> dict[str, Any]:
    for node in ast.walk(function):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "schema"
        ):
            return literal(node.value)
    return {}


def descriptions(docstring: str, names: set[str]) -> dict[str, str]:
    """Each parameter's first description line from the generated
    docstring's "parameter descriptions" block, where each name starts a
    line at the block's own indentation and its text runs on, indented
    further (and may hold lines of dashes)."""
    found: dict[str, str] = {}
    inside = False
    base: int | None = None
    for line in docstring.splitlines():
        text = line.strip()
        if "parameter descriptions" in text:
            inside = True
            continue
        if not inside or not text:
            continue
        indent = len(line) - len(line.lstrip())
        if base is None:
            base = indent
        name, _, rest = text.partition(" ")
        if indent == base and name in names and name not in found:
            found[name] = " ".join(rest.split())
    return found


def parse(text: str) -> ast.Module:
    """CASA's own sources, whose stray escape sequences are not ours to
    warn about."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", SyntaxWarning)
        return ast.parse(text)


def implementation(tree: ast.Module, generated: str) -> tuple[str | None, str | None]:
    """The implementation module and function the generated wrapper calls:
    its `from .private.task_x import x as _x_t` import."""
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module and "private" in node.module:
            for alias in node.names:
                if alias.asname and alias.asname.endswith("_t"):
                    package = generated.rsplit(".", 1)[0]
                    module = node.module
                    if node.level:
                        module = f"{package}.{module}"
                    return module, alias.name
    return None, None


def statements(function: ast.FunctionDef, kind: type) -> list[dict[str, Any]]:
    """`function`'s own `return` or `raise` statements, without those of the
    functions and classes it defines."""
    found = []
    pending = list(function.body)
    while pending:
        node = pending.pop(0)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue
        if isinstance(node, kind):
            value = node.value if isinstance(node, ast.Return) else node.exc
            found.append({"line": node.lineno, "value": None if value is None else ast.unparse(value)})
        pending.extend(ast.iter_child_nodes(node))
    return sorted(found, key=lambda entry: entry["line"])


def implementation_record(module_name: str | None, function_name: str | None) -> dict[str, Any] | None:
    if module_name is None or function_name is None:
        return None
    module = sys.modules.get(module_name) or __import__(module_name, fromlist=[function_name])
    tree = parse(inspect.getsource(module))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            return {
                "source": source_path(inspect.getsourcefile(module) or ""),
                "function": function_name,
                "line": node.lineno,
                "returns": statements(node, ast.Return),
                "raises": statements(node, ast.Raise),
            }
    return None


def parameters(task: Any, schema: dict[str, Any], described: dict[str, str]) -> list[dict[str, Any]]:
    signature = inspect.signature(type(task).__call__)
    entries = []
    for parameter in signature.parameters.values():
        if parameter.name == "self":
            continue
        default = None if parameter.default is inspect.Parameter.empty else repr(parameter.default)
        entries.append(
            {
                "name": parameter.name,
                "kind": parameter.kind.name,
                "default": default,
                "schema": schema.get(parameter.name),
                "description": described.get(parameter.name),
            }
        )
    return entries


def probe(name: str, package: str) -> dict[str, Any]:
    """What a call with every default does, in an empty directory."""
    code = (
        "import json, sys\n"
        f"from {package} import {name} as task\n"
        "try:\n"
        "    value = task()\n"
        "except BaseException as error:\n"
        "    message = str(error).strip().splitlines()\n"
        "    print(json.dumps({'raises': type(error).__name__,\n"
        "                      'message': message[0] if message else ''}))\n"
        "else:\n"
        "    print(json.dumps({'returns': repr(value)[:200]}))\n"
    )
    with tempfile.TemporaryDirectory(prefix=f"casa-probe-{name}-") as directory:
        try:
            completed = subprocess.run(
                [sys.executable, "-I", "-c", code],
                cwd=directory,
                capture_output=True,
                text=True,
                timeout=PROBE_TIMEOUT_SECONDS,
                # No display, on any machine: a task that would open a
                # window must not, and its refusal reads the same everywhere.
                env={
                    **{key: value for key, value in os.environ.items() if key != "DISPLAY"},
                    "QT_QPA_PLATFORM": "offscreen",
                    "MPLBACKEND": "Agg",
                },
            )
        except subprocess.TimeoutExpired:
            return {"timed_out": PROBE_TIMEOUT_SECONDS}
    for line in reversed(completed.stdout.splitlines()):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return {"exit_code": completed.returncode}


def record(name: str, package: str, version: str) -> dict[str, Any]:
    task = task_object(name, package)
    generated = type(task).__module__
    module = sys.modules[generated]
    text = inspect.getsource(module)
    tree = parse(text)
    header = text.splitlines()[:2]
    impl_module, impl_function = implementation(tree, generated)
    names = set(inspect.signature(type(task).__call__).parameters) - {"self"}
    return {
        "casa_version": version,
        "task": name,
        "package": package,
        "source": source_path(inspect.getsourcefile(module) or ""),
        "generator": " ".join(" ".join(line.strip("# ").split()) for line in header),
        "parameters": parameters(
            task, schema_of(call_function(tree)), descriptions(type(task).__doc__ or "", names)
        ),
        "implementation": implementation_record(impl_module, impl_function),
        "defaults_call": probe(name, package),
    }


def records(version: str) -> dict[str, dict[str, Any]]:
    return {name: record(name, package, version) for name, package in TASKS}


def write(directory: Path, version: str, tasks: dict[str, dict[str, Any]]) -> None:
    (directory / "tasks").mkdir(parents=True, exist_ok=True)
    for name, entry in tasks.items():
        path = directory / "tasks" / f"{name}.json"
        path.write_text(json.dumps(entry, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    index = {"casa_version": version, "tasks": sorted(tasks)}
    (directory / "inventory.json").write_text(
        json.dumps(index, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def differences(directory: Path, version: str, tasks: dict[str, dict[str, Any]]) -> list[str]:
    found = []
    index_path = directory / "inventory.json"
    if not index_path.is_file():
        return [f"{index_path} is missing"]
    index = json.loads(index_path.read_text(encoding="utf-8"))
    if index.get("casa_version") != version:
        found.append(f"the fixtures record CASA {index.get('casa_version')}, the installed CASA is {version}")
    if sorted(index.get("tasks", [])) != sorted(tasks):
        found.append(f"the fixtures list {index.get('tasks')}, the inventory {sorted(tasks)}")
    for name, entry in tasks.items():
        path = directory / "tasks" / f"{name}.json"
        if not path.is_file():
            found.append(f"{path} is missing")
            continue
        committed = json.loads(path.read_text(encoding="utf-8"))
        for key in sorted(set(entry) | set(committed)):
            if entry.get(key) != committed.get(key):
                found.append(f"{name}: {key} differs from the installed CASA")
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--out", type=Path, help="write the fixtures to this directory")
    action.add_argument("--check", type=Path, help="compare the installed CASA with these fixtures")
    args = parser.parse_args()
    try:
        version = casa_version()
    except NotCasa as error:
        print(f"dump_casa_task_inventory: {error}", file=sys.stderr)
        return 2
    if version != PINNED_VERSION:
        print(
            f"dump_casa_task_inventory: CASA {version} is installed; the inventory is pinned to "
            f"{PINNED_VERSION}",
            file=sys.stderr,
        )
        return 2
    tasks = records(version)
    if args.out:
        write(args.out, version, tasks)
        print(f"dump_casa_task_inventory: wrote {len(tasks)} tasks to {args.out}")
        return 0
    found = differences(args.check, version, tasks)
    for line in found:
        print(f"dump_casa_task_inventory: {line}", file=sys.stderr)
    if found:
        return 1
    print(f"dump_casa_task_inventory: {len(tasks)} tasks match CASA {version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
