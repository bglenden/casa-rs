#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Extract a tclean case matrix from CASA's own tclean test suite (#980).

Evidence tooling, not a runtime dependency. It reads CASA's
`test_task_tclean.py` (and `task_tclean.py`, for tclean's positional order)
with Python's `ast` and never executes them. It interprets each test method
statically, as a serial (non-MPI) CASA run on Linux would execute it:
`self.parallel` is False, `ParallelTaskHelper.isMPIEnabled()` is False, and a
GPU check is unknown. Every `tclean(...)` call the test makes becomes a case
when its arguments resolve to literals: constants, `self.msfile`, `self.img`
and its concatenations, `self.maskname`, module constants, and locals
assigned from those. A call that does not resolve goes to the matrix's
`unresolved` list with its reasons, never a guess.

The matrix keeps the format `tclean_case_matrix.py` reads, with `common`
empty so that every parameter, `datacolumn` included, is exactly what the
test passes:

    {"inputs": {name: path relative to the casatestdata root},
     "common": {},
     "cases": {name: case},
     "unresolved": [...]}

A case is one tclean call, named `<TestClass>.<test>.<n>` for the test's
n-th tclean call:

    {"vis": input name (a list of names when the test passes a list),
     "imagename": the test's image name (informational for a single call),
     "params": tclean's other keyword arguments (`parallel=self.parallel`
               reads False), "line": the call's line in the test file,
     "note": ..., "skip_scalars": false}

and may carry these additions, which `tclean_case_matrix.py` does not read
yet:

* `"sequence"`: calls that depend on an earlier call of the same test (a
  restart into the same imagename, `calcres`/`calcpsf=False`, a `startmodel`
  or `mask` naming an earlier call's product, a shared `cfcache`). The case
  then has `"sequence": [step, ...]` in place of `vis`/`imagename`/`params`,
  each step `{"call", "depends_on", "line", "vis", "imagename", "params"}`
  (`call` numbers the test's tclean calls in order, `depends_on` names the
  earlier calls it needs); a driver replays the steps in order in one working
  directory, each with its own `imagename`.
* `"stage"`: inputs (masks, CF caches, ephemeris tables) a parameter names.
  A driver copies each into the case's working directory under its input
  name, which is the relative path the parameter uses, and runs the calls
  there. Paths in `params` are relative to that directory, as in CASA's test.
* `"mutates_vis": true`: some call writes a model into the MeasurementSet
  (`savemodel`), so each implementation needs its own copy of it.
* `"expect_error"`: the exception CASA's test asserts (`assertRaises`).
* `"tolerates_error": true`: the call is inside a `try` the test ignores.
* `"casa_skip"`: why CASA's suite skips the test (`unittest.skip`, or a
  `skipIf` condition that holds or is unknown in a serial run).
* `"casa_environment"`: environment variables the test set before the call.

A sequence case is named by its calls joined with `+` (`...restart_mfs.1+2+3`).
An `unresolved` entry names its calls the same way and gives each reason,
the arguments as far as they resolve (`{"unresolved": reason}` elsewhere),
the content of files the test writes, and `after_cases`, the resolved case
holding the earlier calls of its chain (a chain's resolved leading calls
stay a case). Calls in branches a serial run does not take
(`if self.parallel:`) are not cases; the summary counts them. `delmod` and
`TestHelpers.delmodels` only remove model data, which tclean does not image,
so they are noted, not replayed; any other step that changes an input
(flagdata, uvsub, a calibrater open), makes one (split, concat, write_file,
makemask, a copy) or changes an earlier call's products makes the dependent
call unresolved.

The summary counts tests, calls and cases by class, unresolved cases by
reason, the casatestdata inputs and how many resolved cases use each, and
how often each parameter and value appears in the resolved calls.

    python3 -I tools/casa-compat/extract_tclean_test_cases.py \\
        --tests .../test_task_tclean.py --task .../task_tclean.py \\
        --output resources/casa-compat/tclean-casa-test-cases.json \\
        --summary resources/casa-compat/tclean-casa-test-cases-summary.json
"""

import argparse
import ast
import collections
import fnmatch
import hashlib
import json
import posixpath
import re
import shlex
from pathlib import Path

CASA_VERSION = "6.7.6.14"
TESTDATA = "<casatestdata>/"
REFDATA = TESTDATA + "unittest/tclean/"
# The attributes `testref_base.setUp` sets.
BASE_SELF = {
    "epsilon": 0.05,
    "msfile": "",
    "img": "tst",
    "cfcache": "cfcach",
    "img_subdir": "refimager_tst_subdir",
    "parallel": False,
    "nnode": 0,
}
# A product tclean reads back when it restarts into an imagename.
PRODUCT = re.compile(
    r"\.(psf|residual|model|mask|pb|sumwt|weight|image|alpha|beta|workdirectory|gridwt\w*)"
    r"(\.tt\d+)?$"
)
PRODUCT_NAMES = ("psf", "residual", "model", "mask", "pb", "sumwt", "weight", "image")
# Calls that read only, or report: they change nothing tclean reads.
READ_ONLY = re.compile(
    r"^(self\.assert\w*|self\.th\.(check\w*|verdict|get_pix|exists|get_log_length|"
    r"mergeParaCubeResults)|self\.check_final|self\.get_beam|print|casalog\.\w+|np\.\w+|"
    r"numpy\.\w+|os\.path\.\w+|inspect\.\w+|len|list|min|max|int|str|float|range|"
    r"enumerate|sorted|open|\w+\.(read|seek|append|keys|find|rstrip|split|strip|fill)|"
    r"imhead|_ia\.(statistics|getchunk|summary|shape|close|toworld|beamarea|"
    r"brightnessunit|restoringbeam|getregion|coordsys|open)|_cb\.close|"
    r"\w+\.reset|\w+\.setpbimage|\w+\.setpbpoly|vpmanager|ctsys\.resolve|"
    r"ParallelTaskHelper\.isMPIEnabled|testref_base\.isGPUEnabled|self\.isGPUEnabled|"
    r"super.*|retpar.*|ret\w*\[.*)$"
)
# Calls that only remove model data from a MeasurementSet.
MODEL_RESET = ("delmod", "self.th.delmodels")
# Calls that change a MeasurementSet tclean images.
MS_CHANGES = ("flagdata", "uvsub", "_cb.open", "setjy", "applycal", "clearcal", "statwt")
# `_ia` methods that write the open image.
IMAGE_WRITES = re.compile(
    r"^\w+\.(putchunk|calcmask|set|putregion|maskhandler|"
    r"setrestoringbeam|setbrightnessunit|modify|fromarray)$"
)
# Keyword arguments naming what a CASA task writes.
OUTPUT_KEYWORDS = ("outfile", "output", "outputvis", "concatvis", "outputmask")


class Unresolved:
    """A value the extractor cannot know without running the test."""

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return f"Unresolved({self.reason!r})"


def unresolved_in(value):
    """The first Unresolved inside `value`, if any."""
    if isinstance(value, Unresolved):
        return value
    if isinstance(value, dict):
        items = list(value.keys()) + list(value.values())
    elif isinstance(value, (list, tuple)):
        items = value
    else:
        return None
    for item in items:
        found = unresolved_in(item)
        if found:
            return found
    return None


def plain(value):
    """`value` as JSON: tuples become lists."""
    if isinstance(value, dict):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


def short(node, limit=160):
    text = ast.unparse(node)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def calls_in(node):
    """The calls under `node`, in evaluation order (arguments first)."""
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
            continue
        yield from calls_in(child)
    if isinstance(node, ast.Call):
        yield node


def tclean_signature(task_path):
    tree = ast.parse(task_path.read_text())
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "tclean":
            return [arg.arg for arg in node.args.args]
    raise SystemExit(f"{task_path}: no tclean function")


def module_constants(tree):
    """Module-level names with values the tests use."""
    names = {"refdatapath": REFDATA}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id not in names:
                try:
                    names[target.id] = ast.literal_eval(node.value)
                except ValueError:
                    names[target.id] = Unresolved(f"module value {short(node.value)}")
    return names


class Call:
    """One tclean call the test makes."""

    def __init__(self, number, line, kwargs):
        self.number = number
        self.line = line
        self.kwargs = kwargs
        self.reasons = []
        self.deps = set()
        self.stage = set()
        self.notes = []
        self.flags = {}


class TestRun:
    """One test method, interpreted statement by statement."""

    def __init__(self, extractor, cls, method, self_attrs, setup_files):
        self.x = extractor
        self.cls = cls
        self.method = method
        self.self = dict(self_attrs)
        self.environment = {}
        self.calls = []
        self.not_run = []
        self.context = []
        self.expect_error = None
        self.tolerates_error = False
        # Files tclean can name, by the relative path the test uses.
        self.staged = {}  # path -> input name
        self.namespaces = {}  # imagename (or cfcache) -> {"call": n, "tainted": reason}
        self.made = dict(setup_files)  # path -> the non-tclean step that made it
        self.ms_changed = {}  # MS path -> the step that changed it
        self.model_reset = set()
        self.open_images = {}
        self.unknown_namespace = None
        self.via = None

    # Expressions ---------------------------------------------------------

    def ev(self, node, scope):
        method = getattr(self, "ev_" + type(node).__name__, None)
        if method is None:
            return Unresolved(f"expression {short(node, 80)}")
        return method(node, scope)

    def ev_Constant(self, node, scope):
        return node.value

    def ev_List(self, node, scope):
        return [self.ev(item, scope) for item in node.elts]

    ev_Tuple = ev_List

    def ev_Dict(self, node, scope):
        if any(key is None for key in node.keys):
            return Unresolved("dict unpacking")
        keys = [self.ev(key, scope) for key in node.keys]
        if any(isinstance(key, (Unresolved, list, dict)) for key in keys):
            return Unresolved(f"dict key in {short(node, 80)}")
        return {key: self.ev(value, scope) for key, value in zip(keys, node.values)}

    def ev_Name(self, node, scope):
        if node.id in scope:
            return scope[node.id]
        if node.id in self.x.constants:
            return self.x.constants[node.id]
        if node.id in ("True", "False", "None"):
            return {"True": True, "False": False, "None": None}[node.id]
        return Unresolved(f"name {node.id!r} is not known statically")

    def ev_Attribute(self, node, scope):
        if isinstance(node.value, ast.Name) and node.value.id == "self":
            if node.attr in self.self:
                return self.self[node.attr]
            return Unresolved(f"self.{node.attr} is not set")
        if short(node) == "sys.platform":
            return "linux"
        return Unresolved(f"attribute {short(node, 80)}")

    def ev_BinOp(self, node, scope):
        left, right = self.ev(node.left, scope), self.ev(node.right, scope)
        for side in (left, right):
            if isinstance(side, Unresolved):
                return side
        operators = {
            ast.Add: lambda a, b: a + b,
            ast.Sub: lambda a, b: a - b,
            ast.Mult: lambda a, b: a * b,
            ast.Div: lambda a, b: a / b,
            ast.FloorDiv: lambda a, b: a // b,
            ast.Mod: lambda a, b: a % b,
            ast.Pow: lambda a, b: a**b,
        }
        operator = operators.get(type(node.op))
        if operator is None:
            return Unresolved(f"operator in {short(node, 80)}")
        try:
            return operator(left, right)
        except Exception:  # noqa: BLE001 - any failure leaves the value unknown
            return Unresolved(f"computed value {short(node, 80)}")

    def ev_UnaryOp(self, node, scope):
        value = self.ev(node.operand, scope)
        if isinstance(value, Unresolved):
            return value
        if isinstance(node.op, ast.Not):
            return not value
        if isinstance(node.op, ast.USub):
            return -value
        if isinstance(node.op, ast.UAdd):
            return +value
        return Unresolved(f"operator in {short(node, 80)}")

    def ev_BoolOp(self, node, scope):
        values = [self.ev(value, scope) for value in node.values]
        known = [value for value in values if not isinstance(value, Unresolved)]
        if isinstance(node.op, ast.Or):
            if any(known):
                return next(value for value in known if value)
        elif not all(known):
            return next(value for value in known if not value)
        unknown = [value for value in values if isinstance(value, Unresolved)]
        return unknown[0] if unknown else values[-1]

    def ev_Compare(self, node, scope):
        left = self.ev(node.left, scope)
        result = True
        for op, right_node in zip(node.ops, node.comparators):
            right = self.ev(right_node, scope)
            if isinstance(left, Unresolved) or isinstance(right, Unresolved):
                return left if isinstance(left, Unresolved) else right
            tests = {
                ast.Eq: lambda a, b: a == b,
                ast.NotEq: lambda a, b: a != b,
                ast.Lt: lambda a, b: a < b,
                ast.LtE: lambda a, b: a <= b,
                ast.Gt: lambda a, b: a > b,
                ast.GtE: lambda a, b: a >= b,
                ast.In: lambda a, b: a in b,
                ast.NotIn: lambda a, b: a not in b,
                ast.Is: lambda a, b: a is b,
                ast.IsNot: lambda a, b: a is not b,
            }
            try:
                result = result and tests[type(op)](left, right)
            except Exception:  # noqa: BLE001
                return Unresolved(f"comparison {short(node, 80)}")
            left = right
        return result

    def ev_IfExp(self, node, scope):
        test = self.ev(node.test, scope)
        if isinstance(test, Unresolved):
            return test
        return self.ev(node.body if test else node.orelse, scope)

    def ev_Subscript(self, node, scope):
        value = self.ev(node.value, scope)
        if isinstance(value, Unresolved):
            return value
        if isinstance(node.slice, ast.Slice):
            parts = [
                None if part is None else self.ev(part, scope)
                for part in (node.slice.lower, node.slice.upper, node.slice.step)
            ]
            if any(isinstance(part, Unresolved) for part in parts):
                return Unresolved(f"slice {short(node, 80)}")
            index = slice(*parts)
        else:
            index = self.ev(node.slice, scope)
            if isinstance(index, Unresolved):
                return index
        try:
            return value[index]
        except Exception:  # noqa: BLE001
            return Unresolved(f"subscript {short(node, 80)}")

    def ev_JoinedStr(self, node, scope):
        parts = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                parts.append(str(value.value))
                continue
            item = self.ev(value.value, scope)
            if isinstance(item, Unresolved):
                return item
            if value.format_spec is not None or value.conversion not in (-1, 115):
                return Unresolved(f"formatted value {short(node, 80)}")
            parts.append(str(item))
        return "".join(parts)

    def ev_Call(self, node, scope):
        name = short(node.func)
        args = [self.ev(arg, scope) for arg in node.args]
        kwargs = {kw.arg: self.ev(kw.value, scope) for kw in node.keywords if kw.arg}
        if name == "ParallelTaskHelper.isMPIEnabled":
            return False
        if name in ("testref_base.isGPUEnabled", "self.isGPUEnabled"):
            return Unresolved("GPU availability (an L4 GPU for HPG)")
        values = args + list(kwargs.values())
        missing = unresolved_in(values)
        if missing:
            return missing
        pure = {
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "len": len,
            "abs": abs,
            "round": round,
            "min": min,
            "max": max,
            "list": list,
        }
        try:
            if name in pure:
                return pure[name](*args)
            if name == "os.path.join":
                return posixpath.join(*args)
            if name == "ctsys.resolve":
                return TESTDATA + args[0]
            if isinstance(node.func, ast.Attribute):
                target = self.ev(node.func.value, scope)
                if (
                    isinstance(target, Unresolved)
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id not in self.x.constants
                ):
                    return target
                if isinstance(target, str) and node.func.attr in (
                    "join",
                    "replace",
                    "strip",
                    "rstrip",
                    "lstrip",
                    "upper",
                    "lower",
                    "format",
                    "split",
                ):
                    return getattr(target, node.func.attr)(*args, **kwargs)
        except Exception:  # noqa: BLE001 - any failure leaves the value unknown
            return Unresolved(f"value of {short(node, 100)} (fails statically)")
        return Unresolved(f"value of {short(node, 100)}")

    # Statements ----------------------------------------------------------

    def run(self, body, scope):
        for statement in body:
            self.statement(statement, scope)

    def statement(self, node, scope):
        kind = type(node).__name__
        if kind in (
            "FunctionDef",
            "AsyncFunctionDef",
            "ClassDef",
            "Pass",
            "Import",
            "ImportFrom",
            "Global",
            "Nonlocal",
            "Break",
            "Continue",
        ):
            return
        if kind == "If":
            self.branch(node, scope)
        elif kind in ("For", "While"):
            self.loop(node, scope)
        elif kind == "Try":
            self.attempt(node, scope)
        elif kind == "With":
            self.within(node, scope)
        else:
            for call in calls_in(node):
                self.call(call, scope)
            if kind == "Assign":
                for target in node.targets:
                    self.assign(target, node.value, scope)
            elif kind == "AugAssign":
                value = self.ev(ast.BinOp(left=node.target, op=node.op, right=node.value), scope)
                self.assign(node.target, None, scope, value)
            elif kind == "AnnAssign" and node.value is not None:
                self.assign(node.target, node.value, scope)

    def assign(self, target, value_node, scope, value=None):
        if value_node is not None:
            value = self.ev(value_node, scope)
            if isinstance(value_node, ast.Call) and short(value_node.func) in (
                "tclean",
                "self.run_cubetclean",
            ):
                value = Unresolved("a tclean return value")
        if self.context and not isinstance(value, Unresolved):
            value = Unresolved(f"assigned {self.context[-1]}")
        if isinstance(target, ast.Name):
            scope[target.id] = value
        elif isinstance(target, ast.Attribute) and short(target.value) == "self":
            self.self[target.attr] = value
        elif isinstance(target, ast.Subscript) and short(target.value) == "os.environ":
            key = self.ev(target.slice, scope)
            if isinstance(key, str):
                self.environment[key] = plain(value) if not unresolved_in(value) else None
        elif isinstance(target, (ast.Tuple, ast.List)):
            items = value if isinstance(value, (list, tuple)) else None
            for index, element in enumerate(target.elts):
                item = (
                    items[index]
                    if items is not None and len(items) == len(target.elts)
                    else Unresolved(f"unpacked from {short(value_node or target, 60)}")
                )
                self.assign(element, None, scope, item)

    def branch(self, node, scope):
        test = self.ev(node.test, scope)
        if isinstance(test, Unresolved):
            self.context.append(f"under the condition {short(node.test, 80)}")
            self.run(node.body, scope)
            self.run(node.orelse, scope)
            self.context.pop()
            return
        taken, skipped = (node.body, node.orelse) if test else (node.orelse, node.body)
        for statement in skipped:
            for call in calls_in(statement):
                if short(call.func) in ("tclean", "self.run_cubetclean"):
                    self.not_run.append(
                        {
                            "line": call.lineno,
                            "reason": f"branch not taken in a serial run: {short(node.test)}",
                        }
                    )
        self.run(taken, scope)

    def loop(self, node, scope):
        where = f"in a loop (line {node.lineno})"
        self.context.append(where)
        if isinstance(node, ast.For):
            self.assign(node.target, None, scope, Unresolved(f"loop variable {where}"))
        self.run(node.body, scope)
        self.run(node.orelse, scope)
        self.context.pop()

    def attempt(self, node, scope):
        tolerates = self.tolerates_error
        self.tolerates_error = bool(node.handlers)
        self.run(node.body, scope)
        self.tolerates_error = tolerates
        for handler in node.handlers:
            self.context.append(f"in an except handler (line {handler.lineno})")
            self.run(handler.body, scope)
            self.context.pop()
        self.run(node.orelse, scope)
        self.run(node.finalbody, scope)

    def within(self, node, scope):
        expected = None
        for item in node.items:
            text = short(item.context_expr)
            match = re.match(r"self\.assertRaises\((\w+)", text)
            if match:
                expected = match.group(1)
            else:
                for call in calls_in(item.context_expr):
                    self.call(call, scope)
            if item.optional_vars is not None:
                self.assign(
                    item.optional_vars, None, scope, Unresolved(f"bound by with {text[:60]}")
                )
        previous = self.expect_error
        self.expect_error = expected or previous
        self.run(node.body, scope)
        self.expect_error = previous

    # Calls ---------------------------------------------------------------

    def call(self, node, scope):
        name = short(node.func, 400)
        if name == "tclean":
            self.tclean(node, scope)
        elif name.startswith("self.run_cubetclean"):
            self.helper(node, scope)
        elif name in ("self.prepData", "self.delData"):
            self.prep_data(node, scope, name == "self.delData")
        elif name in ("self.prepInputmask", "self.prepCfcache", "self.prepInputTextFile"):
            self.prep_file(node, scope, name)
        elif name == "self.th.write_file":
            path = self.ev(node.args[0], scope) if node.args else Unresolved("no path")
            content = self.ev(node.args[1], scope) if len(node.args) > 1 else None
            if isinstance(path, str):
                text = content if isinstance(content, str) else None
                self.made[path] = {
                    "step": f"self.th.write_file (line {node.lineno})",
                    "content": text,
                }
        elif name in MODEL_RESET:
            for ms in self.paths(node, scope, ("vis", "msname")):
                self.model_reset.add(ms)
        elif name in MS_CHANGES:
            for ms in self.paths(node, scope, ("vis", "msname", "tablename")):
                self.ms_changed[ms] = f"{short(node, 120)} (line {node.lineno})"
        elif name == "os.system":
            self.shell(node, scope)
        elif name in ("shutil.rmtree", "os.remove"):
            for path in self.paths(node, scope, ("path",)):
                self.remove(path, f"{name} (line {node.lineno})")
        elif name in ("shutil.copytree", "shutil.copy", "shutil.copyfile", "os.rename"):
            args = [self.ev(arg, scope) for arg in node.args]
            if len(args) == 2 and isinstance(args[1], str):
                self.produce(args[1], f"{short(node, 120)} (line {node.lineno})")
                if name == "os.rename" and isinstance(args[0], str):
                    self.remove(args[0], f"os.rename (line {node.lineno})")
        elif re.match(r"^_ia\.open$", name):
            paths = self.paths(node, scope, ("infile",))
            self.open_images["_ia"] = paths[0] if paths else None
        elif IMAGE_WRITES.match(name) and name.split(".")[0] in self.open_images:
            path = self.open_images.get(name.split(".")[0])
            if path:
                self.produce(path, f"{name} on {path} (line {node.lineno})")
        elif READ_ONLY.match(name):
            return
        else:
            self.other(node, scope, name)

    def paths(self, node, scope, keywords):
        """String values of a call's first argument or its named keywords."""
        values = [self.ev(node.args[0], scope)] if node.args else []
        values += [self.ev(kw.value, scope) for kw in node.keywords if kw.arg in keywords]
        out = []
        for value in values:
            out += [
                item
                for item in (value if isinstance(value, list) else [value])
                if isinstance(item, str)
            ]
        return out

    def other(self, node, scope, name):
        """A CASA task or tool step: what it writes, by its output keywords."""
        step = f"{short(node, 120)} (line {node.lineno})"
        outputs = [self.ev(kw.value, scope) for kw in node.keywords if kw.arg in OUTPUT_KEYWORDS]
        if name == "widebandpbcor":
            image = self.paths(node, scope, ("imagename",))
            outputs += [image[0] + ".pbcor"] if image else []
        if name.endswith("saveastable"):
            outputs += [self.ev(arg, scope) for arg in node.args[:1]]
            outputs += [self.ev(kw.value, scope) for kw in node.keywords if kw.arg == "tablename"]
        if name == "self.th.fill_mask":
            outputs += [self.ev(arg, scope) for arg in node.args[:1]]
        if name == "gclean":
            outputs += [self.ev(kw.value, scope) for kw in node.keywords if kw.arg == "imagename"]
        for output in outputs:
            if isinstance(output, str):
                self.produce(output, step)
            elif isinstance(output, Unresolved):
                self.unknown_namespace = step

    def shell(self, node, scope):
        command = self.ev(node.args[0], scope) if node.args else Unresolved("no command")
        step = f"os.system({short(node.args[0], 100)}) (line {node.lineno})"
        if not isinstance(command, str):
            self.unknown_namespace = step
            return
        for part in re.split(r"&&|;", command):
            words = shlex.split(part)
            if not words:
                continue
            paths = [word for word in words[1:] if not word.startswith("-")]
            if words[0] == "rm":
                for path in paths:
                    self.remove(path, step)
            elif words[0] in ("cp", "mv") and len(paths) == 2:
                self.produce(paths[1], step)
                if words[0] == "mv":
                    self.remove(paths[0], step)
            else:
                self.unknown_namespace = step

    def remove(self, pattern, step):
        pattern = pattern.rstrip("/")
        for name in list(self.namespaces):
            whole = all(fnmatch.fnmatch(f"{name}.{product}", pattern) for product in PRODUCT_NAMES)
            if whole or name == pattern or name.startswith(pattern + "/"):
                del self.namespaces[name]
            elif any(
                fnmatch.fnmatch(f"{name}.{product}{tail}", pattern)
                for product in PRODUCT_NAMES
                for tail in ("", ".tt0")
            ):
                self.namespaces[name]["tainted"] = f"part of it removed by {step}"
        for registry in (self.made, self.staged):
            for path in [path for path in registry if fnmatch.fnmatch(path, pattern)]:
                del registry[path]

    def produce(self, path, step):
        self.made[path] = {"step": step}
        for name, entry in self.namespaces.items():
            if path.startswith(name + ".") and PRODUCT.match(path[len(name) :]):
                entry["tainted"] = f"{path} changed by {step}"

    def prep_data(self, node, scope, delete):
        msname = (
            self.ev(node.args[0], scope)
            if node.args
            else (self.ev(node.keywords[0].value, scope) if node.keywords else "")
        )
        if isinstance(msname, Unresolved):
            self.self["msfile"] = msname
            return
        if msname:
            self.self["msfile"] = msname
        msfile = self.self["msfile"]
        self.remove(self.self["img_subdir"], f"{short(node.func)} (line {node.lineno})")
        self.remove(f"{self.self['img']}*", f"{short(node.func)} (line {node.lineno})")
        self.unknown_namespace = None
        if not isinstance(msfile, str) or not msfile:
            return
        self.ms_changed.pop(msfile, None)
        self.model_reset.discard(msfile)
        self.made.pop(msfile, None)
        if delete:
            self.staged.pop(msfile, None)
        else:
            self.staged[msfile] = self.x.input(
                msfile, "unittest/tclean/" + msfile, "measurement set"
            )

    def prep_file(self, node, scope, name):
        value = self.ev(node.args[0], scope) if node.args else ""
        attribute = {
            "self.prepInputmask": "maskname",
            "self.prepCfcache": "cfcache",
            "self.prepInputTextFile": "textfile",
        }[name]
        if isinstance(value, Unresolved):
            self.self[attribute] = value
            return
        if value:
            self.self[attribute] = value
        path = self.self.get(attribute)
        if isinstance(path, str) and path:
            self.made.pop(path, None)
            kind = {"maskname": "mask", "cfcache": "CF cache", "textfile": "region text"}
            self.staged[path] = self.x.input(path, "unittest/tclean/" + path, kind[attribute])

    def helper(self, node, scope):
        """Inline `test_cube.run_cubetclean(testid)`."""
        definition = self.x.helpers.get((self.cls, "run_cubetclean"))
        if definition is None:
            return
        args = [self.ev(arg, scope) for arg in node.args]
        local = {param.arg: value for param, value in zip(definition.args.args[1:], args)}
        shown = ", ".join(
            f"{param.arg}={value!r}" for param, value in zip(definition.args.args[1:], args)
        )
        self.via = f"made by {self.cls}.run_cubetclean({shown}), called at line {node.lineno}"
        self.run(definition.body, local)
        self.via = None

    # tclean --------------------------------------------------------------

    def tclean(self, node, scope):
        kwargs = {}
        for index, arg in enumerate(node.args):
            kwargs[self.x.signature[index]] = self.ev(arg, scope)
        for keyword in node.keywords:
            if keyword.arg is None:
                kwargs["**"] = Unresolved(f"keyword unpacking {short(keyword.value, 60)}")
            else:
                kwargs[keyword.arg] = self.ev(keyword.value, scope)
        call = Call(len(self.calls) + 1, node.lineno, kwargs)
        self.calls.append(call)
        if self.context:
            call.reasons.append(f"the call is {self.context[-1]}")
        for name, value in kwargs.items():
            missing = unresolved_in(value)
            if missing:
                call.reasons.append(f"{name}: {missing.reason}")
        unknown = [name for name in kwargs if name not in self.x.signature and name != "**"]
        if unknown:
            call.notes.append(f"parameters tclean does not take: {', '.join(unknown)}")
        if self.via:
            call.notes.append(self.via)
        if self.expect_error:
            call.flags["expect_error"] = self.expect_error
        if self.tolerates_error:
            call.flags["tolerates_error"] = True
        if self.environment:
            call.flags["casa_environment"] = dict(self.environment)
        if kwargs.get("savemodel") not in (None, "none", "") and not isinstance(
            kwargs.get("savemodel"), Unresolved
        ):
            call.flags["mutates_vis"] = True
        if self.unknown_namespace:
            call.reasons.append(
                f"follows a step whose outputs are unknown: {self.unknown_namespace}"
            )
        self.references(call)
        self.register(call)

    def references(self, call):
        """Resolve the files `call` names, and the earlier calls it depends on."""
        kwargs = call.kwargs
        vis = kwargs.get("vis")
        for ms in vis if isinstance(vis, list) else [vis]:
            if isinstance(ms, str):
                self.reference(call, "vis", ms, ms_input=True)
        image = kwargs.get("imagename")
        if isinstance(image, str):
            if image in self.namespaces:
                entry = self.namespaces[image]
                call.deps.add(entry["call"])
                if entry["tainted"]:
                    call.reasons.append(f"imagename {image!r}: {entry['tainted']}")
            made = sorted(
                path
                for path in self.made
                if path.startswith(image + ".") and PRODUCT.match(path[len(image) :])
            )
            if made and not (image in self.namespaces and self.namespaces[image]["tainted"]):
                call.reasons.append(
                    f"imagename {image!r}: {', '.join(made)} made by {self.made[made[0]]['step']}"
                )
        for name in ("startmodel", "mask", "outlierfile", "vptable", "cfcache", "phasecenter"):
            value = kwargs.get(name)
            items = value if isinstance(value, list) else [value]
            for index, item in enumerate(items):
                if not isinstance(item, str) or not item.strip():
                    continue
                if name == "mask" and "[" in item:
                    continue
                if name == "phasecenter" and not (
                    item.startswith(TESTDATA)
                    or item.endswith(".tab")
                    or item in self.staged
                    or item in self.made
                ):
                    continue
                rewritten = self.reference(call, name, item)
                if rewritten != item:
                    if isinstance(value, list):
                        value[index] = rewritten
                    else:
                        kwargs[name] = rewritten

    def reference(self, call, name, path, ms_input=False):
        """Classify one file `call` names; return the path the case uses."""
        if path.startswith(TESTDATA):
            relative = path[len(TESTDATA) :]
            input_name = self.x.input(
                Path(relative).name, relative, "measurement set" if ms_input else f"{name} table"
            )
            if not ms_input:
                call.stage.add(input_name)
            return input_name
        if path in self.made:
            made = self.made[path]
            what = (
                "a file the test writes" if "content" in made else "a file a non-tclean step makes"
            )
            call.reasons.append(f"{name}: {path!r} is {what} ({made['step']})")
            call.flags.setdefault("written_files", {})[path] = made
            return path
        if path in self.staged:
            if ms_input:
                if path in self.ms_changed:
                    call.reasons.append(f"vis: {path!r} was changed by {self.ms_changed[path]}")
                if path in self.model_reset:
                    call.notes.append(f"the test removed {path}'s model data (delmod) first")
            else:
                call.stage.add(self.staged[path])
            return path
        for image, entry in sorted(self.namespaces.items(), key=lambda item: -len(item[0])):
            if path == image or path.startswith((image + ".", image + "/")):
                if entry["call"] != call.number:
                    call.deps.add(entry["call"])
                    if entry["tainted"] and PRODUCT.match(path[len(image) :] or ".image"):
                        call.reasons.append(f"{name} {path!r}: {entry['tainted']}")
                return path
        if name == "cfcache":
            return path  # a new cache this call writes
        call.reasons.append(f"{name}: {path!r} is neither test data nor an earlier call's product")
        return path

    def register(self, call):
        image = call.kwargs.get("imagename")
        if isinstance(image, str):
            self.namespaces[image] = {"call": call.number, "tainted": None}
        else:
            self.unknown_namespace = f"tclean call {call.number} (line {call.line})"
        cfcache = call.kwargs.get("cfcache")
        if (
            isinstance(cfcache, str)
            and cfcache
            and cfcache not in self.staged
            and cfcache not in self.namespaces
        ):
            self.namespaces[cfcache] = {"call": call.number, "tainted": None}


class Extractor:
    def __init__(self, tests_path, task_path):
        self.tests_path = tests_path
        self.task_path = task_path
        self.signature = tclean_signature(task_path)
        self.tree = ast.parse(tests_path.read_text())
        self.constants = module_constants(self.tree)
        self.inputs = {}
        self.kinds = {}
        self.helpers = {}
        self.cases = {}
        self.unresolved = []
        self.not_run = []
        self.tests = []

    def input(self, name, relative, kind):
        """The input name for casatestdata path `relative`."""
        name = name.rstrip("/")
        existing = self.inputs.get(name)
        if existing is not None and existing != relative:
            name = relative
        self.inputs[name] = relative
        self.kinds[name] = kind
        return name

    def classes(self):
        bases = {}
        for node in self.tree.body:
            if isinstance(node, ast.ClassDef):
                bases[node.name] = node
        return bases

    def setup_state(self, cls_node):
        """`self` attributes and files a class's setUp leaves."""
        attrs, files = dict(BASE_SELF), {}
        for node in cls_node.body:
            if isinstance(node, ast.FunctionDef) and node.name == "setUp":
                run = TestRun(self, cls_node.name, "setUp", attrs, {})
                run.run(node.body, {})
                attrs, files = run.self, run.made
                for made in files.values():
                    made["step"] = f"setUp: {made['step']}"
        return attrs, files

    def skip(self, method, scope_run):
        for decorator in method.decorator_list:
            text = short(decorator, 200)
            if not isinstance(decorator, ast.Call):
                continue
            name = short(decorator.func)
            reason = scope_run.ev(decorator.args[-1], {}) if decorator.args else ""
            if name == "unittest.skip":
                return f"unittest.skip: {reason}"
            if name in ("unittest.skipIf", "unittest.skipUnless") and decorator.args:
                condition = scope_run.ev(decorator.args[0], {})
                if isinstance(condition, Unresolved):
                    return f"skipped when {short(decorator.args[0])} ({condition.reason}): {reason}"
                if bool(condition) == (name == "unittest.skipIf"):
                    return f"{text}"
        return None

    def run(self):
        for cls_node in self.classes().values():
            for node in cls_node.body:
                if isinstance(node, ast.FunctionDef) and node.name == "run_cubetclean":
                    self.helpers[(cls_node.name, node.name)] = node
        for cls_node in self.classes().values():
            if cls_node.name == "testref_base":
                continue
            attrs, files = self.setup_state(cls_node)
            for method in cls_node.body:
                if isinstance(method, ast.FunctionDef) and method.name.startswith("test"):
                    self.test(cls_node.name, method, attrs, files)

    def test(self, cls, method, attrs, files):
        run = TestRun(self, cls, method.name, attrs, {k: dict(v) for k, v in files.items()})
        run.run(method.body, {})
        doc = " ".join((ast.get_docstring(method) or "").split("\n\n")[0].split())
        doc = doc if len(doc) <= 200 else doc[:197] + "..."
        skip = self.skip(method, run)
        self.tests.append(
            {"class": cls, "test": method.name, "calls": len(run.calls), "casa_skip": skip}
        )
        for entry in run.not_run:
            self.not_run.append({"test": f"{cls}.{method.name}", **entry})
        # A call is unresolved when it has a reason or depends on such a call;
        # the resolved calls before it in its chain still make a case.
        bad = set()
        for call in run.calls:
            if call.reasons or call.deps & bad:
                bad.add(call.number)
        good = [call for call in run.calls if call.number not in bad]
        unknown = [call for call in run.calls if call.number in bad]
        groups = self.groups(good) + self.groups(unknown, within=bad)
        cases = {}
        for members in sorted(groups, key=lambda members: members[0].number):
            name = self.emit(cls, method.name, doc, skip, members, cases)
            cases.update({call.number: name for call in members})

    @staticmethod
    def groups(calls, within=None):
        """`calls` joined by their dependencies (among `within`, if given)."""
        parent = {call.number: call.number for call in calls}

        def find(number):
            while parent[number] != number:
                parent[number] = parent[parent[number]]
                number = parent[number]
            return number

        for call in calls:
            for dep in call.deps:
                if within is None or dep in within:
                    parent[find(call.number)] = find(dep)
        grouped = collections.defaultdict(list)
        for call in calls:
            grouped[find(call.number)].append(call)
        return sorted(grouped.values(), key=lambda members: members[0].number)

    def step(self, call):
        params = {
            key: plain(value)
            for key, value in call.kwargs.items()
            if key not in ("vis", "imagename")
        }
        vis = call.kwargs.get("vis")
        return {
            "call": call.number,
            "depends_on": sorted(call.deps),
            "line": call.line,
            "vis": plain(vis),
            "imagename": call.kwargs.get("imagename"),
            "params": params,
        }

    def emit(self, cls, test, doc, skip, members, cases):
        """Record `members` as a case or as unresolved; return its name."""
        numbers = "+".join(str(call.number) for call in members)
        name = f"{cls}.{test}.{numbers}"
        numbered = {call.number for call in members}
        reasons = [
            {"call": call.number, "line": call.line, "reason": reason}
            for call in members
            for reason in call.reasons
        ]
        after = sorted(
            {
                cases[dep]
                for call in members
                for dep in call.deps
                if dep not in numbered and dep in cases
            }
        )
        reasons += [
            {
                "call": call.number,
                "line": call.line,
                "reason": f"depends on an unresolved call: call {dep}",
            }
            for call in members
            for dep in sorted(call.deps)
            if dep not in numbered and dep not in cases
        ]
        flags = {}
        for call in members:
            for key, value in call.flags.items():
                if key == "written_files":
                    flags.setdefault(key, {}).update(value)
                elif key not in flags:
                    flags[key] = value
        stage = sorted({item for call in members for item in call.stage})
        notes = sorted({note for call in members for note in call.notes})
        lines = ", ".join(str(call.line) for call in members)
        note = f"CASA {CASA_VERSION} {cls}.{test}, line {lines}" + (f": {doc}" if doc else "")
        if len(members) > 1:
            note += " (calls replayed in order: each depends on an earlier one)"
        if notes:
            note += "; " + "; ".join(notes)
        if reasons:
            entry = {
                "name": name,
                "test": f"{cls}.{test}",
                "lines": [c.line for c in members],
                "reasons": reasons,
                "calls": [self.partial(call) for call in members],
            }
            if skip:
                entry["casa_skip"] = skip
            if "written_files" in flags:
                entry["written_files"] = flags["written_files"]
            if after:
                entry["after_cases"] = after
            self.unresolved.append(entry)
            return name
        case = {}
        if len(members) == 1:
            step = self.step(members[0])
            case.update(
                vis=step["vis"],
                imagename=step["imagename"],
                params=step["params"],
                line=step["line"],
            )
        else:
            case["sequence"] = [self.step(call) for call in members]
        case["note"] = note
        case["skip_scalars"] = False
        if stage:
            case["stage"] = stage
        for key in ("mutates_vis", "expect_error", "tolerates_error", "casa_environment"):
            if key in flags:
                case[key] = flags[key]
        if skip:
            case["casa_skip"] = skip
        self.cases[name] = case
        return name

    @staticmethod
    def partial(call):
        """The call's arguments, with unknown values shown by reason."""

        def show(value):
            if isinstance(value, Unresolved):
                return {"unresolved": value.reason}
            if isinstance(value, dict):
                return {str(key): show(item) for key, item in value.items()}
            if isinstance(value, (list, tuple)):
                return [show(item) for item in value]
            return value

        return {"number": call.number, "line": call.line, "kwargs": show(call.kwargs)}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def summary(extractor):
    """Counts by class, resolution, inputs and parameter values."""
    by_class = collections.defaultdict(lambda: collections.Counter())
    for test in extractor.tests:
        counts = by_class[test["class"]]
        counts["tests"] += 1
        counts["tests_with_tclean"] += bool(test["calls"])
        counts["tclean_calls"] += test["calls"]
        counts["tests_casa_skips"] += bool(test["casa_skip"])
    resolved_calls = collections.Counter()
    for name, case in extractor.cases.items():
        cls = name.split(".")[0]
        steps = case.get("sequence", [case])
        resolved_calls[cls] += len(steps)
        by_class[cls]["resolved_cases"] += 1
        by_class[cls]["resolved_sequences"] += "sequence" in case
    for entry in extractor.unresolved:
        cls = entry["test"].split(".")[0]
        by_class[cls]["unresolved_cases"] += 1
        by_class[cls]["unresolved_calls"] += len(entry["calls"])
    for cls, count in resolved_calls.items():
        by_class[cls]["resolved_calls"] = count
    reasons = collections.Counter()
    examples = collections.defaultdict(list)
    for entry in extractor.unresolved:
        for kind in sorted({reason_kind(item["reason"]) for item in entry["reasons"]}):
            reasons[kind] += 1
            if len(examples[kind]) < 5:
                examples[kind].append(entry["name"])
    parameters = collections.defaultdict(collections.Counter)
    for case in extractor.cases.values():
        for step in case.get("sequence", [case]):
            for key, value in step["params"].items():
                parameters[key][json.dumps(value, sort_keys=True)] += 1
    total_calls = sum(test["calls"] for test in extractor.tests)
    resolved = sum(resolved_calls.values())
    used_inputs = collections.Counter()
    for case in extractor.cases.values():
        for step in case.get("sequence", [case]):
            for ms in step["vis"] if isinstance(step["vis"], list) else [step["vis"]]:
                used_inputs[ms] += 1
        for item in case.get("stage", []):
            used_inputs[item] += 1
    return {
        "source": source(extractor),
        "totals": {
            "test_methods": len(extractor.tests),
            "test_methods_with_tclean": sum(bool(t["calls"]) for t in extractor.tests),
            "test_methods_casa_skips": sum(bool(t["casa_skip"]) for t in extractor.tests),
            "tclean_call_sites": sum(
                1
                for node in ast.walk(extractor.tree)
                if isinstance(node, ast.Call) and short(node.func) == "tclean"
            ),
            "tclean_calls": total_calls,
            "tclean_calls_not_run_serially": len(extractor.not_run),
            "resolved_calls": resolved,
            "unresolved_calls": total_calls - resolved,
            "resolved_cases": len(extractor.cases),
            "resolved_single_call_cases": sum(
                "sequence" not in c for c in extractor.cases.values()
            ),
            "resolved_sequence_cases": sum("sequence" in c for c in extractor.cases.values()),
            "resolved_cases_casa_skips": sum("casa_skip" in c for c in extractor.cases.values()),
            "resolved_cases_expecting_errors": sum(
                "expect_error" in c for c in extractor.cases.values()
            ),
            "unresolved_cases": len(extractor.unresolved),
        },
        "by_class": {cls: dict(sorted(counts.items())) for cls, counts in by_class.items()},
        "unresolved_by_reason": [
            {"reason": kind, "cases": count, "examples": examples[kind]}
            for kind, count in reasons.most_common()
        ],
        "inputs": {
            name: {
                "path": extractor.inputs[name],
                "kind": extractor.kinds[name],
                "resolved_uses": used_inputs[name],
            }
            for name in sorted(extractor.inputs)
        },
        "not_run_serially": extractor.not_run,
        "parameters": {
            key: {
                "calls": sum(values.values()),
                "values": dict(values.most_common(25)),
                "other_distinct_values": max(0, len(values) - 25),
            }
            for key, values in sorted(parameters.items(), key=lambda item: -sum(item[1].values()))
        },
    }


def reason_kind(reason):
    """A short class for an unresolved reason, for counting."""
    rules = [
        ("a file the test writes", "names a file the test writes (outlier file, region file)"),
        ("a file a non-tclean step makes", "names a file a non-tclean step makes"),
        ("in a loop", "inside a loop"),
        ("under the condition", "under a condition unknown statically"),
        ("except handler", "inside an except handler"),
        ("was changed by", "MeasurementSet changed by a non-tclean step first"),
        ("part of it removed", "restart after a non-tclean step removed products"),
        ("changed by", "restart after a non-tclean step changed products"),
        ("was made by", "imagename holds products a non-tclean step made"),
        ("quanta", "value built by casatools quanta/measures"),
        ("_qa.", "value built by casatools quanta/measures"),
        ("_me.", "value built by casatools measures"),
        ("follows a step whose outputs are unknown", "follows a call whose outputs are unknown"),
        ("neither test data", "names a file of unknown origin"),
        ("bound by with open(", "value read from a file at run time"),
        ("GPU", "GPU availability"),
    ]
    for needle, kind in rules:
        if needle in reason:
            return kind
    return "other: " + reason[:100]


def source(extractor):
    return {
        "casa_version": CASA_VERSION,
        "test_file": "test_task_tclean.py of CASA 6.7.6.14 (read, not run)",
        "test_file_sha256": sha256(extractor.tests_path),
        "task_file_sha256": sha256(extractor.task_path),
        "extractor": "tools/casa-compat/extract_tclean_test_cases.py",
        "assumptions": [
            "serial run: self.parallel and ParallelTaskHelper.isMPIEnabled() are False",
            "sys.platform is 'linux'; GPU availability is unknown",
            "inputs are casatestdata paths; tests copy them into the working directory",
        ],
    }


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--tests", type=Path, required=True, help="CASA's test_task_tclean.py")
    parser.add_argument("--task", type=Path, required=True, help="CASA's task_tclean.py")
    parser.add_argument("--output", type=Path, required=True, help="case-matrix JSON")
    parser.add_argument("--summary", type=Path, required=True, help="summary JSON")
    args = parser.parse_args()
    extractor = Extractor(args.tests, args.task)
    extractor.run()
    matrix = {
        "source": source(extractor),
        "format": __doc__.split("\n\n", 2)[2].split("    python3")[0].strip(),
        "inputs": dict(sorted(extractor.inputs.items())),
        "common": {},
        "cases": extractor.cases,
        "unresolved": extractor.unresolved,
    }
    args.output.write_text(json.dumps(matrix, indent=1) + "\n")
    report = summary(extractor)
    args.summary.write_text(json.dumps(report, indent=1) + "\n")
    print(json.dumps(report["totals"], indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
