"""Run a catalog task by starting its executable directly (ADR-0017).

Python resolves the task's parameters through the Rust parameter runtime,
finds the task's executable through the Rust resolver, and starts it with
``--json-run -``: the CASA-named values set are written to its stdin, its
log lines are passed to ``sys.stderr`` as they arrive, and its typed reply on
stdout says how it ended. A Python task call neither loads nor saves Last.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
import errno
import functools
from http import HTTPStatus
import inspect
from json import JSONDecodeError, dumps, loads
from math import isfinite, isnan
from numbers import Real
import os
from os import PathLike
from pathlib import Path
import signal
import subprocess
import sys
import threading
import tempfile
from typing import Any, TypeAlias
from urllib.error import HTTPError, URLError

from ..parameters import (
    ParameterData,
    ProviderInvocation,
    SurfaceParameters,
    TaskParameters,
    _native,
    contract_bundle,
)
from ._completion import completion_projection

StrPath: TypeAlias = str | PathLike[str]

# How long a task has to stop after Python forwards it an interrupt.
_INTERRUPT_GRACE_SECONDS = 10.0


class TaskBinaryNotFoundError(FileNotFoundError):
    """Raised when a task's executable cannot be found.

    The message names the step of the search that applied and what it
    lacked: the task's ``CASARS_<APP>_BIN`` override, the suite at
    ``CASARS_SUITE_ROOT`` or the installed suite, or a development
    checkout's build.
    """


class TaskInvocationError(RuntimeError):
    """Raised when a task's executable was found but could not be started."""


class TaskResultError(RuntimeError):
    """Raised when Rust rejects a successful task's result."""


class TaskExecutionError(RuntimeError):
    """Raised when a task starts and does not succeed; ``completion`` says
    how it ended."""

    def __init__(self, completion: TaskCompletion) -> None:
        self.completion = completion
        super().__init__(completion.describe())


class TaskParameterError(ValueError):
    """Raised when a task refuses its parameters before it changes anything;
    ``completion`` says why (#931).

    A value the parameter contract refuses raises ``ValueError``, or
    ``TypeError`` for a value of the wrong type, before the task starts; this
    is the refusal of a value only the task itself can judge, such as an
    input that does not exist. CASA raises ``AssertionError`` from its
    generated parameter checker in both cases.
    """

    def __init__(self, completion: TaskCompletion) -> None:
        self.completion = completion
        message = completion.message or ""
        # The native front end marks the narrow CASA-compatible missing-path
        # case with its exact wording; retain it for scripts that match CASA.
        prefix = f"the {completion.parameter} parameter must be a path that exists ('"
        if completion.parameter and message.startswith(prefix) and message.endswith("' does not exist)"):
            super().__init__(message)
        else:
            super().__init__(completion.describe())


@dataclass(frozen=True, slots=True)
class TaskCompletion:
    """One finished task process: how it ended and what it wrote.

    ``status`` is the task's outcome: ``succeeded``, ``refused`` (its
    parameters, before it changed anything), ``failed``, ``interrupted``,
    ``signalled``, or ``malformed_reply``. ``stdout`` is the task's reply and
    ``stderr`` its log, which was also written to ``sys.stderr`` as the task
    ran.
    """

    task: str
    command: tuple[str, ...]
    workspace: Path
    returncode: int
    stdout: str
    stderr: str
    provider_invocation: ProviderInvocation
    status: str
    message: str | None
    parameter: str | None
    value: Any
    result: object | None

    @property
    def successful(self) -> bool:
        """Whether the task succeeded."""

        return self.status == "succeeded"

    @property
    def products(self) -> tuple[object, ...]:
        """Typed contract-declared products from the Rust completion decoder."""

        if self.result is None:
            return ()
        return tuple(self.result.products)  # type: ignore[attr-defined]

    def describe(self) -> str:
        """One line saying how the task ended."""

        at = f" (parameter {self.parameter!r})" if self.parameter else ""
        if self.status == "succeeded":
            return f"{self.task} succeeded"
        if self.status == "refused":
            return f"{self.task} refused its parameters{at}: {self.message}"
        if self.status == "failed":
            return f"{self.task} failed{at}: {self.message}"
        if self.status == "interrupted":
            return f"{self.task} was interrupted: {self.message}"
        detail = self.stderr.strip().splitlines()[-1:] or ["no log output"]
        if self.status == "signalled":
            return f"{self.task} was ended by signal {-self.returncode}: {detail[0]}"
        return f"{self.task} gave no valid reply ({self.message}): {detail[0]}"


def run(
    task: str,
    parameters: SurfaceParameters | None = None,
    /,
    *,
    workspace: StrPath | None = None,
    **values: ParameterData,
) -> TaskCompletion:
    """Run any catalog task, with CASA-named ``values``.

    ``parameters`` is a draft to start from, such as a loaded profile; the
    contract's defaults otherwise. The task runs in ``workspace``: the
    draft's, or the current directory. Values are normalized and checked by
    Rust before the task starts, so a value the contract refuses raises
    ``ValueError`` and starts nothing.

    The call loads and saves no Last profile: those record command-line runs.
    :meth:`TaskParameters.last` and :meth:`TaskParameters.write_last` stay
    explicit. CASA's own parameters, such as ``overwrite``, are the consent
    to replace or change data. A task that refuses its parameters raises
    :class:`TaskParameterError`, a ``ValueError``, and one that starts and
    does not succeed :class:`TaskExecutionError`, a ``RuntimeError``; an
    interrupt (Ctrl-C) is passed to the task, which is given time to stop,
    and then raised here.
    """

    return _run_task(task, parameters, workspace, values)


def _run_task(
    task: str,
    parameters: SurfaceParameters | None,
    workspace: StrPath | None,
    values: dict[str, ParameterData],
    *,
    value_only: bool = False,
) -> TaskCompletion:
    resolved_workspace = _resolve_workspace(workspace, parameters)
    if parameters is not None:
        if parameters.surface != task:
            raise ValueError(
                f"parameters target {parameters.surface!r}, not requested task {task!r}"
            )
        resolved = parameters.copy()
    else:
        resolved = TaskParameters.defaults(task, workspace=resolved_workspace)
    if values:
        if task in ("split", "mstransform"):
            if "correlation" in values:
                try:
                    values["correlation"] = _ms_transform_string_or_vector("correlation", values["correlation"])
                except AssertionError as error:
                    raise ValueError(str(error)) from error
            for selector in ("array", "uvrange", "feed"):
                if selector in values and not isinstance(values[selector], str):
                    try:
                        _ms_transform_selector_variant(selector, values[selector])
                    except AssertionError as error:
                        raise ValueError(str(error)) from error
        resolved.set_many(values)
    return _execute(task, resolved, resolved_workspace, value_only=value_only)


def _run(task: str, values: Mapping[str, ParameterData]) -> TaskCompletion:
    """Run ``task`` in the current directory: the keyword wrappers' call."""

    return run(task, **values)


def _call_casa(wrapper: Callable[..., Any], arguments: Mapping[str, Any]) -> Any:
    """Run the task of a CASA-signature ``wrapper`` with the ``arguments`` it
    was called with, in the current directory, and return CASA's value.

    An argument left at its default, or given a scalar equal to it, is not
    sent: the task resolves it to the same default itself, so only the
    values the caller changed are explicit. A default is never mutated, so
    every call sees CASA's defaults afresh.
    """

    defaults = _signature_defaults(wrapper)
    values = {
        name: value
        for name, value in arguments.items()
        if not _is_default(value, defaults.get(name, _MISSING))
    }
    name = wrapper.__name__
    if name in ("applycal", "clearcal", "delmod", "ft", "initweights", "statwt", "uvmodelfit", "uvsub"):
        _validate_required_path(values, "vis", arguments["vis"])
    if name in ("flagdata", "flagmanager"):
        vis = arguments["vis"]
        if vis is not None and not isinstance(vis, (list, tuple)):
            _validate_required_path(values, "vis", vis, search_data=True)
    if name == "plotms":
        return _call_plotms(values, arguments)
    if name == "feather":
        for parameter in ("highres", "lowres"):
            if not arguments[parameter]:
                raise AssertionError(f"{parameter} must be specified")
    if name == "importfits":
        try:
            return run(name, **values).value
        except TaskParameterError as error:
            if error.completion.parameter in ("beam", "defaultaxesvalues"):
                raise TypeError(error.completion.message) from error
            raise
    if name == "importvla":
        if not arguments["vis"]:
            raise RuntimeError("Need valid visibility file name (bad name or already exists)")
        try:
            return run(name, **values).value
        except TaskParameterError as error:
            if error.completion.parameter == "archivefiles":
                raise AssertionError(error.completion.message) from error
            if error.completion.parameter in ("vis", "project", "stoptime"):
                raise RuntimeError(error.completion.message) from error
            raise
    if name == "listcal":
        return _call_listcal(values, arguments)
    if name == "setjy" and arguments["listmodels"]:
        completion = run(name, **values)
        for candidate in loads(completion.stdout)["result"]["models"]:
            print(candidate)
        return completion.value
    if name == "calstat":
        _validate_required_path(values, "caltable", arguments["caltable"], search_data=True)
        for parameter in ("axis", "datacolumn"):
            value = arguments[parameter]
            if not isinstance(value, str):
                message = "null value not allowed" if value is None else "must be of cStr type"
                raise AssertionError(str({parameter: [message]}))
        try:
            return run(name, **values).value
        except (TypeError, ValueError) as error:
            raise AssertionError(str(error)) from error
    if name == "applycal":
        return _call_applycal(values)
    if name == "polfromgain":
        return _call_polfromgain(values, arguments)
    if name == "pccor":
        return _call_pccor(values, arguments)
    if name == "appendantab":
        return _call_appendantab(values, arguments)
    if name == "blcal":
        return _call_blcal(values, arguments)
    if name == "polcal":
        _validate_required_path(values, "vis", arguments["vis"], search_data=True)
        try:
            return run(name, **values).value
        except TaskParameterError as error:
            if error.completion.parameter == "solint":
                raise RuntimeError("Error in Calibrater::setsolve.") from error
            raise
    if name == "gencal":
        if arguments["caltype"] == "jyperk":
            _validate_required_path(values, "vis", arguments["vis"], search_data=True)
        return _call_gencal(values)
    if name == "getjyperkalma":
        _validate_required_path(values, "vis", arguments["vis"], search_data=True)
        return _call_jyperk(name, values)
    if name == "getantposalma":
        return _call_getantposalma(values)
    if name == "sdcal":
        _validate_required_path(values, "infile", arguments["infile"], search_data=True)
        if isinstance(arguments["spwmap"], dict):
            # CASA edits the current mapping in dictionary insertion order.
            values["spwmap"] = {
                "entries": [[str(source), targets]
                            for source, targets in arguments["spwmap"].items()]
            }
        try:
            return run(name, **values).value
        except TaskExecutionError as error:
            message = error.completion.message or ""
            if message in (
                "Applytable name must be specified.",
                "Spwmap type must be list or dictionary.",
            ) or message.startswith("Table doesn't exist: "):
                raise Exception(message) from error
            if (message == "Error in Calibrater::setapply."
                or message.startswith("overwrite is False and output file exists:")
                or (error.completion.parameter == "fraction" and message.startswith("Invalid fraction value ("))
                or (message.startswith("Applytable '") and message.endswith("' is not a caltable format"))):
                raise RuntimeError(message) from error
            raise
        except TaskParameterError as error:
            if (error.completion.parameter == "fraction"
                and error.completion.message == "Too many edge points. fraction must be < 0.5."):
                raise ValueError(error.completion.message) from error
            raise
    if name == "sdgaincal":
        mode = arguments["calmode"]
        if mode != "doublecircle":
            message = "null value not allowed" if mode is None else f"unallowed value {mode}"
            raise AssertionError(str({"calmode": [message]}))
        _validate_required_path(values, "infile", arguments["infile"], search_data=True)
        if not arguments["outfile"]:
            raise ValueError("outfile is empty.")
        try:
            return run(name, **values).value
        except TaskExecutionError as error:
            message = error.completion.message or ""
            if (message.startswith("Error in Calibrater::") or message.endswith(" exists.")
                    or message.startswith("Double-circle gain needs")
                    or message.startswith("Table ") and message.endswith(" does not exist.")):
                raise RuntimeError(message) from error
            raise
        except TaskParameterError as error:
            if error.completion.parameter in ("interp", "spwmap"):
                raise AssertionError() from error
            raise
    if name == "sdsmooth":
        _validate_required_path(values, "infile", arguments["infile"], search_data=True)
        for parameter, allowed in (
            ("datacolumn", ("data", "DATA", "float_data", "FLOAT_DATA", "corrected", "CORRECTED")),
            ("kernel", ("gaussian", "boxcar")),
        ):
            value = arguments[parameter]
            if value not in allowed:
                message = "null value not allowed" if value is None else f"unallowed value {value}"
                raise AssertionError(str({parameter: [message]}))
        if not arguments["outfile"]:
            raise ValueError("outfile is empty.")
        try:
            return run(name, **values).value
        except TaskExecutionError as error:
            message = error.completion.message or ""
            if message.endswith(" exists."):
                raise ValueError(message) from error
            raise RuntimeError(message) from error
    if name == "sdatmcor":
        return _call_sdatmcor(values)
    if name == "sdfit":
        return _call_sdfit(values, arguments)
    if name == "sdbaseline":
        _validate_required_path(values, "infile", arguments["infile"], search_data=True)
        choices = {
            "datacolumn": ("data", "DATA", "float_data", "FLOAT_DATA", "corrected", "CORRECTED"),
            "maskmode": ("list", "LIST", "auto", "AUTO"),
            "blfunc": ("poly", "chebyshev", "cspline", "sinusoid", "variable",
                       "POLY", "CHEBYSHEV", "CSPLINE", "SINUSOID", "VARIABLE"),
            "fftmethod": ("fft", "FFT"),
            "sigmavalue": ("stddev", "rms"),
        }
        for parameter, allowed in choices.items():
            value = arguments[parameter]
            if value not in allowed:
                message = "null value not allowed" if value is None else f"unallowed value {value}"
                raise AssertionError(str({parameter: [message]}))
        if arguments["blfunc"].lower() == "sinusoid" and str(arguments["blmode"]).lower() == "fit":
            import numpy as np

            for parameter in ("addwn", "rejwn"):
                value = arguments[parameter]
                if isinstance(value, tuple):
                    raise AssertionError(str({parameter: ["must be of cVariant type"]}))
                if isinstance(value, np.ndarray):
                    raise ValueError("wrong value given for addwn/rejwn")
        try:
            return run(name, **values).value
        except TaskParameterError as error:
            if error.completion.message == "Cannot sort a wavenumber list containing strings and numbers.":
                raise TypeError(error.completion.message) from error
            raise
        except TaskExecutionError as error:
            message = error.completion.message or ""
            if message.endswith("is not yet implemented (#1532)"):
                raise NotImplementedError(message) from error
            if (error.completion.parameter in ("outfile", "bltable", "blformat", "bloutput", "blparam")
                    and message):
                raise ValueError(message) from error
            raise RuntimeError(message) from error
    if name == "initweights":
        return _call_initweights(values)
    if name == "plotbandpass":
        return _call_plotbandpass(values, arguments)
    if name == "predictcomp":
        return _call_predictcomp(values, arguments)
    if name == "plotweather":
        return _call_plotweather(values, arguments)
    if name in ("split", "mstransform"):
        _validate_ms_transform_datacolumn(arguments["datacolumn"])
        _coerce_ms_task_options(name, arguments, values)
        _coerce_ms_transform_selectors(name, values)
        _coerce_ms_channel_width(name, values)
        try:
            return run(name, **values).value
        except TaskExecutionError as error:
            if name == "split" and error.completion.message == "Unable to continue with MMS processing":
                raise Exception(error.completion.message) from error
            raise
        except TaskParameterError as error:
            message = error.completion.message or ""
            if (name == "mstransform" and error.completion.parameter == "callib"
                    and message == "No such file or directory"):
                raise FileNotFoundError(2, message, arguments["callib"]) from error
            if (name == "mstransform" and error.completion.parameter == "callib"
                    and message.startswith("Problem parsing cal library line ")):
                raise Exception(message) from error
            if name == "mstransform" and error.completion.parameter == "separationaxis":
                if message.startswith("unallowed value "):
                    raise AssertionError(str({"separationaxis": [message]})) from error
                if message.startswith("Cannot create MMS using separationaxis="):
                    raise Exception(message) from error
            if name == "mstransform" and error.completion.parameter == "numsubms":
                if message == "float division by zero":
                    raise ZeroDivisionError(message) from error
                if message.startswith("'<' not supported"):
                    raise TypeError(message) from error
            if error.completion.parameter == "outputvis":
                if message.startswith("Output MS "):
                    raise OSError(message) from error
                if message.startswith("input and output MeasurementSet paths must differ: "):
                    path = arguments["outputvis"]
                    raise OSError(f"Output MS {path} already exists - will not overwrite it.") from error
            if error.completion.parameter == "spw" and message.startswith("invalid spectral-window selection "):
                raise RuntimeError(message) from error
            if error.completion.parameter == "datacolumn" and message.startswith("unsupported datacolumn "):
                invalid = arguments["datacolumn"]
                raise AssertionError(str({"datacolumn": [f"unallowed value {invalid}"]})) from error
            if name == "mstransform" and arguments["regridms"]:
                if message.startswith("unallowed value "):
                    raise AssertionError(str({error.completion.parameter: [message]})) from error
                if error.completion.parameter in ("start", "width") and message.endswith(" units"):
                    raise TypeError(message) from error
            if error.completion.parameter == "width" and message == "list index out of range":
                raise IndexError(message) from error
            raise
    if name in ("clearcal", "delmod", "ft", "uvmodelfit", "uvsub"):
        from ._model_arguments import normalize

        normalize(name, values, arguments)
        try:
            return run(name, **values).value
        except TaskParameterError as error:
            if name == "ft":
                raise ValueError(error.completion.message) from error
            raise
        except TaskExecutionError as error:
            if error.completion.parameter in ("addmodel", "otf", "scr", "incremental"):
                raise TypeError(error.completion.message) from error
            if name == "ft" and error.completion.parameter == "reffreq":
                raise RuntimeError(("*** Error *** In conversion of reffreq='", arguments["reffreq"],
                                    "' to a numerical value")) from error
            if name == "ft" and error.completion.parameter == "model":
                raise RuntimeError(error.completion.message) from error
            if name == "uvmodelfit":
                raise RuntimeError(error.completion.message) from error
            raise
    if name in ("smoothcal", "rerefant"):
        for path in ("vis", "tablein"):
            _validate_required_path(values, path, arguments[path])
        if name == "smoothcal":
            width = arguments["smoothtime"]
            if isinstance(width, bool):
                values["smoothtime"] = float(width)
            elif isinstance(width, Real) and width == float("inf"):
                # The parameter profile carries finite values. This interval
                # still covers the whole group for MeasurementSet epochs.
                values["smoothtime"] = sys.float_info.max
        try:
            return run(name, **values).value
        except (TypeError, ValueError) as error:
            raise AssertionError(str(error)) from error
    if name == "flagdata":
        argument_text: dict[str, str] = {}
        correlation_vector, column_vector = _coerce_flagdata_options(values, argument_text)
        if values.get("savepars"):
            argument_text.update(_flagdata_saved_argument_text(arguments))
        try:
            if argument_text:
                workspace = Path.cwd()
                resolved = TaskParameters.defaults(name, workspace=workspace)
                resolved.set_many(values)
                return _execute_task(name, resolved, workspace, None, argument_text=argument_text).value
            return run(name, **values).value
        except TaskParameterError as error:
            completion = error.completion
            if (completion.parameter == "datacolumn" and completion.message in (
                    "Failed to parse parameters for mode clip",
                    "Failed to parse parameters for mode tfcrop",
                    "Failed to parse parameters for mode rflag",
            )):
                raise ValueError(completion.message) from error
            if completion.parameter in ("vis", "outfile"):
                raise ValueError(completion.message) from error
            raise
        except TaskExecutionError as error:
            completion = error.completion
            if completion.parameter == "correlation":
                if correlation_vector:
                    raise AttributeError(f"'{correlation_vector}' object has no attribute 'upper'") from error
                if completion.message == "no flagdata summary report for the selected correlations":
                    raise KeyError("nreport") from error
                if (completion.message or "").startswith("Unknown polarization requested,"):
                    raise RuntimeError(completion.message) from error
            if completion.parameter == "calibration-selection":
                raise RuntimeError(completion.message) from error
            if completion.parameter == "calibration-agent":
                raise ValueError(completion.message) from error
            if completion.parameter == "calibration-save-command":
                raise Exception(completion.message) from error
            if completion.parameter == "flag-list-input":
                raise ValueError(completion.message) from error
            if completion.parameter == "flag-list-no-report":
                raise KeyError("nreport") from error
            if completion.parameter == "flag-list-agent":
                raise RuntimeError(completion.message) from error
            if completion.parameter == "flag-list-threshold-type":
                raise TypeError(completion.message) from error
            if completion.parameter == "flag-list-clip-range":
                raise IndexError(completion.message) from error
            if completion.parameter == "rflag-threshold-file":
                raise UnboundLocalError(completion.message) from error
            if completion.parameter == "rflag-threshold-path.not-found":
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), completion.message) from error
            if completion.parameter == "rflag-threshold-path.is-directory":
                raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), completion.message) from error
            if completion.parameter == "datacolumn" and column_vector:
                raise AttributeError(f"'{column_vector}' object has no attribute 'upper'") from error
            if (completion.parameter in ("maxnpieces", "halfwin", "winsize")
                    and completion.message == ("<built-in function agentflagger_parseagentparameters>"
                                               " returned a result with an exception set")):
                raise SystemError(completion.message) from error
            if completion.message == f"argument _{completion.parameter} must be a string":
                raise TypeError(completion.message) from error
            if completion.parameter == "ntime":
                if completion.message == "Parameter ntime cannot be < = 0":
                    raise ValueError(completion.message) from error
                raise RuntimeError(completion.message) from error
            if completion.parameter == "addantenna":
                if (completion.message or "").startswith("could not convert string to float:"):
                    raise ValueError(completion.message) from error
                if completion.message == "list index out of range":
                    raise IndexError(completion.message) from error
                raise Exception(completion.message) from error
            if completion.parameter in ("channelavg", "chanbin", "timeavg", "outfile"):
                raise RuntimeError(completion.message) from error
            if completion.parameter == "outfile.not_found":
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), arguments["outfile"]) from error
            raise
    if name == "flagmanager":
        errors = _flagging_choice_errors(name, values)
        for parameter in ("versionname", "oldname", "comment", "merge"):
            if parameter not in values:
                continue
            value = values[parameter]
            if value is None:
                errors[parameter] = ["null value not allowed"]
            elif not isinstance(value, str):
                errors[parameter] = ["must be of cStr type"]
        if errors:
            raise AssertionError(str(dict(sorted(errors.items()))))
        try:
            return run(name, **values).value
        except TaskParameterError as error:
            if error.completion.parameter in ("versionname", "oldname"):
                raise ValueError(error.completion.message) from error
            raise
        except TaskExecutionError as error:
            if error.completion.parameter == "versionname":
                raise RuntimeError(error.completion.message) from error
            raise
    return run(name, **values).value


def _call_plotms(values, arguments):
    from ._plotms_plot import validate

    paths = {name: arguments[name].strip() for name in ("vis", "caltable")
             if isinstance(arguments[name], str) and arguments[name].strip()}
    if len(paths) > 1:
        raise RuntimeError("Cannot set both vis and caltable parameters")
    if not paths and arguments["plotfile"]:
        raise RuntimeError("vis and caltable not set, cannot export empty plot")
    for name, path in paths.items():
        absolute = os.path.abspath(path)
        if not os.path.exists(absolute):
            raise RuntimeError(f"Input file not found:\n{absolute}")
        values[name] = absolute
    index = arguments["plotindex"]
    if isinstance(index, int) and index < 0:
        raise ValueError("a negative plotindex is not valid.")
    if isinstance(index, int) and index and arguments["clearplots"]:
        raise ValueError("a nonzero plotindex is not valid when clearing plots.")
    if arguments["correlation"] and arguments["polarization"]:
        raise RuntimeError("Cannot select both correlation and polarization.")
    if arguments["averagedata"]:
        if arguments["avgantenna"] and arguments["avgbaseline"]:
            raise ValueError("averaging over baselines is mutually exclusive with per-antenna averaging.")
        if arguments["avgchannel"] and float(arguments["avgchannel"]) < 0:
            raise ValueError("cannot average negative number of channels")
        try:
            if arguments["avgtime"] and float(arguments["avgtime"]) < 0:
                raise ValueError
        except ValueError:
            raise ValueError("avgtime value must be numerical string in seconds (no units)") from None
    validate(arguments)
    if arguments["showatm"] or arguments["showtsky"]:
        if arguments["showatm"] and arguments["showtsky"]:
            print("Warning: you have selected both showatm and showtsky.  Defaulting to showatm=True only.")
            values["showtsky"] = False
        if arguments["xaxis"] not in ("", "chan", "channel", "Channel", "freq", "frequency", "Frequency"):
            raise ValueError("xaxis must be channel or frequency for showatm and showtsky")
    elif arguments["showimage"]:
        values["showimage"] = False
    if not paths and all(isinstance(arguments[name], str) for name in ("vis", "caltable")):
        return None
    callib = arguments["callib"]
    entries = [callib] if isinstance(callib, str) else callib
    if isinstance(entries, (list, tuple)) and len(entries) == 1:
        filename = entries[0]
        if isinstance(filename, str) and "=" not in filename and not os.path.exists(filename):
            raise RuntimeError("callib file does not exist")
    for name in ("field", "spw"):
        if name in values and isinstance(values[name], (int, float)):
            values[name] = str(values[name])
    try:
        return run("plotms", **values).value
    except (TaskExecutionError, TaskParameterError) as error:
        if error.completion.message == "Remove duplicate y-axes.":
            raise RuntimeError(error.completion.message) from error
        raise


def _call_plotbandpass(values, arguments):
    from json import loads
    from ._bandpass_arguments import normalize

    caltable = arguments["caltable"]
    if caltable is None:
        raise AssertionError("{'caltable': ['null value not allowed']}")
    if not isinstance(caltable, str):
        raise AssertionError(f"the caltable parameter must be a path that exists ('{caltable}' does not exist)")
    _validate_required_path(values, "caltable", caltable, search_data=True)
    normalize(values, arguments)
    try:
        completion = run("plotbandpass", **values)
    except TaskExecutionError as error:
        message = error.completion.message
        exceptions = {
            "name 'resampleSolution' is not defined": NameError,
            "cannot access local variable 'mytime' where it is not associated with a value": UnboundLocalError,
            "expected string or bytes-like object, got 'float'": TypeError,
            "unsupported operand type(s) for //=: 'str' and 'int'": TypeError,
        }
        if message is not None and message.startswith("invalid literal for int()"):
            raise ValueError(message) from error
        if message in exceptions:
            raise exceptions[message](message) from error
        raise
    if completion.value is not None:
        return _bandpass_statistics(completion.value)
    return () if loads(completion.stdout)["result"]["empty_tuple"] else None


def _bandpass_statistics(value):
    import numpy as np

    if isinstance(value, dict):
        return {key: _bandpass_statistics(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_bandpass_statistics(item) for item in value]
    if isinstance(value, float):
        return np.float64(value)
    return value


def _flagdata_saved_argument_text(arguments: Mapping[str, Any]) -> dict[str, str]:
    """Spell the CASA values whose conversion for the flagger loses their form."""
    import numpy as np

    text = {}
    for name in ("field", "spw", "array", "feed", "scan", "antenna", "uvrange", "timerange", "intent"):
        value = arguments[name]
        if isinstance(value, (list, tuple, np.ndarray)) and _flagdata_string_vector(value):
            text[name] = (str(value) if isinstance(value, np.ndarray) and value.ndim == 0
                          else repr([str(item) for item in value]))
    for name in ("autocorr", "clipoutside", "channelavg", "timeavg", "clipzeros", "quackincrement"):
        if isinstance(arguments[name], np.bool_):
            text[name] = str(bool(arguments[name]))
    width = arguments["chanbin"]
    if isinstance(width, np.ndarray) and width.ndim == 0:
        text["chanbin"] = str(width)
    clip = arguments["clipminmax"]
    if isinstance(clip, (list, tuple, np.ndarray)) and not (isinstance(clip, np.ndarray) and clip.ndim != 1) and len(clip) == 2:
        if all(isinstance(item, (int, float, np.integer, np.floating)) for item in clip):
            if any(isinstance(item, (float, np.floating)) and not isfinite(item) for item in clip):
                text["clipminmax"] = str([float(item) for item in clip])
    interval = arguments["quackinterval"]
    if isinstance(interval, (float, np.floating)) and not isfinite(interval):
        text["quackinterval"] = str(float(interval))
    return text


@functools.cache
def _flagging_choices(surface: str) -> dict[str, frozenset[str]]:
    concepts = contract_bundle(surface)["catalog"]["concepts"]
    return {
        concept["casa_name"]: frozenset(concept["value_domain"]["values"])
        for concept in concepts
        if concept["casa_name"] in ("mode", "action") and concept["value_domain"]["kind"] == "choice"
    }


def _flagging_choice_errors(surface: str, values: dict[str, Any]) -> dict[str, list[str]]:
    errors = {}
    if "vis" in values and values["vis"] is None:
        errors["vis"] = ["null value not allowed"]
    for parameter, choices in _flagging_choices(surface).items():
        if parameter not in values:
            continue
        value = values[parameter]
        if value is None:
            errors[parameter] = ["unallowed value None", "null value not allowed"]
        elif not isinstance(value, str):
            errors[parameter] = ["must be of cStr type"]
        elif value not in choices:
            errors[parameter] = [f"unallowed value {value}"]
    return errors


def _coerce_flagdata_options(
    values: dict[str, Any], argument_text: dict[str, str],
) -> tuple[str | None, str | None]:
    import numpy as np

    cfloat = (float, int, np.float32, np.float64, np.int32, np.int64)
    cint = (int, np.int32, np.int64)
    errors = _flagging_choice_errors("flagdata", values)
    column_vector = _coerce_flagdata_column(values, argument_text, errors)
    _coerce_flagdata_list_options(values, errors, cfloat)
    for parameter in ("tolerance", "lowerlimit", "upperlimit", "minchanfrac", "growtime", "growfreq",
                      "timecutoff", "freqcutoff"):
        if parameter in values:
            value = values[parameter]
            if value is None:
                errors[parameter] = ["null value not allowed"]
            elif isinstance(value, cfloat):
                values[parameter] = float(value)
            else:
                errors[parameter] = ["must be of cFloat type"]
    if "extendpols" in values:
        value = values["extendpols"]
        if value is None:
            errors["extendpols"] = ["null value not allowed"]
        elif not isinstance(value, (str, list, dict, np.ndarray, *cfloat)):
            errors["extendpols"] = ["must be of cVariant type"]
        elif isinstance(value, (np.ndarray, np.int32, np.int64, np.float32, np.float64)):
            argument_text["extendpols"] = str(value)
    for parameter in ("timedev", "freqdev"):
        if parameter not in values:
            continue
        value = values[parameter]
        if value is None:
            errors[parameter] = ["null value not allowed"]
        elif not isinstance(value, (str, list, dict, np.ndarray, *cfloat)):
            errors[parameter] = ["must be of cVariant type"]
    for parameter, default in (("maxnpieces", 7), ("halfwin", 1), ("winsize", 3)):
        if parameter not in values:
            continue
        value = values[parameter]
        if value is None:
            errors[parameter] = ["null value not allowed"]
        elif not isinstance(value, cint):
            errors[parameter] = ["must be of cInt type"]
        elif isinstance(value, (np.int32, np.int64)):
            argument_text[parameter] = str(value)
            values[parameter] = default
        elif not isinstance(value, bool):
            value = int(value)
            # The agent reads a 32-bit record value, while commands keep the Python integer.
            if not -(2**31) <= value < 2**31:
                argument_text[parameter] = str(value)
            values[parameter] = max(-(2**63), min(value, 2**63 - 1))
    if "addantenna" in values:
        value = values["addantenna"]
        if value is None:
            errors["addantenna"] = ["null value not allowed"] * 3
        elif isinstance(value, str):
            values["addantenna"] = str(value)
        elif isinstance(value, dict):
            argument_text["addantenna"] = repr(value)
        else:
            errors["addantenna"] = ["must be of cStr type", "must be of cDict type"]
    correlation_vector = None
    for parameter in ("field", "spw", "array", "feed", "scan", "antenna", "uvrange", "timerange", "intent"):
        if parameter not in values:
            continue
        value = values[parameter]
        if isinstance(value, str):
            values[parameter] = str(value)
        elif value is None:
            errors[parameter] = ["null value not allowed"] * 3
        elif _flagdata_string_vector(value):
            values[parameter] = ([str(value.item())] if isinstance(value, np.ndarray) and value.ndim == 0
                                 else [str(item) for item in value])
        else:
            errors[parameter] = ["must be of cStr type", "must be of cStrVec type"]
    if "correlation" in values:
        value = values["correlation"]
        if isinstance(value, str):
            values["correlation"] = str(value)
        elif value is None:
            errors["correlation"] = ["null value not allowed"] * 3
        elif _flagdata_string_vector(value):
            if isinstance(value, np.ndarray) and value.ndim == 0:
                item = str(value.item())
                values["correlation"] = [item] if item else ""
                if item:
                    correlation_vector = "numpy.ndarray"
            else:
                values["correlation"] = [str(item) for item in value]
                correlation_vector = "list"
        else:
            errors["correlation"] = ["must be of cStr type", "must be of cStrVec type"]
    if "observation" in values:
        value = values["observation"]
        if value is None:
            errors["observation"] = ["null value not allowed"] * 3
        elif isinstance(value, str):
            values["observation"] = str(value)
        elif isinstance(value, (np.int32, np.int64)):
            values["observation"] = int(value)
        elif isinstance(value, int):
            if not -(2**63) <= value < 2**63:
                # Preserve the selector for native range validation even
                # when the transport cannot represent it as an integer.
                values["observation"] = str(value)
        else:
            errors["observation"] = ["must be of cStr type", "must be of cInt type"]
    for parameter, default in (("clipoutside", True), ("quackincrement", False),
                               ("channelavg", False), ("timeavg", False)):
        if parameter not in values:
            continue
        value = values[parameter]
        if value is None:
            errors[parameter] = ["null value not allowed"] * 3
        else:
            converted = _flagdata_bool_option(value, numpy_default=default)
            if converted is None:
                errors[parameter] = ["must be of cBool type", "must be of cBoolVec type"]
            else:
                values[parameter] = converted
    _coerce_flagdata_quack_options(values, errors, cfloat, cint)
    _coerce_flagdata_channel_bins(values, errors, cint)
    if "ntime" in values:
        duration = values["ntime"]
        if duration is None:
            errors["ntime"] = ["null value not allowed"] * 3
        elif isinstance(duration, cfloat):
            values["ntime"] = float(duration)
        elif not isinstance(duration, str):
            errors["ntime"] = ["must be of cFloat type", "must be of cStr type"]
    for parameter, expected, casa_type in (
        ("autocorr", bool, "cBool"),
        ("spwchan", bool, "cBool"),
        ("spwcorr", bool, "cBool"),
        ("basecnt", bool, "cBool"),
        ("fieldcnt", bool, "cBool"),
        ("name", str, "cStr"),
        ("display", str, "cStr"),
        ("cmdreason", str, "cStr"),
        ("outfile", str, "cStr"),
        ("antint_ref_antenna", str, "cStr"),
        ("timefit", str, "cStr"),
        ("freqfit", str, "cStr"),
        ("flagdimension", str, "cStr"),
        ("usewindowstats", str, "cStr"),
        ("flagbackup", bool, "cBool"),
        ("savepars", bool, "cBool"),
        ("overwrite", bool, "cBool"),
        ("timebin", str, "cStr"),
        ("minrel", cfloat, "cFloat"),
        ("maxrel", cfloat, "cFloat"),
        ("minabs", cint, "cInt"),
        ("maxabs", cint, "cInt"),
        ("clipzeros", bool, "cBool"),
        ("writeflags", bool, "cBool"),
        ("combinescans", bool, "cBool"),
        ("growaround", bool, "cBool"),
        ("flagneartime", bool, "cBool"),
        ("flagnearfreq", bool, "cBool"),
        ("verbose", bool, "cBool"),
    ):
        if parameter not in values:
            continue
        value = values[parameter]
        if value is None:
            errors[parameter] = ["null value not allowed"]
        elif expected is bool and isinstance(value, np.bool_):
            if parameter in ("combinescans", "growaround", "flagneartime", "flagnearfreq", "verbose"):
                argument_text[parameter] = str(bool(value))
            # Some CASA switches use their defaults for NumPy Booleans.
            values[parameter] = (
                bool(value) if parameter in ("autocorr", "writeflags", "flagbackup", "savepars", "overwrite")
                else False
            )
        elif not isinstance(value, expected):
            errors[parameter] = [f"must be of {casa_type} type"]
        elif casa_type == "cFloat":
            fraction = float(value)
            if np.isnan(fraction):
                # CASA's comparisons ignore a NaN bound.
                fraction = 0.0 if parameter == "minrel" else 1.0
            values[parameter] = max(-sys.float_info.max, min(fraction, sys.float_info.max))
        elif casa_type == "cInt":
            # Counts from stored MeasurementSets fit in a signed 64-bit value.
            values[parameter] = max(-(2**63), min(int(value), 2**63 - 1))
    if "clipminmax" in values:
        try:
            values["clipminmax"] = _flagdata_clip_range(
                values["clipminmax"], cfloat, outside=values.get("clipoutside") is not False,
            )
        except ValueError as error:
            errors["clipminmax"] = [str(error)]
    if errors:
        raise AssertionError(str(dict(sorted(errors.items()))))
    return correlation_vector, column_vector


def _coerce_flagdata_list_options(values, errors, numeric) -> None:
    import numpy as np

    for parameter in ("inpfile", "reason"):
        if parameter not in values:
            continue
        value = values[parameter]
        if isinstance(value, str):
            values[parameter] = str(value)
        elif value is None:
            errors[parameter] = ["null value not allowed"] * 3
        elif _flagdata_string_vector(value):
            if not isinstance(value, np.ndarray) or value.ndim != 0:
                values[parameter] = [str(item) for item in value]
        else:
            errors[parameter] = ["must be of cStr type", "must be of cStrVec type"]
    if "tbuff" not in values:
        return
    value = values["tbuff"]
    if value is None:
        errors["tbuff"] = ["null value not allowed"] * 3
    elif isinstance(value, numeric):
        values["tbuff"] = float(value)
    elif isinstance(value, np.ndarray) and value.ndim == 0:
        second = ("field 'tbuff' cannot be coerced: iteration over a 0-d array"
                  if value.dtype.type in numeric else "must be of cFloatVec type")
        errors["tbuff"] = ["must be of cFloat type", second]
    elif isinstance(value, Iterable) and not isinstance(value, str):
        items = list(value)
        if all(isinstance(item, numeric) for item in items):
            values["tbuff"] = [float(item) for item in items]
        else:
            errors["tbuff"] = ["must be of cFloat type", "must be of cFloatVec type"]
    else:
        errors["tbuff"] = ["must be of cFloat type", "must be of cFloatVec type"]


def _coerce_flagdata_column(values, argument_text, errors) -> str | None:
    import numpy as np

    if "datacolumn" not in values:
        return None
    column = values["datacolumn"]
    if isinstance(column, str):
        values["datacolumn"] = str(column)
    elif column is None:
        errors["datacolumn"] = ["null value not allowed"] * 3
    elif _flagdata_string_vector(column):
        if isinstance(column, np.ndarray) and column.ndim == 0:
            values["datacolumn"] = [str(column.item())]
            argument_text["datacolumn"] = str(column)
            return "numpy.ndarray"
        items = list(column)
        values["datacolumn"] = [str(item) for item in items]
        argument_text["datacolumn"] = repr(items)
        return "list"
    else:
        errors["datacolumn"] = ["must be of cStr type", "must be of cStrVec type"]
    return None


def _flagdata_string_vector(value: Any) -> bool:
    import numpy as np

    if isinstance(value, np.ndarray) and value.ndim == 0:
        return value.dtype.type is np.str_
    return isinstance(value, Iterable) and all(isinstance(item, str) for item in value)


def _flagdata_bool_option(value: Any, *, numpy_default: bool) -> bool | list[bool] | None:
    import numpy as np

    if isinstance(value, bool):
        return value
    if isinstance(value, np.bool_):
        return numpy_default
    if isinstance(value, np.ndarray) and value.ndim == 0:
        if value.dtype.type is not np.bool_:
            return None
        value = (value[()],)
    if isinstance(value, Iterable) and not isinstance(value, str):
        items = list(value)
        if all(isinstance(item, (bool, np.bool_)) for item in items):
            return [bool(item) for item in items]
    return None


def _coerce_flagdata_channel_bins(
    values: dict[str, Any], errors: dict[str, list[str]], integer: tuple[type, ...],
) -> None:
    import numpy as np

    if "chanbin" not in values:
        return
    value = values["chanbin"]
    if value is None:
        errors["chanbin"] = ["null value not allowed"] * 3
        return
    if isinstance(value, integer):
        return
    if isinstance(value, np.ndarray) and value.ndim == 0:
        if value.dtype.type in integer:
            return
    elif isinstance(value, Iterable) and not isinstance(value, str):
        items = list(value)
        if all(isinstance(item, integer) for item in items):
            values["chanbin"] = [item if isinstance(item, bool) else int(item) for item in items]
            return
    errors["chanbin"] = ["must be of cInt type", "must be of cIntVec type"]


def _coerce_flagdata_quack_options(
    values: dict[str, Any],
    errors: dict[str, list[str]],
    numeric: tuple[type, ...],
    integer: tuple[type, ...],
) -> None:
    import numpy as np

    if "quackmode" in values:
        mode = values["quackmode"]
        if isinstance(mode, str):
            values["quackmode"] = str(mode)
        elif mode is None:
            errors["quackmode"] = ["null value not allowed"] * 3
        else:
            if isinstance(mode, np.ndarray) and mode.ndim == 0:
                options = [mode[()]] if mode.dtype.type is np.str_ else None
            else:
                options = list(mode) if isinstance(mode, Iterable) else None
            if options is not None and all(isinstance(item, str) for item in options):
                values["quackmode"] = [str(item) for item in options]
            else:
                errors["quackmode"] = ["must be of cStr type", "must be of cStrVec type"]

    if "quackinterval" not in values:
        return
    interval = values["quackinterval"]
    if isinstance(interval, numeric):
        values["quackinterval"] = _flagdata_quack_seconds(interval)
        return
    if interval is None:
        errors["quackinterval"] = ["null value not allowed"] * 5
        return

    messages = [f"must be of {kind} type" for kind in ("cFloat", "cFloatVec", "cInt", "cIntVec")]
    if isinstance(interval, np.ndarray) and interval.ndim == 0:
        if interval.dtype.type in integer:
            interval = (interval[()],)
        else:
            if interval.dtype.type in numeric:
                messages[1] = "field 'quackinterval' cannot be coerced: iteration over a 0-d array"
            errors["quackinterval"] = messages
            return
    options = list(interval) if isinstance(interval, Iterable) and not isinstance(interval, str) else None
    if options is not None and all(isinstance(item, numeric) for item in options):
        values["quackinterval"] = [_flagdata_quack_seconds(item) for item in options]
    else:
        errors["quackinterval"] = messages


def _flagdata_quack_seconds(value: Any) -> float:
    seconds = float(value)
    # The profile carries finite numbers. NaN disables the agent and an
    # infinite positive interval covers the entire finite scan.
    if isnan(seconds):
        return 0.0
    return max(-sys.float_info.max, min(seconds, sys.float_info.max))


def _flagdata_clip_range(
    value: Any, numeric: tuple[type, ...], *, outside: bool
) -> list[float]:
    if value is None:
        raise ValueError("null value not allowed")
    if isinstance(value, numeric) and not isinstance(value, bool):
        items = [value]
    elif isinstance(value, Iterable) and not isinstance(value, str):
        try:
            items = list(value)
        except TypeError as error:
            raise ValueError(f"field 'clipminmax' cannot be coerced: {error}") from error
    else:
        raise ValueError("must be of cFloatVec type")
    if not all(isinstance(item, numeric) for item in items):
        raise ValueError("must be of cFloatVec type")
    bounds = [float(item) for item in items]
    # The parameter profile accepts finite values. The clipping agent's
    # single-precision bounds turn these finite doubles into infinities.
    limit = sys.float_info.max
    if len(bounds) == 2 and not outside and any(isnan(bound) for bound in bounds):
        return [limit, -limit]
    for index, bound in enumerate(bounds):
        if isnan(bound):
            bounds[index] = -limit if index == 0 else limit
        else:
            bounds[index] = max(-limit, min(bound, limit))
    return bounds


def _call_predictcomp(values: dict[str, Any], arguments: Mapping[str, Any]) -> Any:
    from os.path import expanduser

    import numpy as np

    flags = ("showplot", "include0amp", "include0bl", "showbl0flux")
    errors = {}
    for parameter, value in arguments.items():
        if parameter == "nfreqs":
            valid = isinstance(value, (int, np.int32, np.int64))
            expected = "cInt"
        elif parameter in flags:
            valid = isinstance(value, (bool, np.bool_))
            expected = "cBool"
        else:
            valid = isinstance(value, str)
            expected = "cPath" if parameter == "prefix" else "cStr"
        if not valid:
            errors[parameter] = ["null value not allowed" if value is None else f"must be of {expected} type"]
    if errors:
        raise AssertionError(str(errors))
    if "nfreqs" in values:
        values["nfreqs"] = int(values["nfreqs"])
    for flag in flags:
        if flag in values:
            values[flag] = bool(values[flag])
    if "prefix" in values:
        prefix = values["prefix"]
        if prefix and not prefix.startswith(("/", "./", "../")):
            values["prefix"] = expanduser(prefix)
    # Direct filenames are literal, even when they contain characters
    # reserved by reusable parameter profiles.
    literal = {
        key: values.pop(key)
        for key in ("prefix", "savefig")
        if key in values and any(character in values[key] for character in "$`")
    }
    try:
        if literal:
            workspace = Path.cwd()
            resolved = TaskParameters.defaults("predictcomp", workspace=workspace)
            resolved.set_many(values)
            result = _execute_task("predictcomp", resolved, workspace, None, argument_text=literal).value
        else:
            result = run("predictcomp", **values).value
    except TaskParameterError as error:
        if error.completion.message == "m0" and error.completion.parameter == "epoch":
            raise KeyError("m0") from error
        if error.completion.parameter in ("objname", "minfreq"):
            raise ValueError(error.completion.message) from error
        raise
    except ValueError as error:
        for parameter in ("standard", "blunit"):
            if f"for {parameter}:" in str(error):
                raise AssertionError(str({parameter: [f"unallowed value {arguments[parameter]}"]})) from error
        raise
    if result and arguments["antennalist"]:
        zero_flux = result["spectrum"]["bl0flux"]
        zero_flux["value"] = np.float64(zero_flux["value"])
    return result


def _call_plotweather(values: dict[str, Any], arguments: Mapping[str, Any]) -> Any:
    import numpy as np

    errors: dict[str, list[str]] = {}
    vis = arguments["vis"]
    if vis is None:
        errors["vis"] = ["null value not allowed"]
    elif not isinstance(vis, str):
        raise AssertionError(f"the vis parameter must be a path that exists ('{vis}' does not exist)")
    else:
        _validate_required_path(values, "vis", vis, search_data=True)
    types = {
        "seasonal_weight": ((float, int, np.int32, np.int64, np.float32, np.float64), "cFloat", float),
        "doPlot": ((bool, np.bool_), "cBool", bool),
        "plotName": ((str,), "cStr", str),
    }
    for name, (expected, casa_type, convert) in types.items():
        value = arguments[name]
        if value is None:
            errors[name] = ["null value not allowed"]
        elif not isinstance(value, expected):
            errors[name] = [f"must be of {casa_type} type"]
        elif name in values:
            values[name] = convert(value)
    if errors:
        raise AssertionError(str(dict(sorted(errors.items()))))
    try:
        value = run("plotweather", **values).value
    except TaskExecutionError as error:
        if error.completion.message in {"array of sample points is empty", "Axis limits cannot be NaN or Inf"}:
            raise ValueError(error.completion.message) from error
        raise
    return [np.float64(item) for item in value]


def _call_listcal(values: dict[str, Any], arguments: Mapping[str, Any]) -> None:
    for name in ("vis", "caltable"):
        value = arguments[name]
        if not isinstance(value, str):
            raise AssertionError(f"the {name} parameter must be a path that exists ('{value}' does not exist)")
        _validate_required_path(values, name, value, search_data=True)
    for name in ("field", "antenna", "spw", "listfile"):
        if not isinstance(arguments[name], str):
            message = "null value not allowed" if arguments[name] is None else "must be of cStr type"
            raise AssertionError(str({name: [message]}))
    pagerows = arguments["pagerows"]
    if not isinstance(pagerows, int):
        import numpy as np

        if not isinstance(pagerows, (np.int32, np.int64)):
            message = "null value not allowed" if pagerows is None else "must be of cInt type"
            raise AssertionError(str({"pagerows": [message]}))
        values["pagerows"] = int(pagerows)
    elif isinstance(pagerows, bool):
        values["pagerows"] = int(pagerows)
    try:
        completion = run("listcal", **values)
    except (TypeError, ValueError) as error:
        raise AssertionError(str(error)) from error
    _present_listcal(loads(completion.stdout)["result"])


def _present_listcal(report: Mapping[str, Any]) -> None:
    if report["listfile"] is not None:
        print(f"Writing output to file: {report['listfile']}")
        return
    prompt = True
    quit_requested = False
    solutions = 0
    listing = report["listing"]
    for part in listing["parts"]:
        print(part["text"], end="")
        solutions = part["solutions"]
        if prompt and part["prompt"]:
            try:
                response = input("Type Q to quit, A to list all, or RETURN to continue [continue]: ")
            except EOFError:
                response = ""
            command = response[:1].lower()
            quit_requested |= command == "q"
            prompt &= command != "a"
        if quit_requested and part["last_in_integration"]:
            break
    else:
        if listing["error"] is not None:
            raise RuntimeError(listing["error"])
    print(f"\nListed {solutions} antenna solutions.\n")


def _call_sdatmcor(values: dict[str, Any]) -> None:
    if "datacolumn" in values:
        column = values["datacolumn"]
        if column is None:
            messages = ["unallowed value None", "null value not allowed"]
        elif not isinstance(column, str):
            messages = ["must be of cStr type"]
        elif column not in ("data", "float_data", "corrected"):
            messages = [f"unallowed value {column}"]
        else:
            messages = []
        if messages:
            raise AssertionError(str({"datacolumn": messages}))
    if isinstance(values.get("gainfactor"), dict):
        values["gainfactor"] = {str(key): item for key, item in values["gainfactor"].items()}
    try:
        return run("sdatmcor", **values).value
    except TaskExecutionError as error:
        message = error.completion.message or ""
        name = error.completion.parameter
        if name == "gainfactor" and message.endswith(" should exist."):
            raise FileNotFoundError(message) from error
        if name in ("infile", "outfile", "atmtype", "humidity", "datacolumn"):
            raise Exception(message) from error
        if name in ("dtem_dh", "h0", "altitude", "temperature", "pressure", "pwv",
                    "dp", "layerboundaries", "layertemperature"):
            raise ValueError(message) from error
        raise RuntimeError(message) from error


def _call_sdfit(values: dict[str, Any], arguments: Mapping[str, Any]) -> Any:
    import numpy as np

    _validate_required_path(values, "infile", arguments["infile"], search_data=True)
    for name, allowed in (
        ("fitfunc", ("gaussian", "lorentzian")),
        ("fitmode", ("list", "auto")),
        ("polaverage", ("", "stokes", "geometric")),
    ):
        value = arguments[name]
        if isinstance(value, str) and value not in allowed:
            raise AssertionError(str({name: [f"unallowed value {value}"]}))
    for name in ("edge", "nfit"):
        if name not in values:
            continue
        value = values[name]
        if isinstance(value, (int, np.integer)):
            value = [value]
        if value is None:
            raise AssertionError(str({name: ["null value not allowed"]}))
        try:
            items = list(value)
        except TypeError:
            items = None
        if (isinstance(value, (str, bytes)) or items is None
                or not all(isinstance(item, (int, np.integer)) for item in items)):
            raise AssertionError(str({name: ["must be of cIntVec type"]}))
        values[name] = [int(item) for item in items]
    for name in ("thresh", "minwidth", "avg_limit"):
        if type(values.get(name)) is bool:
            values[name] = int(values[name])
    try:
        result = run("sdfit", **values).value
        nfit = arguments["nfit"]
        if arguments["fitmode"] == "list" and isinstance(nfit, np.ndarray) and len(nfit):
            count = sum(nfit)
            result["nfit"] = [count] * len(result["nfit"])
        return result
    except TaskExecutionError as error:
        message = error.completion.message or ""
        if message == "Invalid line component result.":
            raise AssertionError() from error
        if message.endswith(" exists.") or message.startswith("fitmode="):
            raise ValueError(message) from error
        raise RuntimeError(message) from error
    except (TypeError, ValueError) as error:
        raise AssertionError(str(error)) from error


def _call_initweights(values: dict[str, Any]) -> Any:
    import numpy as np

    if isinstance(values.get("tsystable"), str):
        values["tsystable"] = os.path.expanduser(values["tsystable"])
    if "spwmap" in values:
        mapping = values["spwmap"]
        try:
            items = list(mapping)
        except TypeError:
            items = [mapping] if isinstance(mapping, (int, np.int32)) else None
        if mapping is None:
            raise AssertionError("{'spwmap': ['null value not allowed']}")
        if items is None or not all(isinstance(item, (int, np.int32, np.int64)) for item in items):
            raise AssertionError("{'spwmap': ['must be of cIntVec type']}")
        values["spwmap"] = [int(item) for item in items]
    if isinstance(values.get("dowtsp"), np.bool_):
        raise TypeError("in method 'calibrater_initweights', argument 3 of type 'bool'")
    mode = values.get("wtmode", "nyq")
    allowed = ("delwtsp", "nyq", "ones", "tsys", "weight", "tinttsys", "sigma", "delsigsp")
    if isinstance(mode, str) and mode not in allowed:
        raise AssertionError(str({"wtmode": [f"unallowed value {mode}"]}))
    if mode is None:
        raise AssertionError("{'wtmode': ['unallowed value None', 'null value not allowed']}")
    try:
        return run("initweights", **values).value
    except TaskExecutionError as error:
        if error.completion.parameter == "tsystable.missing":
            raise Exception(error.completion.message) from error
        raise
    except (TypeError, ValueError) as error:
        raise AssertionError(str(error)) from error


def _coerce_ms_transform_selectors(task: str, values: dict[str, Any]) -> None:
    import numpy as np

    for name in ("field", "spw", "scan", "antenna", "timerange"):
        if name not in values or isinstance(values[name], str):
            continue
        original = values[name]
        candidate = original
        if not isinstance(original, np.ndarray):
            try:
                candidate = list(original)
            except TypeError:
                pass
        converted, record_type = _ms_transform_selector_variant(name, candidate)
        # Preserve scalar NumPy provenance even for a value whose tool
        # record CASA omits, or whose split selector is read as text.
        scalar_array = isinstance(original, np.ndarray) and original.ndim == 0
        values[name] = original if record_type is None or scalar_array else converted
        if not (task == "split" and name != "timerange") and record_type is not None:
            raise RuntimeError(
                "RecordRep::get_pointer - incorrect data type String   "
                f"used for field {name} with type {record_type:<8}"
            )
    if "correlation" in values:
        values["correlation"] = _ms_transform_string_or_vector("correlation", values["correlation"])
    for name in ("observation", "intent", "array", "uvrange", "feed"):
        if name not in values:
            continue
        value = values[name]
        if isinstance(value, str):
            continue
        converted, record_type = _ms_transform_selector_variant(name, value)
        if name == "observation" or (task == "split" and name in ("array", "feed")):
            values[name] = converted
        elif record_type is None:
            # The Rust tool omits NumPy integer selectors where CASA
            # does. Keep their values available for provenance.
            if name == "intent":
                del values[name]
        else:
            raise RuntimeError(
                "RecordRep::get_pointer - incorrect data type String   "
                f"used for field {name} with type {record_type:<8}"
            )


def _coerce_ms_channel_width(task: str, values: dict[str, Any]) -> None:
    import numpy as np

    name = "width" if task == "split" else "chanbin"
    if name not in values:
        return
    value = values[name]
    integers = (int, np.int32, np.int64)
    if isinstance(value, np.ndarray) and value.ndim == 0 and value.dtype.type in integers:
        values[name] = value.item()
        return
    if isinstance(value, integers):
        return
    if name == "width" and isinstance(value, str):
        return
    if not isinstance(value, (str, bytes)):
        try:
            vector = list(iter(value))
        except TypeError:
            vector = None
        if vector is not None and all(isinstance(item, integers) for item in vector):
            values[name] = vector
            return
        if name == "width" and vector and all(isinstance(item, str) for item in vector):
            # A numeric string list is converted to a map by CASA's Python
            # task. The processing raises at the subsequent scalar comparison.
            if vector[0].isdigit():
                list(map(int, vector))
                raise TypeError("'>' not supported between instances of 'map' and 'int'")
            raise TypeError("'int' object is not iterable")
    kinds = ["cStr", "cStrVec"] if name == "width" else []
    kinds.extend(["cInt", "cIntVec"])
    if value is None:
        raise AssertionError(str({name: ["null value not allowed"] * len(kinds)}))
    raise AssertionError(str({name: [f"must be of {kind} type" for kind in kinds]}))


def _validate_ms_transform_datacolumn(value: Any) -> None:
    if value is None:
        raise AssertionError(str({"datacolumn": ["unallowed value None", "null value not allowed"]}))
    if not isinstance(value, str):
        raise AssertionError(str({"datacolumn": ["must be of cStr type"]}))
    # Retain the original casa-rs aliases and spelling for the two existing layouts.
    if value.strip().lower() in ("data", "corrected", "corrected_data"):
        return
    if value != value.strip() or value not in (value.lower(), value.upper()):
        raise AssertionError(str({"datacolumn": [f"unallowed value {value}"]}))


def _ms_transform_selector_variant(name: str, value: Any) -> tuple[Any, str | None]:
    import numpy as np

    if value is None:
        raise AssertionError(str({name: ["null value not allowed"] * 5}))
    if type(value) is bool:
        return value, "Bool"
    if isinstance(value, int):
        return int(value), "Int"
    if isinstance(value, (np.int32, np.int64)):
        return int(value), None
    if isinstance(value, np.ndarray):
        scalar_dtype = value.dtype.type in (np.int32, np.int64, np.str_)
        if value.ndim == 0 and scalar_dtype:
            return value.item(), "unknown"
        if value.ndim == 1 and (not value.size or scalar_dtype or value.dtype.kind == "O"):
            value = value.tolist()
    if isinstance(value, (list, tuple)) and (
        all(isinstance(item, str) for item in value)
        or all(isinstance(item, (int, np.int32, np.int64)) for item in value)
    ):
        return list(value), "unknown"
    types = ("cStr", "cStrVec", "cInt", "cIntVec")
    raise AssertionError(str({name: [f"must be of {kind} type" for kind in types]}))


def _coerce_ms_task_options(
    task: str, arguments: Mapping[str, Any], values: dict[str, Any]
) -> None:
    import numpy as np

    def check(name: str, types: tuple[type, ...], casa_type: str) -> Any:
        value = arguments[name]
        if not isinstance(value, types):
            message = "null value not allowed" if value is None else f"must be of {casa_type} type"
            raise AssertionError(str({name: [message]}))
        return value

    check("timebin", (str,), "cStr")
    span = "combine" if task == "split" else "timespan"
    if span in values:
        values[span] = _ms_transform_string_or_vector(span, values[span])
    if task == "split":
        keep_mms = check("keepmms", (bool, np.bool_), "cBool")
        if "keepmms" in values:
            values["keepmms"] = bool(keep_mms)
    else:
        for flag in ("timeaverage", "hanning", "combinespws"):
            active = check(flag, (bool, np.bool_), "cBool")
            if flag in values:
                values[flag] = bool(active)
        distance = check("maxuvwdistance", (float, int, np.float32, np.float64, np.int32, np.int64),
                         "cFloat")
        if "maxuvwdistance" in values:
            values["maxuvwdistance"] = float(distance)


def _ms_transform_string_or_vector(name: str, value: Any) -> Any:
    if isinstance(value, str):
        return value
    import numpy as np

    if value is None:
        raise AssertionError(str({name: ["null value not allowed"] * 3}))
    if isinstance(value, np.ndarray):
        if value.ndim == 0 and value.dtype.type is np.str_:
            return value
        if value.ndim == 1 and (not value.size or value.dtype.kind in ("U", "O")):
            value = value.tolist()
    if isinstance(value, (list, tuple)) and all(isinstance(label, str) for label in value):
        return value
    raise AssertionError(str({name: ["must be of cStr type", "must be of cStrVec type"]}))


def _call_gencal(values: dict[str, Any]) -> Any:
    if "parameter" in values:
        import numpy as np

        parameter = values["parameter"]
        numbers = (float, int, np.float32, np.float64, np.int32, np.int64)
        # CASA applies to_list before to_floatvec. Python bools are numeric
        # inside the resulting list, whereas boolean scalars and np.bool_ are not.
        try:
            vector = [parameter] if isinstance(parameter, str) else list(parameter)
        except TypeError:
            vector = [parameter] if type(parameter) in numbers else None
        if vector is None or not all(isinstance(item, numbers) for item in vector):
            raise TypeError("{'parameter': ['must be of cFloatVec type']}")
        values["parameter"] = [float(item) for item in vector]
    if values.get("caltype") == "jyperk":
        return _call_jyperk("gencal", values)
    try:
        return run("gencal", **values).value
    except TaskExecutionError as error:
        parameter, message = error.completion.parameter, error.completion.message
        errors = {
            "infile.missing": FileNotFoundError,
            "infile.permission": PermissionError,
            "infile.directory": IsADirectoryError,
            "infile.io": OSError,
            "infile.key": KeyError,
            "infile.index": IndexError,
            "infile.type": TypeError,
            "infile.attribute": AttributeError,
        }
        if parameter in errors:
            raise errors[parameter](message) from error
        if parameter == "infile.json":
            raise JSONDecodeError(message, "", 0) from error
        if parameter == "antenna.archive.url":
            raise URLError(message) from error
        if parameter and parameter.startswith("antenna.archive.http."):
            status = int(parameter.rsplit(".", 1)[-1])
            url = message.rsplit(" at ", 1)[-1]
            try:
                reason = HTTPStatus(status).phrase
            except ValueError:
                reason = message
            raise HTTPError(url, status, reason, None, None) from error
        raise


def _call_jyperk(name: str, values: dict[str, Any]) -> Any:
    try:
        return run(name, **values).value
    except TaskParameterError as error:
        if error.completion.parameter in ("infile", "caltable", "endpoint"):
            raise ValueError(error.completion.message) from error
        raise
    except TaskExecutionError as error:
        error_type = {
            "jyperk.os": OSError,
            "jyperk.no_factors": Exception,
            "jyperk.vis": FileNotFoundError,
        }.get(error.completion.parameter, RuntimeError)
        raise error_type(error.completion.message) from error


def _call_getantposalma(values: dict[str, Any]) -> Any:
    import numpy as np

    outfile = values.get("outfile")
    if isinstance(outfile, (str, PathLike)):
        values["outfile"] = os.path.expanduser(os.fspath(outfile))
    hosts = values.get("hosts")
    if hosts is not None and not isinstance(hosts, str):
        try:
            values["hosts"] = list(hosts)
        except TypeError:
            pass
    snr = values.get("snr")
    if isinstance(snr, np.int32):
        values["snr"] = int(snr)
    elif isinstance(snr, (np.int64, np.float32, np.float64)):
        values["snr"] = float(snr)
    retries = values.get("nretry")
    if isinstance(retries, (bool, np.int32, np.int64)):
        values["nretry"] = int(retries)
    delay = values.get("rdelay")
    if isinstance(delay, (int, np.int32, np.int64, np.float32, np.float64)):
        values["rdelay"] = float(delay)
    if isinstance(values.get("overwrite"), np.bool_):
        values["overwrite"] = bool(values["overwrite"])
    if isinstance(values.get("firstintegration"), np.bool_):
        raise ValueError("Parameter firstintegration must be a boolean")
    try:
        return run("getantposalma", **values).value
    except TaskExecutionError as error:
        if error.completion.parameter == "hosts.json":
            raise JSONDecodeError(error.completion.message, "", 0) from error
        if error.completion.parameter == "snr.type":
            raise TypeError(error.completion.message) from error
        raise
    except TaskParameterError:
        raise
    except (TypeError, ValueError) as error:
        raise AssertionError(str(error)) from error


def _validate_required_path(
    values: dict[str, Any], name: str, argument: Any, *, search_data: bool = False
) -> None:
    # CASA validates and expands a required path before opening the table.
    if isinstance(argument, (str, PathLike)):
        original = os.fspath(argument)
        path = os.path.expanduser(original)
        if (search_data and path and not Path(path).is_absolute()
                and not path.startswith(("./", "../")) and not os.path.exists(path)):
            path = _resolve_relative_data_path(path)
        if path and os.path.exists(path):
            if path != original:
                values[name] = path
            return
    else:
        path = argument
    raise AssertionError(f"the {name} parameter must be a path that exists ('{path}' does not exist)")


def _resolve_relative_data_path(path: str) -> str:
    roots = []
    if configured := os.environ.get("CASA_RS_MEASURESPATH"):
        roots.append(Path(configured))
    if installation := os.environ.get("CASAPATH", "").split():
        roots.append(Path(installation[0]) / "data")
    roots.append(Path.home() / ".casa" / "data")
    for root in roots:
        candidate = root / path
        if candidate.exists():
            return str(candidate)
    return path


def _call_appendantab(values: dict[str, Any], arguments: Mapping[str, Any]) -> Any:
    for key in ("vis", "antab"):
        value = arguments[key]
        if value is None:
            continue
        if not isinstance(value, str):
            raise AssertionError(
                f"the {key} parameter must be a path that exists ('{value}' does not exist)")
        _validate_required_path(values, key, value, search_data=True)
    errors = {}
    for key, value in arguments.items():
        if value is None:
            errors[key] = ["null value not allowed"]
        elif key == "outvis" and not isinstance(value, str):
            errors[key] = ["must be of cStr type"]
        elif key in ("overwrite", "append_tsys", "append_gc") and not isinstance(value, bool):
            from numpy import bool_
            if not isinstance(value, bool_):
                errors[key] = ["must be of cBool type"]
    if errors:
        raise AssertionError(str(dict(sorted(errors.items()))))
    try:
        return run("appendantab", **values).value
    except TaskExecutionError as error:
        if error.completion.parameter == "antab_header":
            raise KeyError(error.completion.message) from error
        raise RuntimeError(error.completion.message) from error
    except (TypeError, ValueError) as error:
        raise AssertionError(str(error)) from error


def _call_pccor(values: dict[str, Any], arguments: Mapping[str, Any]) -> Any:
    errors = {}
    for key, value in arguments.items():
        if value is None:
            errors[key] = ["null value not allowed"]
        elif key == "vis" and not isinstance(value, str):
            errors[key] = [f"the vis parameter must be a path that exists ('{value}' does not exist)"]
        elif key != "cablecal_correction" and not isinstance(value, str):
            errors[key] = ["must be of cStr type"]
    if errors:
        if list(errors) == ["vis"] and arguments["vis"] is not None:
            raise AssertionError(errors["vis"][0])
        raise AssertionError(str(errors))
    _validate_required_path(values, "vis", arguments["vis"], search_data=True)
    try:
        return run("pccor", **values).value
    except TaskExecutionError as error:
        kind = {
            "selection": ValueError,
            "ff_table": Exception,
            "pccor_index": IndexError,
        }.get(error.completion.parameter, RuntimeError)
        raise kind(error.completion.message) from error
    except (TypeError, ValueError) as error:
        raise AssertionError(str(error)) from error


def _call_polfromgain(values: dict[str, Any], arguments: Mapping[str, Any]) -> Any:
    import numpy as np

    for name in ("vis", "tablein"):
        _validate_required_path(values, name, arguments[name], search_data=True)
    output = arguments["caltable"]
    if not isinstance(output, (str, PathLike)):
        message = "null value not allowed" if output is None else "must be of cStr type"
        raise AssertionError(str({"caltable": [message]}))
    for name in ("paoffset", "minpacov"):
        value = arguments[name]
        if not isinstance(value, (float, int, np.float32, np.float64, np.int32, np.int64)):
            message = "null value not allowed" if value is None else "must be of cFloat type"
            raise AssertionError(str({name: [message]}))
        if name in values:
            number = float(value)
            limit = sys.float_info.max
            # The finite transport value overflows in CASA's degrees-to-radians
            # operation. NaN coverage disables the minimum comparison.
            if isnan(number):
                number = -limit if name == "minpacov" else limit
            values[name] = max(-limit, min(number, limit))
    try:
        return run("polfromgain", **values).value
    except TaskExecutionError as error:
        if error.completion.message == "SVD did not converge in Linear Least Squares":
            raise np.linalg.LinAlgError(error.completion.message) from error
        raise


def _call_blcal(values: dict[str, Any], arguments: Mapping[str, Any]) -> Any:
    errors = {key: ["null value not allowed"] for key, value in arguments.items() if value is None}
    if "calmode" in errors:
        errors["calmode"].insert(0, "unallowed value None")
    if "observation" in errors:
        errors["observation"] *= 3
    if errors:
        raise AssertionError(str(dict(sorted(errors.items()))))
    _validate_required_path(values, "vis", arguments["vis"], search_data=True)
    _check_applycal_selectors(values, variants=("uvrange", "spwmap", "solint"))
    mode = arguments["calmode"]
    mapping = arguments["spwmap"]
    has_mapping = len(mapping) > 0
    if has_mapping and isinstance(mapping, dict):
        raise KeyError(0)
    if not has_mapping:
        values.pop("spwmap", None)
    if isinstance(mode, str) and mode not in ("a", "p", "ap"):
        raise AssertionError(str({"calmode": [f"unallowed value {mode}"]}))
    try:
        return run("blcal", **values).value
    except TaskParameterError as error:
        completion = error.completion
        if completion.parameter == "solint":
            raise RuntimeError("Error in Calibrater::setsolve.") from error
        if completion.parameter == "spwmap":
            if isinstance(values.get("spwmap"), dict):
                raise KeyError(0) from error
            raise TypeError(completion.message) from error
        raise
    except TaskExecutionError as error:
        message = error.completion.message or str(error)
        raise RuntimeError(message) from error
    except (TypeError, ValueError) as error:
        raise AssertionError(str(error)) from error


def _call_applycal(values: dict[str, Any]) -> Any:
    _check_applycal_selectors(values)
    try:
        return _run_task("applycal", None, None, values, value_only=True).value
    except TaskExecutionError as error:
        if error.completion.parameter == "calwt":
            raise IndexError(error.completion.message) from error
        if error.completion.parameter == "spwmap":
            if isinstance(values.get("spwmap"), dict):
                raise KeyError(0) from error
            raise TypeError(error.completion.message) from error
        raise
    except (TypeError, ValueError) as error:
        # The generated CASA wrapper uses AssertionError for schema refusals.
        raise AssertionError(str(error)) from error



def _check_applycal_selectors(values: Mapping[str, Any], variants=("uvrange", "spwmap")) -> None:
    observation = values.get("observation", "")
    supplied = {key: values[key] for key in variants if key in values}
    ordinary_variant = (str, int, float, list, dict)
    if isinstance(observation, (str, int)) and all(
        isinstance(value, ordinary_variant) for value in supplied.values()
    ):
        return
    import numpy as np

    if not isinstance(observation, (str, int, np.int32, np.int64)):
        raise AssertionError("{'observation': ['must be of cStr type', 'must be of cInt type']}")
    accepted_variant = (*ordinary_variant, np.int32, np.int64, np.float32, np.float64, np.ndarray)
    for key, value in supplied.items():
        if not isinstance(value, accepted_variant):
            raise AssertionError(str({key: ["must be of cVariant type"]}))


_MISSING = object()


def _is_default(value: Any, default: Any) -> bool:
    if value is default:
        return True
    return (
        type(value) is type(default)
        and isinstance(default, (str, int, float))
        and value == default
    )


@functools.cache
def _signature_defaults(wrapper: Callable[..., Any]) -> dict[str, Any]:
    return {
        name: parameter.default
        for name, parameter in inspect.signature(wrapper).parameters.items()
        if parameter.default is not inspect.Parameter.empty
    }


def _require_plotting() -> None:
    """Fail before creating scientific products when plotting is unavailable."""

    from matplotlib import pyplot as _plt  # noqa: F401


def _execute(
    task: str, resolved: SurfaceParameters, workspace: Path, *, value_only: bool = False,
) -> TaskCompletion:
    use_payload = task in {"simobserve", "simanalyze", "simalma"} and resolved.get("graphics") in {"file", "screen", "both"}
    if task == "plotms":
        path = resolved.get("plotfile") or ""
        export = resolved.get("expformat") or Path(path).suffix.lstrip(".")
        use_payload = resolved.get("showgui") or (bool(path) and export.lower() != "txt")
    if use_payload:
        # Import the renderer before the task creates scientific products.
        _require_plotting()

        with tempfile.TemporaryDirectory(prefix="casars-plots-") as directory:
            payload = Path(directory) / "arrays.bin"
            payload.touch()
            return _execute_task(task, resolved, workspace, payload)
    return _execute_task(task, resolved, workspace, None, value_only=value_only)


def _plotms_model_requested(parameters: SurfaceParameters) -> bool:
    y = parameters.get("ydatacolumn")
    ordinates = y if isinstance(y, (list, tuple)) else (y,)
    columns = (parameters.get("xdatacolumn"), *ordinates)
    names = (column.lower() for column in columns if isinstance(column, str))
    return any("model" in name or name == "residual" for name in names)


def _execute_task(
    task: str, resolved: SurfaceParameters, workspace: Path, payload: Path | None,
    *, bandpass_completed_pages: int | None = None, argument_text: Mapping[str, str] | None = None,
    value_only: bool = False,
) -> TaskCompletion:
    invocation = resolved.provider_invocation()
    if argument_text:
        assert invocation.stdin is not None
        request = loads(invocation.stdin)
        request["argument_text"] = argument_text
        invocation = replace(invocation, stdin=dumps(request))
    if task == "plotms" and (resolved.get("callib") or _plotms_model_requested(resolved)):
        command = (_task_executable("calibrate"), "plotms", *invocation.args)
    else:
        command = (_task_executable(task), *invocation.args)
    environment = dict(os.environ)
    environment.pop("CASARS_APPLYCAL_VALUE_ONLY", None)
    if task == "applycal" and value_only:
        environment["CASARS_APPLYCAL_VALUE_ONLY"] = "1"
    if task == "listcal":
        # A later listing error takes effect only when the reader reaches it.
        environment["CASARS_LISTCAL_PAGED_REPLY"] = "1"
    if task in {"simobserve", "simanalyze", "simalma", "plotms"}:
        environment.pop("CASARS_PLOT_PAYLOAD", None)
    if payload is not None:
        environment["CASARS_PLOT_PAYLOAD"] = str(payload)
    if task == "plotbandpass":
        environment.pop("CASARS_BANDPASS_COMPLETED_PAGES", None)
        if bandpass_completed_pages is not None:
            environment["CASARS_BANDPASS_COMPLETED_PAGES"] = str(bandpass_completed_pages)
    table_write = None
    if task == "clearcal" and (vis := resolved.get("vis")):
        table_write = _native().prepare_task_table_write(workspace / vis[0])
    try:
        try:
            # The task runs in its own session, so a terminal's Ctrl-C reaches
            # Python alone, which passes it on once, in a terminal as in a
            # notebook: a second interrupt ends the imager at once.
            process = subprocess.Popen(
                command,
                cwd=workspace,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                start_new_session=True,
                env=environment,
            )
        except OSError as error:
            raise TaskInvocationError(f"cannot start {task} ({command[0]}): {error}") from error
        stdout, stderr = _communicate(process, invocation.stdin or "")
    finally:
        if table_write is not None:
            table_write.release()

    if payload is not None:
        try:
            plots = _native().task_plot_documents(task, process.returncode, stdout, payload.stat().st_size)
            _render_task_plots(plots, payload, workspace, resolved if task == "plotms" else None)
        except Exception as error:
            raise TaskResultError(f"{task} plot rendering failed: {error}") from error

    if task == "plotweather" and process.returncode == 0:
        plot = loads(stdout)["result"]["plot"]
        if plot is not None:
            from ._weather_plot import render_weather

            render_weather(plot, workspace)

    if task == "predictcomp" and process.returncode == 0:
        plot = loads(stdout)["result"]["plot"]
        if plot is not None:
            from ._predictcomp_plot import render_prediction

            render_prediction(plot, workspace)

    values = [
        (name, _display_value(state.value))
        for name, state in sorted(resolved.states.items())
        if state.value is not None
    ]
    try:
        outcome = _native().task_outcome(
            task, process.returncode, stdout, str(workspace), values
        )
    except ValueError as error:
        raise TaskResultError(
            f"{task} produced a successful but invalid result: {error}"
        ) from error
    completion = TaskCompletion(
        task=task,
        command=command,
        workspace=workspace,
        returncode=process.returncode,
        stdout=stdout,
        stderr=stderr,
        provider_invocation=invocation,
        status=outcome["status"],
        message=outcome.get("message", outcome.get("reason")),
        parameter=outcome.get("parameter"),
        value=outcome.get("value"),
        result=(
            completion_projection(outcome["completion"])
            if "completion" in outcome
            else None
        ),
    )
    if completion.status == "refused":
        raise TaskParameterError(completion)
    if not completion.successful:
        raise TaskExecutionError(completion)
    if task == "plotbandpass" and bandpass_completed_pages is None:
        from ._bandpass_plot import render_bandpass

        reply = loads(stdout)["result"]
        completed, immediate = render_bandpass(reply, workspace)
        if immediate:
            return replace(completion, value=None)
        if completion.value is not None and completed < len(reply["pages"]):
            partial = _execute_task(task, resolved, workspace, payload, bandpass_completed_pages=completed)
            return replace(completion, value=partial.value)
    return completion


def _render_task_plots(plots, payload: Path, workspace: Path, plotms=None) -> None:
    """Draw validated native artists, then release the temporary array file."""

    if not plots:
        return
    import numpy as np

    from matplotlib import pyplot as plt
    from .._plot_document import render_document

    # NumPy owns the mapping for as long as any view needs it, including
    # views retained in an artist's exception traceback. Explicit mmap.close
    # could invalidate those arrays while their Python objects are still live.
    mapped = np.memmap(payload, dtype="u1", mode="r") if payload.stat().st_size else None
    try:
        for plot in plots:
            figure, panel_axes = render_document(plot["document"], payload=mapped)
            completed = False
            try:
                if plotms is not None:
                    from ._plotms_plot import configure

                    if plot["output_file"] is not None:
                        dpi = _size_plotms_figure(figure, plotms)
                    configure(figure, panel_axes, plot["document"], plotms)
                if plot["output_file"] is not None:
                    output = workspace / plot["output_file"]
                    if plotms is None:
                        _save_figure(figure, output)
                    else:
                        _save_plotms_figure(figure, output, plotms, dpi)
                if plot["show"]:
                    figure.show()
                completed = True
            finally:
                if not plot["show"] or not completed:
                    plt.close(figure)
    finally:
        del mapped


def _size_plotms_figure(figure, options):
    dpi = options.get("dpi")
    if dpi <= 0:
        dpi = 300 if options.get("highres") else figure.dpi
    width, height = options.get("width"), options.get("height")
    if width > 0 or height > 0:
        current_width, current_height = figure.get_size_inches()
        figure.set_size_inches(width / dpi if width > 0 else current_width,
                               height / dpi if height > 0 else current_height)
    figure.set_dpi(dpi)
    return dpi


def _save_plotms_figure(figure, destination: Path, options, dpi) -> None:
    format_ = options.get("expformat") or destination.suffix.lstrip(".") or "png"
    _save_figure(figure, destination, replace_existing=options.get("overwrite"), format=format_.lower(), dpi=dpi)


def _save_figure(figure, destination: Path, *, replace_existing=True, **options) -> None:
    """Publish a complete figure, preserving the previous file on failure."""

    with tempfile.NamedTemporaryFile(prefix=f".{destination.name}.", suffix=destination.suffix or ".png", dir=destination.parent, delete=False) as staged:
        path = Path(staged.name)
    try:
        figure.savefig(path, **options)
        if replace_existing:
            path.replace(destination)
        else:
            os.link(path, destination)
    finally:
        path.unlink(missing_ok=True)


def _communicate(process: subprocess.Popen[str], request: str) -> tuple[str, str]:
    """Send ``request``, pass the log on as it arrives, and collect the reply.

    Each pipe has its own thread, so none fills while another is waited on,
    and the log keeps draining while an interrupted task shuts down. The main
    thread only waits for the task; anything that stops the wait, an
    interrupt above all, is passed to the task first.
    """

    reply: list[str] = []
    log: list[str] = []

    def send() -> None:
        try:
            assert process.stdin is not None
            process.stdin.write(request)
            process.stdin.close()
        except (BrokenPipeError, OSError):
            pass

    def collect() -> None:
        assert process.stdout is not None
        reply.append(process.stdout.read())

    def relay() -> None:
        assert process.stderr is not None
        for line in process.stderr:
            log.append(line)
            terminal = sys.stderr
            if terminal is None:
                continue
            # A terminal that fails to write must not stop the drain.
            try:
                terminal.write(line)
                terminal.flush()
            except Exception:
                pass

    threads = [threading.Thread(target=target) for target in (send, collect, relay)]
    for thread in threads:
        thread.daemon = True
        thread.start()
    try:
        process.wait()
    except BaseException:
        _stop(process)
        raise
    finally:
        for thread in threads:
            thread.join(timeout=_INTERRUPT_GRACE_SECONDS)
    return "".join(reply), "".join(log)


def _stop(process: subprocess.Popen[str]) -> None:
    """Interrupt a running task as Ctrl-C would, and kill it if it stays.

    The task has the grace period to stop, its log still draining; a second
    interrupt in that time kills it at once.
    """

    if process.poll() is not None:
        return
    try:
        process.send_signal(signal.SIGINT)
        process.wait(timeout=_INTERRUPT_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    except BaseException:
        process.kill()
        process.wait()
        raise


def _task_executable(task: str) -> str:
    """The executable that runs ``task``, found by the Rust resolver."""

    try:
        path = _native().task_executable(task, dict(os.environ), sys.prefix)
    except FileNotFoundError as error:
        raise TaskBinaryNotFoundError(str(error)) from None
    return os.fspath(path)


def _resolve_workspace(
    workspace: StrPath | None,
    parameters: SurfaceParameters | None,
) -> Path:
    if workspace is None and parameters is not None:
        root = parameters.workspace
    else:
        root = Path.cwd() if workspace is None else Path(workspace)
    root = root.resolve()
    if not root.is_dir():
        raise FileNotFoundError(
            f"task workspace does not exist or is not a directory: {root}"
        )
    return root


def _display_value(value: ParameterData) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return format(value, ".15g")
    if isinstance(value, (str, int)):
        return str(value)
    if isinstance(value, (list, tuple)):
        return ",".join(_display_value(item) for item in value)
    if isinstance(value, Mapping):
        import json

        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return str(value)


__all__ = [
    "TaskBinaryNotFoundError",
    "TaskCompletion",
    "TaskExecutionError",
    "TaskInvocationError",
    "TaskParameterError",
    "TaskResultError",
    "run",
]
