"""Raw ``casars-imager`` provider request/result object API.

This module intentionally does not load profiles or update managed Last state.
Use :func:`casars.tasks.profiles.imager` for the unified CASA-named parameter
lifecycle.
"""

from __future__ import annotations

import os
from os import PathLike
from typing import Any, Literal, TypeAlias

from .._task_runtime import (
    ProtocolInfo,
    configure_imager_binary,
    fetch_imager_schema,
    get_imager_protocol_info,
    invoke_imager_task,
)

StrPath: TypeAlias = str | PathLike[str]
TaskResult: TypeAlias = dict[str, Any]
SpectralMode: TypeAlias = Literal["mfs", "cube", "cubedata"]
Deconvolver: TypeAlias = Literal["hogbom", "mtmfs", "clark", "multiscale"]
HogbomIterationMode: TypeAlias = Literal["strict", "casa", "casa_inclusive"]
RestoringBeamMode: TypeAlias = Literal["per_plane", "common"]
WTermMode: TypeAlias = Literal["none", "direct", "wproject"]
GridderMode: TypeAlias = Literal[
    "standard", "wproject", "widefield", "mosaic", "awproject", "awp2", "awphpg"
]
PlaneSelection: TypeAlias = Literal["I", "Q", "U", "V", "XX", "YY", "RR", "LL"]
SaveModel: TypeAlias = Literal["none", "modelcolumn"]
CleanMaskMode: TypeAlias = Literal["user", "auto-multithresh"]
ImagingFftPrecision: TypeAlias = Literal["auto", "f64", "f32"]
ImagingFftBackend: TypeAlias = Literal["auto", "rustfft", "accelerate", "metal-mpsgraph"]


def configure(*, binary: StrPath | None) -> None:
    """Configure the default ``casars-imager`` binary override for this module."""

    configure_imager_binary(binary)


def protocol_info(*, binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol information for the selected binary."""

    return get_imager_protocol_info(binary=binary)


def schema(*, binary: StrPath | None = None) -> dict[str, Any]:
    """Return the Rust-emitted request/result schema bundle."""

    return fetch_imager_schema(binary=binary)


def run(request: dict[str, Any], *, binary: StrPath | None = None) -> TaskResult:
    """Execute one canonical ``casars-imager`` run request."""

    return invoke_imager_task(kind="run", request=request, binary=binary)


def mfs(
    measurement_set: StrPath,
    image_name: StrPath,
    *,
    image_size: int,
    cell_arcsec: float,
    field_ids: list[int] | None = None,
    phasecenter_field: int | None = None,
    phasecenter: str | None = None,
    ddid: int | None = None,
    spw: str | None = None,
    channel_start: int | None = None,
    channel_count: int | None = None,
    data_column: str | None = None,
    save_model: SaveModel = "none",
    start_model: StrPath | None = None,
    outlier_file: StrPath | None = None,
    correlation: PlaneSelection | None = None,
    weighting: str = "natural",
    robust: float = 0.5,
    gridder: GridderMode = "standard",
    use_pointing: bool = False,
    deconvolver: Deconvolver = "hogbom",
    nterms: int = 1,
    niter: int = 0,
    hogbom_iteration_mode: HogbomIterationMode = "strict",
    gain: float = 0.1,
    threshold_jy: float = 0.0,
    nsigma: float = 0.0,
    write_pb: bool = False,
    pbcor: bool = False,
    mosaic_pb_limit: float = 0.2,
    use_mask: CleanMaskMode = "user",
    auto_mask: dict[str, Any] | None = None,
    mask_boxes: list[tuple[int, int, int, int]] | None = None,
    mask_image: StrPath | None = None,
    w_term_mode: WTermMode = "none",
    w_project_planes: int | None = None,
    dirty_only: bool = False,
    parallel: bool | None = None,
    imaging_read_ahead_blocks: int | None = None,
    imaging_fft_precision: ImagingFftPrecision = "auto",
    imaging_fft_backend: ImagingFftBackend = "auto",
    write_preview_pngs: bool = True,
    binary: StrPath | None = None,
) -> TaskResult:
    """Run a CASA-style MFS imaging request through ``casars-imager``.

    The executable remains the owner of imaging behavior; this helper only
    builds the documented JSON request shape and delegates to ``--json-run``.
    """

    resolved_w_term_mode = _gridder_w_term_mode(gridder)
    if w_term_mode != "none" and resolved_w_term_mode != w_term_mode:
        raise ValueError("gridder and w_term_mode conflict; prefer gridder for CASA-style use")
    if w_term_mode != "none":
        resolved_w_term_mode = w_term_mode

    request = {
        "measurement_set": os.fspath(measurement_set),
        "image_name": os.fspath(image_name),
        "image_size": image_size,
        "cell_arcsec": cell_arcsec,
        "field_ids": field_ids,
        "phasecenter_field": phasecenter_field,
        "phasecenter": phasecenter,
        "ddid": ddid,
        "spw_selector": spw,
        "channel_start": channel_start,
        "channel_count": channel_count,
        "data_column": data_column,
        "save_model": save_model,
        "start_model": None if start_model is None else os.fspath(start_model),
        "outlier_file": None if outlier_file is None else os.fspath(outlier_file),
        "correlation": correlation,
        "spectral_mode": "mfs",
        "weighting": _weighting_request(weighting, robust),
        "use_pointing": use_pointing,
        "restoring_beam_mode": "per_plane",
        "deconvolver": deconvolver,
        "nterms": nterms,
        "niter": niter,
        "hogbom_iteration_mode": _hogbom_iteration_mode_request(hogbom_iteration_mode),
        "gain": gain,
        "threshold_jy": threshold_jy,
        "nsigma": nsigma,
        "write_pb": write_pb,
        "pbcor": pbcor,
        "mosaic_pb_limit": mosaic_pb_limit,
        "use_mask": use_mask,
        "auto_mask": {} if auto_mask is None else dict(auto_mask),
        "mask_boxes": [list(box) for box in (mask_boxes or [])],
        "mask_image": None if mask_image is None else os.fspath(mask_image),
        "w_term_mode": resolved_w_term_mode,
        "w_project_planes": w_project_planes,
        "dirty_only": dirty_only,
        "parallel": parallel,
        "imaging_read_ahead_blocks": imaging_read_ahead_blocks,
        "imaging_fft_precision": imaging_fft_precision,
        "imaging_fft_backend": imaging_fft_backend,
        "write_preview_pngs": write_preview_pngs,
    }
    return run(request, binary=binary)


def cube(
    measurement_set: StrPath,
    image_name: StrPath,
    *,
    image_size: int,
    cell_arcsec: float,
    field_ids: list[int] | None = None,
    phasecenter_field: int | None = None,
    phasecenter: str | None = None,
    ddid: int | None = None,
    spw: str | None = None,
    channel_start: int | None = None,
    channel_count: int | None = None,
    data_column: str | None = None,
    save_model: SaveModel = "none",
    outlier_file: StrPath | None = None,
    correlation: PlaneSelection | None = None,
    cube_axis: dict[str, Any] | None = None,
    weighting: str = "natural",
    robust: float = 0.5,
    per_channel_weight_density: bool | None = None,
    gridder: GridderMode = "standard",
    use_pointing: bool = False,
    restoring_beam_mode: RestoringBeamMode = "per_plane",
    deconvolver: Deconvolver = "hogbom",
    niter: int = 0,
    hogbom_iteration_mode: HogbomIterationMode = "strict",
    gain: float = 0.1,
    threshold_jy: float = 0.0,
    nsigma: float = 0.0,
    write_pb: bool = False,
    pbcor: bool = False,
    mosaic_pb_limit: float = 0.2,
    use_mask: CleanMaskMode = "user",
    auto_mask: dict[str, Any] | None = None,
    mask_boxes: list[tuple[int, int, int, int]] | None = None,
    mask_image: StrPath | None = None,
    w_term_mode: WTermMode = "none",
    w_project_planes: int | None = None,
    dirty_only: bool = False,
    parallel: bool | None = None,
    chanchunks: int | None = None,
    imaging_read_ahead_blocks: int | None = None,
    imaging_fft_precision: ImagingFftPrecision = "auto",
    imaging_fft_backend: ImagingFftBackend = "auto",
    write_preview_pngs: bool = True,
    binary: StrPath | None = None,
) -> TaskResult:
    """Run a CASA-style cube imaging request through ``casars-imager``."""

    return _spectral_cube(
        "cube",
        measurement_set,
        image_name,
        image_size=image_size,
        cell_arcsec=cell_arcsec,
        field_ids=field_ids,
        phasecenter_field=phasecenter_field,
        phasecenter=phasecenter,
        ddid=ddid,
        spw=spw,
        channel_start=channel_start,
        channel_count=channel_count,
        data_column=data_column,
        save_model=save_model,
        outlier_file=outlier_file,
        correlation=correlation,
        cube_axis=cube_axis,
        weighting=weighting,
        robust=robust,
        per_channel_weight_density=per_channel_weight_density,
        gridder=gridder,
        use_pointing=use_pointing,
        restoring_beam_mode=restoring_beam_mode,
        deconvolver=deconvolver,
        niter=niter,
        hogbom_iteration_mode=hogbom_iteration_mode,
        gain=gain,
        threshold_jy=threshold_jy,
        nsigma=nsigma,
        write_pb=write_pb,
        pbcor=pbcor,
        mosaic_pb_limit=mosaic_pb_limit,
        use_mask=use_mask,
        auto_mask=auto_mask,
        mask_boxes=mask_boxes,
        mask_image=mask_image,
        w_term_mode=w_term_mode,
        w_project_planes=w_project_planes,
        dirty_only=dirty_only,
        parallel=parallel,
        chanchunks=chanchunks,
        imaging_read_ahead_blocks=imaging_read_ahead_blocks,
        imaging_fft_precision=imaging_fft_precision,
        imaging_fft_backend=imaging_fft_backend,
        write_preview_pngs=write_preview_pngs,
        binary=binary,
    )


def cubedata(
    measurement_set: StrPath,
    image_name: StrPath,
    *,
    image_size: int,
    cell_arcsec: float,
    field_ids: list[int] | None = None,
    phasecenter_field: int | None = None,
    phasecenter: str | None = None,
    ddid: int | None = None,
    spw: str | None = None,
    channel_start: int | None = None,
    channel_count: int | None = None,
    data_column: str | None = None,
    save_model: SaveModel = "none",
    outlier_file: StrPath | None = None,
    correlation: PlaneSelection | None = None,
    cube_axis: dict[str, Any] | None = None,
    weighting: str = "natural",
    robust: float = 0.5,
    per_channel_weight_density: bool | None = None,
    gridder: GridderMode = "standard",
    use_pointing: bool = False,
    restoring_beam_mode: RestoringBeamMode = "per_plane",
    deconvolver: Deconvolver = "hogbom",
    niter: int = 0,
    hogbom_iteration_mode: HogbomIterationMode = "strict",
    gain: float = 0.1,
    threshold_jy: float = 0.0,
    nsigma: float = 0.0,
    write_pb: bool = False,
    pbcor: bool = False,
    mosaic_pb_limit: float = 0.2,
    use_mask: CleanMaskMode = "user",
    auto_mask: dict[str, Any] | None = None,
    mask_boxes: list[tuple[int, int, int, int]] | None = None,
    mask_image: StrPath | None = None,
    w_term_mode: WTermMode = "none",
    w_project_planes: int | None = None,
    dirty_only: bool = False,
    parallel: bool | None = None,
    chanchunks: int | None = None,
    imaging_read_ahead_blocks: int | None = None,
    imaging_fft_precision: ImagingFftPrecision = "auto",
    imaging_fft_backend: ImagingFftBackend = "auto",
    write_preview_pngs: bool = True,
    binary: StrPath | None = None,
) -> TaskResult:
    """Run a CASA-style cubedata imaging request through ``casars-imager``."""

    return _spectral_cube(
        "cubedata",
        measurement_set,
        image_name,
        image_size=image_size,
        cell_arcsec=cell_arcsec,
        field_ids=field_ids,
        phasecenter_field=phasecenter_field,
        phasecenter=phasecenter,
        ddid=ddid,
        spw=spw,
        channel_start=channel_start,
        channel_count=channel_count,
        data_column=data_column,
        save_model=save_model,
        outlier_file=outlier_file,
        correlation=correlation,
        cube_axis=cube_axis,
        weighting=weighting,
        robust=robust,
        per_channel_weight_density=per_channel_weight_density,
        gridder=gridder,
        use_pointing=use_pointing,
        restoring_beam_mode=restoring_beam_mode,
        deconvolver=deconvolver,
        niter=niter,
        hogbom_iteration_mode=hogbom_iteration_mode,
        gain=gain,
        threshold_jy=threshold_jy,
        nsigma=nsigma,
        write_pb=write_pb,
        pbcor=pbcor,
        mosaic_pb_limit=mosaic_pb_limit,
        use_mask=use_mask,
        auto_mask=auto_mask,
        mask_boxes=mask_boxes,
        mask_image=mask_image,
        w_term_mode=w_term_mode,
        w_project_planes=w_project_planes,
        dirty_only=dirty_only,
        parallel=parallel,
        chanchunks=chanchunks,
        imaging_read_ahead_blocks=imaging_read_ahead_blocks,
        imaging_fft_precision=imaging_fft_precision,
        imaging_fft_backend=imaging_fft_backend,
        write_preview_pngs=write_preview_pngs,
        binary=binary,
    )


def _spectral_cube(
    spectral_mode: Literal["cube", "cubedata"],
    measurement_set: StrPath,
    image_name: StrPath,
    *,
    image_size: int,
    cell_arcsec: float,
    field_ids: list[int] | None,
    phasecenter_field: int | None,
    phasecenter: str | None,
    ddid: int | None,
    spw: str | None,
    channel_start: int | None,
    channel_count: int | None,
    data_column: str | None,
    save_model: SaveModel,
    outlier_file: StrPath | None,
    correlation: PlaneSelection | None,
    cube_axis: dict[str, Any] | None,
    weighting: str,
    robust: float,
    per_channel_weight_density: bool | None,
    gridder: GridderMode,
    use_pointing: bool,
    restoring_beam_mode: RestoringBeamMode,
    deconvolver: Deconvolver,
    niter: int,
    hogbom_iteration_mode: HogbomIterationMode,
    gain: float,
    threshold_jy: float,
    nsigma: float,
    write_pb: bool,
    pbcor: bool,
    mosaic_pb_limit: float,
    use_mask: CleanMaskMode,
    auto_mask: dict[str, Any] | None,
    mask_boxes: list[tuple[int, int, int, int]] | None,
    mask_image: StrPath | None,
    w_term_mode: WTermMode,
    w_project_planes: int | None,
    dirty_only: bool,
    parallel: bool | None,
    chanchunks: int | None,
    imaging_read_ahead_blocks: int | None,
    imaging_fft_precision: ImagingFftPrecision,
    imaging_fft_backend: ImagingFftBackend,
    write_preview_pngs: bool,
    binary: StrPath | None,
) -> TaskResult:
    resolved_w_term_mode = _gridder_w_term_mode(gridder)
    if w_term_mode != "none" and resolved_w_term_mode != w_term_mode:
        raise ValueError("gridder and w_term_mode conflict; prefer gridder for CASA-style use")
    if w_term_mode != "none":
        resolved_w_term_mode = w_term_mode

    request = {
        "measurement_set": os.fspath(measurement_set),
        "image_name": os.fspath(image_name),
        "image_size": image_size,
        "cell_arcsec": cell_arcsec,
        "field_ids": field_ids,
        "phasecenter_field": phasecenter_field,
        "phasecenter": phasecenter,
        "ddid": ddid,
        "spw_selector": spw,
        "channel_start": channel_start,
        "channel_count": channel_count,
        "data_column": data_column,
        "save_model": save_model,
        "start_model": None,
        "outlier_file": None if outlier_file is None else os.fspath(outlier_file),
        "correlation": correlation,
        "spectral_mode": spectral_mode,
        "cube_axis": {} if cube_axis is None else dict(cube_axis),
        "weighting": _weighting_request(weighting, robust),
        "per_channel_weight_density": per_channel_weight_density,
        "use_pointing": use_pointing,
        "restoring_beam_mode": restoring_beam_mode,
        "deconvolver": deconvolver,
        "nterms": 1,
        "niter": niter,
        "hogbom_iteration_mode": _hogbom_iteration_mode_request(hogbom_iteration_mode),
        "gain": gain,
        "threshold_jy": threshold_jy,
        "nsigma": nsigma,
        "write_pb": write_pb,
        "pbcor": pbcor,
        "mosaic_pb_limit": mosaic_pb_limit,
        "use_mask": use_mask,
        "auto_mask": {} if auto_mask is None else dict(auto_mask),
        "mask_boxes": [list(box) for box in (mask_boxes or [])],
        "mask_image": None if mask_image is None else os.fspath(mask_image),
        "w_term_mode": resolved_w_term_mode,
        "w_project_planes": w_project_planes,
        "dirty_only": dirty_only,
        "parallel": parallel,
        "chanchunks": chanchunks,
        "imaging_read_ahead_blocks": imaging_read_ahead_blocks,
        "imaging_fft_precision": imaging_fft_precision,
        "imaging_fft_backend": imaging_fft_backend,
        "write_preview_pngs": write_preview_pngs,
    }
    return run(request, binary=binary)


def _weighting_request(weighting: str, robust: float) -> dict[str, Any]:
    if weighting == "natural":
        return {"kind": "natural"}
    if weighting == "uniform":
        return {"kind": "uniform"}
    if weighting == "briggs":
        return {"kind": "briggs", "robust": robust}
    if weighting == "briggsbwtaper":
        return {"kind": "briggs_bw_taper", "robust": robust}
    raise ValueError("weighting must be 'natural', 'uniform', 'briggs', or 'briggsbwtaper'")


def _hogbom_iteration_mode_request(mode: HogbomIterationMode) -> str:
    if mode == "strict":
        return "strict"
    if mode in {"casa", "casa_inclusive"}:
        return "casa_inclusive"
    raise ValueError("hogbom_iteration_mode must be 'strict', 'casa', or 'casa_inclusive'")


def _gridder_w_term_mode(gridder: GridderMode) -> WTermMode:
    if gridder in {"standard", "mosaic"}:
        return "none"
    if gridder == "wproject":
        return "wproject"
    if gridder in {"widefield", "awproject", "awp2", "awphpg"}:
        raise NotImplementedError(
            f"gridder={gridder!r} is not implemented by casa-rs imager yet; "
            "supported gridder values are 'standard', 'wproject', and 'mosaic'. "
            "Track widefield/AW-family parity in https://github.com/bglenden/casa-rs/issues/52"
        )
    raise ValueError(
        "gridder must be 'standard', 'wproject', 'widefield', 'mosaic', "
        "'awproject', 'awp2', or 'awphpg'"
    )
