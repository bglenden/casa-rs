"""Generated CASA-named wrappers for every catalog task.

Do not edit by hand; run ``scripts/generate-python-parameter-wrappers.py``.
"""

from __future__ import annotations

from collections.abc import Mapping
from os import PathLike
from typing import Any, Literal, TypeAlias

from numpy import floating
from numpy.typing import NDArray

from ..parameters import ParameterData
from ._runner import TaskCompletion, _call_casa, _run

StrPath: TypeAlias = str | PathLike[str]
_UNSET = object()


class _ImplicitStringDefault(str):
    """A default that keeps the signature text and distinguishes explicit strings."""


def _explicit(values: Mapping[str, object], names: tuple[str, ...]) -> dict[str, ParameterData]:
    return {name: values[name] for name in names if values[name] is not _UNSET}  # type: ignore[misc]


def msexplore(
    *,
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    format: Literal['json', 'text'] | object = _UNSET,
    output: StrPath | object = _UNSET,
    overwrite: bool | object = _UNSET,
    selectdata: bool | object = _UNSET,
    field: str | object = _UNSET,
    spw: str | object = _UNSET,
    timerange: str | object = _UNSET,
    uvrange: str | object = _UNSET,
    antenna: str | object = _UNSET,
    scan: str | object = _UNSET,
    correlation: str | object = _UNSET,
    observation: str | object = _UNSET,
    array: str | object = _UNSET,
    intent: str | object = _UNSET,
    feed: str | object = _UNSET,
    msselect: str | object = _UNSET,
    page_spec: StrPath | object = _UNSET,
    preset: Literal['amplitude_phase_vs_time_stacked', 'amplitude_vs_channel', 'amplitude_vs_frequency', 'amplitude_vs_time', 'amplitude_vs_uv_distance', 'amplitude_vs_velocity', 'antenna_layout', 'azimuth_vs_elevation', 'azimuth_vs_time', 'elevation_vs_time', 'flag_vs_time', 'flagrow_vs_time', 'hour_angle_vs_time', 'none', 'parallactic_angle_vs_time', 'phase_vs_channel', 'phase_vs_frequency', 'phase_vs_time', 'phase_vs_velocity', 'real_vs_imaginary', 'scan_timeline', 'sigma_spectrum_vs_time', 'sigma_vs_time', 'spectral_window_coverage', 'uv_coverage', 'weight_spectrum_vs_time', 'weight_vs_time'] | object = _UNSET,
    x_axis: str | object = _UNSET,
    y_axis: str | object = _UNSET,
    y_axis2: str | object = _UNSET,
    datacolumn: str | object = _UNSET,
    color_by: Literal['baseline', 'correlation', 'field', 'none', 'scan', 'spw'] | object = _UNSET,
    avgchannel: int | Literal['none'] | object = _UNSET,
    avgtime: float | Literal['none'] | object = _UNSET,
    avgscan: bool | object = _UNSET,
    avgfield: bool | object = _UNSET,
    avgbaseline: bool | object = _UNSET,
    avgantenna: bool | object = _UNSET,
    avgspw: bool | object = _UNSET,
    scalar: bool | object = _UNSET,
    freqframe: Literal['BARY', 'CMB', 'GALACTO', 'GEO', 'LGROUP', 'LSRD', 'LSRK', 'TOPO', 'none'] | object = _UNSET,
    restfreq: str | object = _UNSET,
    veldef: Literal['OPTICAL', 'RADIO', 'TRUE'] | object = _UNSET,
    iteraxis: Literal['correlation', 'field', 'none', 'scan', 'spw'] | object = _UNSET,
    gridrows: int | object = _UNSET,
    gridcols: int | object = _UNSET,
    xselfscale: bool | object = _UNSET,
    yselfscale: bool | object = _UNSET,
    xsharedaxis: bool | object = _UNSET,
    ysharedaxis: bool | object = _UNSET,
    title: str | object = _UNSET,
    xlabel: str | object = _UNSET,
    ylabel: str | object = _UNSET,
    symbol_size: float | Literal['none'] | object = _UNSET,
    showlegend: bool | object = _UNSET,
    legendposition: Literal['exteriorBottom', 'exteriorLeft', 'exteriorRight', 'exteriorTop', 'lowerLeft', 'lowerRight', 'upperLeft', 'upperRight'] | object = _UNSET,
    showmajorgrid: bool | object = _UNSET,
    showminorgrid: bool | object = _UNSET,
    headeritems: str | object = _UNSET,
    max_points: int | object = _UNSET,
    plot_output: StrPath | object = _UNSET,
    plot_format: Literal['txt'] | object = _UNSET,
    flag_action: Literal['flag', 'none', 'unflag'] | object = _UNSET,
    flag_xmin: float | Literal['none'] | object = _UNSET,
    flag_xmax: float | Literal['none'] | object = _UNSET,
    flag_ymin: float | Literal['none'] | object = _UNSET,
    flag_ymax: float | Literal['none'] | object = _UNSET,
    flag_plotindex: int | Literal['none'] | object = _UNSET,
    flag_panel: str | object = _UNSET,
    flag_extcorr: bool | object = _UNSET,
    flag_extchannel: bool | object = _UNSET,
    flag_selected: bool | object = _UNSET,
    flag_apply: bool | object = _UNSET,
    flag_output: StrPath | object = _UNSET,
) -> TaskCompletion:
    """explore and export common MeasurementSet plotms-style plots"""
    return _run("msexplore", _explicit(locals(), ('vis', 'format', 'output', 'overwrite', 'selectdata', 'field', 'spw', 'timerange', 'uvrange', 'antenna', 'scan', 'correlation', 'observation', 'array', 'intent', 'feed', 'msselect', 'page_spec', 'preset', 'x_axis', 'y_axis', 'y_axis2', 'datacolumn', 'color_by', 'avgchannel', 'avgtime', 'avgscan', 'avgfield', 'avgbaseline', 'avgantenna', 'avgspw', 'scalar', 'freqframe', 'restfreq', 'veldef', 'iteraxis', 'gridrows', 'gridcols', 'xselfscale', 'yselfscale', 'xsharedaxis', 'ysharedaxis', 'title', 'xlabel', 'ylabel', 'symbol_size', 'showlegend', 'legendposition', 'showmajorgrid', 'showminorgrid', 'headeritems', 'max_points', 'plot_output', 'plot_format', 'flag_action', 'flag_xmin', 'flag_xmax', 'flag_ymin', 'flag_ymax', 'flag_plotindex', 'flag_panel', 'flag_extcorr', 'flag_extchannel', 'flag_selected', 'flag_apply', 'flag_output')))

def calibrate(
    *,
    mode: Literal['apply', 'export_corrected_data', 'fluxscale', 'gencal', 'solve_bandpass', 'solve_gain', 'stats', 'summary'] | object = _UNSET,
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    outputvis: StrPath | object = _UNSET,
    summary_paths: StrPath | list[StrPath] | tuple[StrPath, ...] | Literal['none'] | object = _UNSET,
    gaintables: StrPath | list[StrPath] | tuple[StrPath, ...] | Literal['none'] | object = _UNSET,
    table_path: StrPath | object = _UNSET,
    callib: StrPath | object = _UNSET,
    gainfield: str | object = _UNSET,
    interp: str | object = _UNSET,
    spwmap: str | object = _UNSET,
    apply_mode: Literal['calflag', 'calonly', 'trial'] | object = _UNSET,
    calwt: str | object = _UNSET,
    output: StrPath | object = _UNSET,
    overwrite: bool | object = _UNSET,
    parang: bool | object = _UNSET,
    format: Literal['json', 'text'] | object = _UNSET,
    stats_axis: Literal['amp', 'imag', 'phase', 'real'] | object = _UNSET,
    stats_datacolumn: str | object = _UNSET,
    use_flags: bool | object = _UNSET,
    out_table: StrPath | object = _UNSET,
    refant: str | object = _UNSET,
    gain_type: Literal['g', 't'] | object = _UNSET,
    solve_mode: Literal['ap', 'p'] | object = _UNSET,
    solint: str | object = _UNSET,
    gain_combine: str | object = _UNSET,
    gain_model_source: Literal['model-column', 'point'] | object = _UNSET,
    smodel: str | object = _UNSET,
    min_snr: float | object = _UNSET,
    bandpass_combine: Literal['field', 'none', 'scan', 'scan,field'] | object = _UNSET,
    bandtype: Literal['b', 'bpoly'] | object = _UNSET,
    solnorm: bool | object = _UNSET,
    fluxscale_input: StrPath | object = _UNSET,
    reference_fields: str | object = _UNSET,
    transfer_fields: str | object = _UNSET,
    refspwmap: str | object = _UNSET,
    gainthreshold: float | Literal['none'] | object = _UNSET,
    incremental: bool | object = _UNSET,
    selectdata: bool | object = _UNSET,
    field: str | object = _UNSET,
    spw: str | object = _UNSET,
    antenna: str | object = _UNSET,
    scan: str | object = _UNSET,
    observation: str | object = _UNSET,
    array: str | object = _UNSET,
    timerange: str | object = _UNSET,
    msselect: str | object = _UNSET,
    caltype: Literal['antpos', 'gceff', 'opac'] | Literal['none'] | object = _UNSET,
    parameter: str | object = _UNSET,
    gaincurve_table: StrPath | object = _UNSET,
    flagbackup: bool | object = _UNSET,
    minblperant: int | object = _UNSET,
    degamp: int | object = _UNSET,
    degphase: int | object = _UNSET,
) -> TaskCompletion:
    """apply, inspect, and solve CASA-style calibration workflows"""
    return _run("calibrate", _explicit(locals(), ('mode', 'vis', 'outputvis', 'summary_paths', 'gaintables', 'table_path', 'callib', 'gainfield', 'interp', 'spwmap', 'apply_mode', 'calwt', 'output', 'overwrite', 'parang', 'format', 'stats_axis', 'stats_datacolumn', 'use_flags', 'out_table', 'refant', 'gain_type', 'solve_mode', 'solint', 'gain_combine', 'gain_model_source', 'smodel', 'min_snr', 'bandpass_combine', 'bandtype', 'solnorm', 'fluxscale_input', 'reference_fields', 'transfer_fields', 'refspwmap', 'gainthreshold', 'incremental', 'selectdata', 'field', 'spw', 'antenna', 'scan', 'observation', 'array', 'timerange', 'msselect', 'caltype', 'parameter', 'gaincurve_table', 'flagbackup', 'minblperant', 'degamp', 'degphase')))

def importvla(
    archivefiles: StrPath | list[StrPath] | tuple[StrPath, ...] = [],
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    bandname: Literal['', '4', 'C', 'K', 'Ka', 'L', 'P', 'Q', 'S', 'U', 'X'] = '',
    frequencytol: str | int | float = _ImplicitStringDefault('150000.0Hz'),
    project: str = '',
    starttime: str = '',
    stoptime: str = '',
    applytsys: bool = True,
    autocorr: bool = False,
    antnamescheme: Literal['new', 'old'] = 'new',
    keepblanks: bool = False,
    evlabands: bool = False,
) -> Any:
    """scan or import old VLA export archives from disk

    CASA's ``importvla``: its parameters, defaults and return value.
    """
    return _call_casa(importvla, locals())

def tclean(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    selectdata: bool = True,
    field: str | list[str] | tuple[str, ...] = '',
    spw: str | list[str] | tuple[str, ...] = '',
    timerange: str | list[str] | tuple[str, ...] = '',
    uvrange: str | list[str] | tuple[str, ...] = '',
    antenna: str | list[str] | tuple[str, ...] = '',
    scan: str | list[str] | tuple[str, ...] = '',
    observation: str = '',
    intent: str | list[str] | tuple[str, ...] = '',
    datacolumn: str = 'corrected',
    imagename: StrPath = '',
    imsize: int | list[int] | tuple[int, ...] = [100],
    cell: str | list[str] | tuple[str, ...] = [],
    phasecenter: str = '',
    stokes: str = 'I',
    projection: str = 'SIN',
    startmodel: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    specmode: Literal['cont', 'cube', 'cubedata', 'cubesource', 'mfs', 'mvc'] = 'mfs',
    reffreq: str = '',
    nchan: int = -1,
    start: str = '',
    width: str = '',
    outframe: str = 'LSRK',
    veltype: str = 'radio',
    restfreq: str | list[str] | tuple[str, ...] = [],
    interpolation: Literal['cubic', 'linear', 'nearest'] = 'linear',
    perchanweightdensity: bool = True,
    gridder: Literal['awp2', 'awphpg', 'awproject', 'awprojectft', 'ft', 'ftmosaic', 'gridft', 'imagemosaic', 'mosaic', 'mosaicft', 'standard', 'widefield', 'wproject', 'wprojectft'] = 'standard',
    facets: int = 1,
    psfphasecenter: str = '',
    wprojplanes: int = 1,
    vptable: str = '',
    mosweight: bool = True,
    aterm: bool = True,
    psterm: bool = False,
    wbawp: bool = True,
    conjbeams: bool = False,
    cfcache: StrPath = '',
    usepointing: bool = False,
    computepastep: float = 360.0,
    rotatepastep: float = 360.0,
    pointingoffsetsigdev: float | list[float] | tuple[float, ...] = [],
    pblimit: float = 0.2,
    normtype: Literal['flatnoise', 'flatsky', 'pbsquare'] = 'flatnoise',
    deconvolver: Literal['asp', 'clark', 'clark_exp', 'clarkstokes', 'clarkstokes_exp', 'hogbom', 'mem', 'mtmfs', 'multiscale'] = 'hogbom',
    scales: float | list[float] | tuple[float, ...] = [],
    nterms: int = 2,
    smallscalebias: float = 0.0,
    fusedthreshold: float = 0.0,
    largestscale: int = -1,
    restoration: bool = True,
    restoringbeam: str | list[str] | tuple[str, ...] = [],
    pbcor: bool = False,
    outlierfile: StrPath = '',
    weighting: Literal['briggs', 'briggsabs', 'briggsbwtaper', 'natural', 'radial', 'superuniform', 'uniform'] = 'natural',
    robust: float = 0.5,
    noise: str = '1.0Jy',
    npixels: int = 0,
    uvtaper: str | list[str] | tuple[str, ...] = [''],
    niter: int = 0,
    gain: float = 0.1,
    threshold: str = 0.0,
    nsigma: float = 0.0,
    cycleniter: int = -1,
    cyclefactor: float = 1.0,
    minpsffraction: float = 0.05,
    maxpsffraction: float = 0.8,
    interactive: bool = False,
    nmajor: int = -1,
    fullsummary: bool = False,
    usemask: Literal['auto-multithresh', 'pb', 'user'] = 'user',
    mask: str | list[str] | tuple[str, ...] = '',
    pbmask: float = 0.0,
    sidelobethreshold: float = 3.0,
    noisethreshold: float = 5.0,
    lownoisethreshold: float = 1.5,
    negativethreshold: float = 0.0,
    smoothfactor: float = 1.0,
    minbeamfrac: float = 0.3,
    cutthreshold: float = 0.01,
    growiterations: int = 75,
    dogrowprune: bool = True,
    minpercentchange: float = -1.0,
    verbose: bool = False,
    fastnoise: bool = True,
    restart: bool = True,
    savemodel: Literal['modelcolumn', 'none', 'virtual'] = 'none',
    calcres: bool = True,
    calcpsf: bool = True,
    psfcutoff: float = 0.35,
    parallel: bool = False,
    *,
    ddid: str = 'none',
    channel_start: int | Literal['none'] = 'none',
    channel_count: int | Literal['none'] = 'none',
    hogbom_iteration_mode: str = 'casa-inclusive',
    write_pb: bool = True,
    fitspw: str = 'none',
    fitorder: int = 0,
    save_continuum_residual: bool = False,
    backend: Literal['auto', 'cpu', 'gpu'] = 'auto',
    gridprecision: Literal['auto', 'f32', 'f64'] = 'auto',
    aw_cf_source: Literal['casa-import', 'native-evla'] = 'casa-import',
    cf_resident_mb: int | Literal['auto'] = 'auto',
    native_cf_cache: StrPath | Literal['none'] = 'none',
    evla_surface: StrPath | Literal['none'] = 'none',
    native_cf_policy: Literal['reuse-only', 'generate-missing', 'regenerate'] = 'reuse-only',
    native_cf_working_size: int | Literal['none'] = 'none',
    native_cf_oversampling: int | Literal['none'] = 'none',
    native_cf_cache_bytes: int | Literal['none'] = 'none',
    native_cf_maximum_cells: int | Literal['none'] = 'none',
    stage_timing: bool = False,
    metal_stage_timing: bool = False,
    science_trace: bool = False,
) -> Any:
    """Make dirty and deconvolved images of a MeasurementSet, as CASA's tclean does.

    CASA's ``tclean``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #980:
    ``interactive``.
    Accepted only at CASA's default, where CASA reads them, until #1070:
    ``computepastep``, ``rotatepastep``.
    """
    return _call_casa(tclean, locals())

def simobserve(
    project: str = 'sim',
    skymodel: StrPath = '',
    inbright: str = '',
    indirection: str = '',
    incell: str = '',
    incenter: str = '',
    inwidth: str = '',
    complist: StrPath = '',
    compwidth: str = '"8GHz"',
    comp_nchan: int = 1,
    setpointings: bool = True,
    ptgfile: StrPath = '$project.ptg.txt',
    integration: str = '10s',
    direction: str | list[str] | tuple[str, ...] = [],
    mapsize: str | list[str] | tuple[str, ...] = ['', ''],
    maptype: Literal['ALMA', 'ALMA-OT', 'ALMA2012', 'alma', 'hex', 'hexagonal', 'square'] = 'hexagonal',
    pointingspacing: str = '',
    caldirection: str = '',
    calflux: str = '1Jy',
    obsmode: Literal['', 'int', 'sd'] = 'int',
    refdate: str = '2014/01/01',
    hourangle: str = 'transit',
    totaltime: str = '7200s',
    antennalist: str = '',
    sdantlist: str = 'aca.tp.cfg',
    sdant: int = 0,
    outframe: str = 'LSRK',
    thermalnoise: Literal['', 'tsys-atm', 'tsys-manual'] = 'tsys-atm',
    user_pwv: float = 0.5,
    t_ground: float = 270.0,
    t_sky: float = 260.0,
    tau0: float = 0.1,
    seed: int = 11111,
    leakage: float = 0.0,
    correlator: Literal['', 'ACA', 'ACASpec', 'BLC', 'BLC_4x4bit'] = '',
    graphics: Literal['', 'both', 'file', 'none', 'screen'] = 'both',
    verbose: bool = False,
    overwrite: bool = True,
) -> Any:
    """Simulate an observation of a sky model: CASA's simobserve

    CASA's ``simobserve``: its parameters, defaults and return value.
    """
    return _call_casa(simobserve, locals())

def imhead(
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    mode: Literal['add', 'del', 'get', 'history', 'list', 'put', 'summary'] = 'summary',
    hdkey: str = '',
    hdvalue: str | bool | int | float | dict[str, ParameterData] | list[ParameterData] = '',
    verbose: bool = False,
) -> Any:
    """Inspect or update CASA image header metadata.

    CASA's ``imhead``: its parameters, defaults and return value.
    """
    return _call_casa(imhead, locals())

def imstat(
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    axes: int | list[int] | tuple[int, ...] = [],
    region: str = '',
    box: str = '',
    chans: str = '',
    stokes: str = '',
    listit: bool = True,
    verbose: bool = True,
    mask: str = '',
    stretch: bool = False,
    logfile: StrPath = '',
    append: bool = True,
    algorithm: str = 'classic',
    fence: float = -1.0,
    center: str = 'mean',
    lside: bool = True,
    zscore: float = -1.0,
    maxiter: int = -1,
    clmethod: str = 'auto',
    niter: int = 3,
) -> Any:
    """Compute CASA image statistics over optional pixel, region, and channel selections.

    CASA's ``imstat``: its parameters, defaults and return value.
    """
    return _call_casa(imstat, locals())

def immoments(
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    moments: int | list[int] | tuple[int, ...] = [0],
    axis: int | str = 'spectral',
    region: str = '',
    box: str = '',
    chans: str = '',
    stokes: str = '',
    mask: str = '',
    includepix: float | list[float] | tuple[float, ...] = -1,
    excludepix: float | list[float] | tuple[float, ...] = -1,
    outfile: StrPath = '',
    stretch: bool = False,
    *,
    overwrite: bool = False,
) -> Any:
    """Create CASA-style image moment maps

    CASA's ``immoments``: its parameters, defaults and return value.
    """
    return _call_casa(immoments, locals())

def exportfits(
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    fitsimage: StrPath = '',
    velocity: bool = False,
    optical: bool = False,
    bitpix: int = -32,
    minpix: float = 0,
    maxpix: float = -1,
    overwrite: bool = False,
    dropstokes: bool = False,
    stokeslast: bool = True,
    history: bool = True,
    dropdeg: bool = False,
) -> Any:
    """Export CASA images to FITS

    CASA's ``exportfits``: its parameters, defaults and return value.
    """
    return _call_casa(exportfits, locals())

def mstransform(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    outputvis: StrPath = '',
    createmms: bool = False,
    separationaxis: str = 'auto',
    numsubms: str | int = 'auto',
    tileshape: list[int] | tuple[int, ...] = [0],
    field: str = '',
    spw: str = '',
    scan: str = '',
    antenna: str = '',
    correlation: str = '',
    timerange: str = '',
    intent: str = '',
    array: str = '',
    uvrange: str = '',
    observation: str | int = '',
    feed: str = '',
    datacolumn: str = 'corrected',
    realmodelcol: bool = False,
    keepflags: bool = True,
    usewtspectrum: bool = False,
    combinespws: bool = False,
    chanaverage: bool = False,
    chanbin: int | list[int] | tuple[int, ...] = 1,
    hanning: bool = False,
    regridms: bool = False,
    mode: str = 'channel',
    nchan: int = -1,
    start: ParameterData = 0,
    width: ParameterData = 1,
    nspw: int = 1,
    interpolation: str = 'linear',
    phasecenter: ParameterData = '',
    restfreq: str = '',
    outframe: str = '',
    veltype: str = 'radio',
    preaverage: bool = False,
    timeaverage: bool = False,
    timebin: str = '0s',
    timespan: str | list[str] | tuple[str, ...] = '',
    maxuvwdistance: float = 0.0,
    docallib: bool = False,
    callib: StrPath = '',
    douvcontsub: bool = False,
    fitspw: str = '',
    fitorder: int = 0,
    want_cont: bool = False,
    denoising_lib: bool = True,
    nthreads: int = 1,
    niter: int = 1,
    disableparallel: bool = False,
    ddistart: int = -1,
    taql: str = '',
    monolithic_processing: bool = False,
    reindex: bool = True,
) -> Any:
    """Materialize a selected MeasurementSet into a new output MS.

    CASA's ``mstransform``: its parameters, defaults and return value.
    """
    return _call_casa(mstransform, locals())

def split(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    outputvis: StrPath = '',
    keepmms: bool = True,
    field: str | int = '',
    spw: str | int = '',
    scan: str | int = '',
    antenna: str | int = '',
    correlation: str | list[str] = '',
    timerange: str = '',
    intent: str = '',
    array: str | int = '',
    uvrange: str = '',
    observation: str | int = '',
    feed: str | int = '',
    datacolumn: str = 'corrected',
    keepflags: bool = True,
    width: int | str | list[int] | tuple[int, ...] = 1,
    timebin: str = '0s',
    combine: str | list[str] | tuple[str, ...] = '',
    *,
    msselect: str = '',
) -> Any:
    """Create a selected MeasurementSet subset, equivalent to CASA split.

    CASA's ``split``: its parameters, defaults and return value.
    """
    return _call_casa(split, locals())

def applycal(
    vis: StrPath = '',
    field: str = '',
    spw: str = '',
    intent: str = '',
    selectdata: bool = True,
    timerange: str = '',
    uvrange: str | int | float | list[str] = '',
    antenna: str = '',
    scan: str = '',
    observation: str | int = '',
    msselect: str = '',
    docallib: bool = False,
    callib: str = '',
    gaintable: StrPath | list[StrPath] | tuple[StrPath, ...] = [],
    gainfield: str | list[str] | tuple[str, ...] = [],
    interp: str | list[str] | tuple[str, ...] = [],
    spwmap: list[int] | list[list[int]] = [],
    calwt: bool | list[bool] | tuple[bool, ...] = [True],
    parang: bool = False,
    applymode: Literal['', 'calflag', 'calflagstrict', 'calonly', 'flagonly', 'flagonlystrict', 'trial'] = '',
    flagbackup: bool = True,
    *,
    output: StrPath = '',
    overwrite: bool = False,
    format: Literal['json', 'text'] = 'text',
    array: str = '',
) -> Any:
    """Apply one or more calibration tables to a MeasurementSet.

    CASA's ``applycal``: its parameters, defaults and return value.
    """
    return _call_casa(applycal, locals())

def gaincal(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    caltable: StrPath = '',
    field: str = '',
    spw: str = '',
    intent: str = '',
    selectdata: bool = True,
    timerange: str = '',
    uvrange: str | int | float | list[str] = '',
    antenna: str = '',
    scan: str = '',
    observation: str | int = '',
    msselect: str = '',
    solint: str | int | float = 'inf',
    combine: str = '',
    preavg: float = -1.0,
    refant: str = '',
    refantmode: Literal['flex', 'strict'] = 'flex',
    minblperant: int = 4,
    minsnr: float = 3.0,
    solnorm: bool = False,
    normtype: Literal['mean', 'median'] = 'mean',
    gaintype: Literal['G', 'GSPLINE', 'K', 'KCROSS', 'T', 'XY+QU', 'XYf+QU'] = 'G',
    smodel: float | list[float] | tuple[float, ...] = [],
    calmode: Literal['a', 'ap', 'p'] = 'ap',
    solmode: Literal['', 'L1', 'L1R', 'R'] = '',
    rmsthresh: float | list[float] | tuple[float, ...] = [],
    corrdepflags: bool = False,
    append: bool = False,
    splinetime: float = 3600.0,
    npointaver: int = 3,
    phasewrap: float = 180.0,
    docallib: bool = False,
    callib: StrPath = '',
    gaintable: StrPath | list[StrPath] | tuple[StrPath, ...] = [],
    gainfield: str | list[str] | tuple[str, ...] = [],
    interp: str | list[str] | tuple[str, ...] = [],
    spwmap: list[list[int] | tuple[int, ...]] | tuple[list[int] | tuple[int, ...], ...] = [],
    parang: bool = False,
    *,
    output: StrPath = '',
    overwrite: bool = False,
    format: Literal['json', 'text'] = 'text',
    array: str = '',
) -> Any:
    """Solve antenna gain calibration for a MeasurementSet.

    CASA's ``gaincal``: its parameters, defaults and return value.
    """
    return _call_casa(gaincal, locals())

def accor(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    caltable: StrPath = '',
    field: str = '',
    spw: str = '',
    intent: str = '',
    selectdata: bool = True,
    timerange: str = '',
    antenna: str = '',
    scan: str = '',
    observation: str | int = '',
    msselect: str = '',
    solint: str | int | float = 'inf',
    combine: str = '',
    corrdepflags: bool = False,
    append: bool = False,
    docallib: bool = False,
    callib: StrPath = '',
    gaintable: StrPath | list[StrPath] | tuple[StrPath, ...] = [],
    gainfield: str | list[str] | tuple[str, ...] = [],
    interp: str | list[str] | tuple[str, ...] = [],
    spwmap: list[list[int] | tuple[int, ...]] | tuple[list[int] | tuple[int, ...], ...] = [],
    *,
    output: StrPath = '',
    overwrite: bool = False,
    format: Literal['json', 'text'] = 'text',
) -> Any:
    """Normalize sampler amplitudes using antenna autocorrelations.

    CASA's ``accor``: its parameters, defaults and return value.
    """
    return _call_casa(accor, locals())

def blcal(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    caltable: StrPath = '',
    field: str = '',
    spw: str = '',
    intent: str = '',
    selectdata: bool = True,
    timerange: str = '',
    uvrange: str | int | float | list[str] = '',
    antenna: str = '',
    scan: str = '',
    observation: str | int = '',
    msselect: str = '',
    solint: str | int | float = 'inf',
    combine: str = 'scan',
    freqdep: bool = False,
    calmode: Literal['a', 'ap', 'p'] = 'ap',
    solnorm: bool = False,
    gaintable: StrPath | list[StrPath] | tuple[StrPath, ...] = [],
    gainfield: str | list[str] | tuple[str, ...] = [],
    interp: str | list[str] | tuple[str, ...] = [],
    spwmap: list[list[int] | tuple[int, ...]] | tuple[list[int] | tuple[int, ...], ...] = [],
    parang: bool = False,
    *,
    output: StrPath = '',
    overwrite: bool = False,
    format: Literal['json', 'text'] = 'text',
) -> Any:
    """Solve per-baseline complex visibility calibration.

    CASA's ``blcal``: its parameters, defaults and return value.
    """
    return _call_casa(blcal, locals())

def fringefit(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    caltable: StrPath = '',
    field: str = '',
    spw: str = '',
    intent: str = '',
    selectdata: bool = True,
    timerange: str = '',
    uvrange: str | int | float | list[str] = '',
    antenna: str = '',
    scan: str = '',
    observation: str | int = '',
    msselect: str = '',
    solint: str | int | float = 'inf',
    combine: str = '',
    refant: str = '',
    minsnr: float = 3.0,
    zerorates: bool = False,
    globalsolve: bool = True,
    niter: int = 100,
    delaywindow: float | list[float] | tuple[float, ...] = [],
    ratewindow: float | list[float] | tuple[float, ...] = [],
    append: bool = False,
    corrdepflags: bool = False,
    corrcomb: Literal['none', 'stokes', 'parallel'] = 'none',
    docallib: bool = False,
    callib: StrPath = '',
    gaintable: StrPath | list[StrPath] | tuple[StrPath, ...] = [],
    gainfield: str | list[str] | tuple[str, ...] = [],
    interp: str | list[str] | tuple[str, ...] = [],
    spwmap: list[list[int] | tuple[int, ...]] | tuple[list[int] | tuple[int, ...], ...] = [],
    paramactive: bool | list[bool] | tuple[bool, ...] = [],
    concatspws: bool = True,
    parang: bool = False,
    *,
    output: StrPath = '',
    overwrite: bool = False,
    format: Literal['json', 'text'] = 'text',
    array: str = '',
) -> Any:
    """Fit antenna phases, delays and delay rates to VLBI visibilities.

    CASA's ``fringefit``: its parameters, defaults and return value.
    """
    return _call_casa(fringefit, locals())

def pccor(
    vis: str = '',
    pccor_caltable: str = '',
    refant: str = '',
    timerange: str = 'none',
    scan: str = 'none',
    spw: str = 'none',
    antenna: str = 'none',
    cablecal_correction: bool = False,
    ff_table: str = 'none',
) -> Any:
    """Align VLBI pulse and cable calibration to a reference fringe solution.

    CASA's ``pccor``: its parameters, defaults and return value.
    """
    return _call_casa(pccor, locals())

def appendantab(
    vis: str = '',
    outvis: str = '',
    antab: str = '',
    overwrite: bool = False,
    append_tsys: bool = True,
    append_gc: bool = True,
) -> Any:
    """Import ANTAB temperatures and gain curves into a copied MeasurementSet.

    CASA's ``appendantab``: its parameters, defaults and return value.
    """
    return _call_casa(appendantab, locals())

def bandpass(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    caltable: StrPath = '',
    field: str = '',
    spw: str = '',
    intent: str = '',
    selectdata: bool = True,
    timerange: str = '',
    uvrange: str | int | float | list[str] = '',
    antenna: str = '',
    scan: str = '',
    observation: str | int = '',
    msselect: str = '',
    solint: str | int | float = 'inf',
    combine: str = 'scan',
    refant: str = '',
    minblperant: int = 4,
    minsnr: float = 3.0,
    solnorm: bool = False,
    bandtype: Literal['B', 'BPOLY'] = 'B',
    smodel: float | list[float] | tuple[float, ...] = [],
    corrdepflags: bool = False,
    append: bool = False,
    fillgaps: int = 0,
    degamp: int = 3,
    degphase: int = 3,
    visnorm: bool = False,
    maskcenter: int = 0,
    maskedge: int = 5,
    docallib: bool = False,
    callib: StrPath = '',
    gaintable: StrPath | list[StrPath] | tuple[StrPath, ...] = [],
    gainfield: str | list[str] | tuple[str, ...] = [],
    interp: str | list[str] | tuple[str, ...] = [],
    spwmap: list[list[int] | tuple[int, ...]] | tuple[list[int] | tuple[int, ...], ...] = [],
    parang: bool = False,
    *,
    output: StrPath = '',
    overwrite: bool = False,
    format: Literal['json', 'text'] = 'text',
    array: str = '',
) -> Any:
    """Solve bandpass calibration for a MeasurementSet.

    CASA's ``bandpass``: its parameters, defaults and return value.
    """
    return _call_casa(bandpass, locals())

def fluxscale(
    vis: StrPath = '',
    caltable: StrPath = '',
    fluxtable: StrPath = '',
    reference: str | list[str] | tuple[str, ...] = [],
    transfer: str | list[str] | tuple[str, ...] = [],
    listfile: str = '',
    append: bool = False,
    refspwmap: int | list[int] | tuple[int, ...] = [-1],
    gainthreshold: float = -1.0,
    antenna: str = '',
    timerange: str = '',
    scan: str = '',
    incremental: bool = False,
    fitorder: int = 1,
    display: bool = False,
    *,
    output: StrPath = '',
    overwrite: bool = False,
    format: Literal['json', 'text'] = 'text',
) -> Any:
    """Scale gain solutions using reference calibrator fields.

    CASA's ``fluxscale``: its parameters, defaults and return value.
    """
    return _call_casa(fluxscale, locals())

def uvsub(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    reverse: bool = False,
) -> Any:
    """Subtract or restore the current visibility model in corrected data.

    CASA's ``uvsub``: its parameters, defaults and return value.
    """
    return _call_casa(uvsub, locals())

def ft(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    field: str = '',
    spw: str = '',
    timerange: str = '',
    uvrange: str = '',
    antenna: str = '',
    scan: str = '',
    observation: str = '',
    intent: str = '',
    model: str | list[str] = '',
    nterms: int = 1,
    reffreq: str = '',
    complist: StrPath = '',
    incremental: bool = False,
    usescratch: bool = False,
) -> Any:
    """Predict model visibilities from component lists and model images.

    CASA's ``ft``: its parameters, defaults and return value.
    """
    return _call_casa(ft, locals())

def setjy(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    field: str = '',
    spw: str = '',
    selectdata: bool = False,
    timerange: str | list[str] | tuple[str, ...] = '',
    scan: str | list[str] | tuple[str, ...] = '',
    intent: str = '',
    observation: str | int = '',
    scalebychan: bool = True,
    standard: Literal['Perley-Taylor 95', 'Butler-JPL-Horizons 2010', 'Perley-Butler 2010', 'Stevens-Reynolds 2016', 'Perley-Taylor 99', 'Perley-Butler 2013', 'Perley 90', 'Scaife-Heald 2012', 'manual', 'Butler-JPL-Horizons 2012', 'Perley-Butler 2017', 'fluxscale', 'Baars'] = 'Perley-Butler 2017',
    model: str = '',
    listmodels: bool = False,
    fluxdensity: float | list[float] | tuple[float, ...] = -1,
    spix: float | list[float] | tuple[float, ...] = 0.0,
    reffreq: str = '1GHz',
    polindex: list[float] | tuple[float, ...] = [],
    polangle: list[float] | tuple[float, ...] = [],
    rotmeas: float = 0.0,
    fluxdict: Mapping[str, ParameterData] = {},
    useephemdir: bool = False,
    interpolation: Literal['nearest', 'linear', 'spline', '', 'cubic'] = 'nearest',
    usescratch: bool = True,
    ismms: bool = False,
) -> Any:
    """Populate a calibrator visibility model with setjy.

    CASA's ``setjy``: its parameters, defaults and return value.
    """
    return _call_casa(setjy, locals())

def uvmodelfit(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    field: str = '',
    spw: str = '',
    selectdata: bool = True,
    timerange: str = '',
    uvrange: str | int | float | list[str] = '',
    antenna: str = '',
    scan: str = '',
    msselect: str = '',
    niter: int = 5,
    comptype: Literal['P', 'G', 'D'] = 'P',
    sourcepar: float | list[float] | tuple[float, ...] = [1.0, 0.0, 0.0],
    varypar: bool | list[bool] | tuple[bool, ...] = [],
    outfile: StrPath = '',
) -> Any:
    """Fit a single sky component to selected visibilities.

    CASA's ``uvmodelfit``: its parameters, defaults and return value.
    """
    return _call_casa(uvmodelfit, locals())

def clearcal(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    field: str = '',
    spw: str = '',
    intent: str = '',
    addmodel: bool = False,
) -> Any:
    """Initialize or reset scratch visibility columns.

    CASA's ``clearcal``: its parameters, defaults and return value.
    """
    return _call_casa(clearcal, locals())

def delmod(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    otf: bool = True,
    field: str = '',
    scr: bool = False,
) -> Any:
    """Remove scratch or virtual model visibilities.

    CASA's ``delmod``: its parameters, defaults and return value.
    """
    return _call_casa(delmod, locals())

def gencal(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    caltable: StrPath = '',
    caltype: Literal['', 'amp', 'antpos', 'antposvla', 'eff', 'eop', 'evlagain', 'gc', 'gceff', 'jyperk', 'mbd', 'opac', 'ph', 'rq', 'sbd', 'swp/rq', 'swpow', 'swpwts', 'tecim', 'tsys'] = '',
    infile: str = '',
    endpoint: str = 'asdm',
    timeout: int = 180,
    retry: int = 3,
    retry_wait_time: int = 5,
    ant_pos_time_limit: int = 0,
    spw: str = '',
    antenna: str = '',
    pol: str = '',
    parameter: float | list[float] | tuple[float, ...] = [],
    uniform: bool = True,
    *,
    output: StrPath = '',
    overwrite: bool = False,
    format: Literal['json', 'text'] = 'text',
    gaincurve_table: StrPath = '',
) -> Any:
    """Generate a calibration table such as antpos, gceff, or opac.

    CASA's ``gencal``: its parameters, defaults and return value.
    """
    return _call_casa(gencal, locals())

def plotms(
    vis: StrPath = '',
    caltable: StrPath = '',
    gridrows: int = 1,
    gridcols: int = 1,
    rowindex: int = 0,
    colindex: int = 0,
    plotindex: int = 0,
    xaxis: Literal['parangle', 'baseline', 'delay', 'antenna position', 'swp', 'u', 'tec', 'ant-parang', 'gamp', 'Antenna1', 'UVdist', 'WeightSpectrum', 'Baseline', 'flag', 'Real', 'switchedpower', 'timeint', 'WtSp', 'Ant-Elevation', 'GainReal', 'time_interval', 'uvdist_l', 'GainAmp', 'ant1', 'flagrow', 'Wt', 'Velocity', 'opac', 'Feed2', 'corr', 'imaginary', 'gainamp', 'Vwave', 'wwave', 'Frequency', 'observation', 'feed1', 'ant-azimuth', 'rate', 'Elevation', 'Intent', 'Row', 'swpower', 'antpos', 'ant-elevation', 'antenna', 'v', 'V', 'interval', 'sigmaspectrum', 'DelayRate', 'tsys', 'gainphase', 'Ant-Parangle', 'imag', 'Radial Velocity', 'UVwave', 'Observation', 'Poln', 'W', 'timeinterval', 'Channel', 'snr', 'SNR', 'frequency', 'Interval', 'uvdistl', 'disp', 'Imag', 'dispdelay', 'Feed1', 'rho', 'Ant-RA', 'delayrate', 'spgain', 'Antenna', 'Uwave', 'SigmaSpectrum', 'Azimuth', 'Scan', 'Tsys', 'polarization', 'intent', 'phase', 'amp', 'ant-parangle', 'SigmaSp', 'freq', 'wt*amp', 'channel', 'uwave', 'Flag', 'correlation', 'FlagRow', 'TEC', 'SwPower', 'GainPhase', 'GainImag', 'ant-parallacticangle', 'real', 'Time', 'weight', 'antenna2', 'gainimag', 'gimag', 'Antenna2', 'spw', 'gphase', 'Opac', 'parang', 'Amp', 'ant-ra', 'Sigma', 'amplitude', 'Ant-DEC', 'HourAngle', 'U', 'row', 'Wt*Amp', 'gainreal', 'chan', 'Field', 'DispDelay', 'hourang', 'vwave', 'radialvelocity', 'sigma', 'Antenna Position', 'Delay', 'uvwave', 'vel', 'uvdist', 'velocity', 'hourangle', 'poln', 'ant2', 'Phase', 'azimuth', 'Corr', 'parallacticangle', 'weightspectrum', 'Ant-Azimuth', 'wtsp', '', 'Distance', 'opacity', 'antenna1', 'feed2', 'sigmasp', 'time', 'w', 'wt', 'field', 'greal', 'elevation', 'del', 'ant', 'ant-dec', 'Spw', 'scan', 'Wwave', 'ParAngle'] = '',
    xdatacolumn: Literal['model', 'corrected', 'data-model_vector', 'data-model', 'corrected/model_vector', 'residual', 'corrected-model', 'data/model', 'data/model_scalar', 'float', 'corrected/model_scalar', 'corrected/model', '', 'corrected-model_vector', 'data-model_scalar', 'corrected-model_scalar', 'data', 'data/model_vector'] = '',
    xframe: Literal['icrs', 'azelgeo', 'b1950', 'galactic', '', 'j2000'] = '',
    xinterp: Literal['', 'nearest', 'cubic spline', 'spline'] = '',
    yaxis: str | list[str] | tuple[str, ...] = '',
    ydatacolumn: str | list[str] | tuple[str, ...] = '',
    yframe: str | list[str] | tuple[str, ...] = '',
    yinterp: str | list[str] | tuple[str, ...] = '',
    yaxislocation: str | list[str] | tuple[str, ...] = '',
    selectdata: bool = True,
    field: str = '',
    spw: str = '',
    timerange: str = '',
    uvrange: str = '',
    antenna: str = '',
    scan: str = '',
    correlation: str = '',
    polarization: str = '',
    antpos: str = '',
    array: str = '',
    observation: str = '',
    intent: str = '',
    feed: str = '',
    msselect: str = '',
    averagedata: bool = True,
    avgchannel: str = '',
    avgtime: str = '',
    avgscan: bool = False,
    avgfield: bool = False,
    avgbaseline: bool = False,
    avgantenna: bool = False,
    avgspw: bool = False,
    scalar: bool = False,
    transform: bool = True,
    freqframe: Literal['LSRK', 'CMB', 'GALACTO', 'GEO', 'TOPO', 'LSRD', 'LGROUP', 'BARY', ''] = '',
    restfreq: str = '',
    veldef: Literal['RADIO', 'OPTICAL', 'TRUE'] = 'RADIO',
    phasecenter: str = '',
    extendflag: bool = False,
    extcorr: bool = False,
    extchannel: bool = False,
    iteraxis: Literal['baseline', 'Baseline', 'corr', 'antpos', 'antenna', 'Poln', 'AntPos', 'Antenna', 'Scan', 'polarization', 'correlation', 'Time', 'spw', 'Field', 'poln', 'Corr', '', 'time', 'field', 'Spw', 'scan'] = '',
    xselfscale: bool = False,
    yselfscale: bool = False,
    xsharedaxis: bool = False,
    ysharedaxis: bool = False,
    customsymbol: bool | list[bool] | tuple[bool, ...] = [],
    symbolshape: str | list[str] | tuple[str, ...] = 'autoscaling',
    symbolsize: int | list[int] | tuple[int, ...] = 2,
    symbolcolor: str | list[str] | tuple[str, ...] = '0000ff',
    symbolfill: str | list[str] | tuple[str, ...] = 'fill',
    symboloutline: bool | list[bool] | tuple[bool, ...] = False,
    coloraxis: Literal['baseline', 'Antenna1', 'Baseline', 'ant1', 'corr', 'observation', 'Intent', 'antpos', 'Observation', 'Poln', 'Channel', 'AntPos', 'Scan', 'polarization', 'intent', 'channel', 'correlation', 'Time', 'antenna2', 'Antenna2', 'spw', 'chan', 'Field', 'poln', 'ant2', 'Corr', '', 'antenna1', 'time', 'field', 'Spw', 'scan'] = '',
    customflaggedsymbol: bool | list[bool] | tuple[bool, ...] = False,
    flaggedsymbolshape: str | list[str] | tuple[str, ...] = 'circle',
    flaggedsymbolsize: int | list[int] | tuple[int, ...] = 2,
    flaggedsymbolcolor: str | list[str] | tuple[str, ...] = 'ff0000',
    flaggedsymbolfill: str | list[str] | tuple[str, ...] = 'fill',
    flaggedsymboloutline: bool | list[bool] | tuple[bool, ...] = False,
    xconnector: Literal['', 'none', 'line', 'step'] = '',
    timeconnector: bool = False,
    plotrange: float | list[float] | tuple[float, ...] = [],
    title: str = '',
    titlefont: int = 0,
    xlabel: str = '',
    xaxisfont: int = 0,
    ylabel: str = '',
    yaxisfont: int = 0,
    showmajorgrid: bool = False,
    majorwidth: int = 1,
    majorstyle: Literal['none', 'dot', 'solid', 'dash', ''] = '',
    majorcolor: str = 'B0B0B0',
    showminorgrid: bool = False,
    minorwidth: int = 1,
    minorstyle: Literal['none', 'dot', 'solid', 'dash', ''] = '',
    minorcolor: str = 'D0D0D0',
    showlegend: bool = False,
    legendposition: Literal['exteriorRight', 'lowerRight', 'exteriorTop', 'exteriorLeft', 'lowerLeft', 'upperRight', 'upperLeft', 'exteriorBottom', ''] = '',
    plotfile: StrPath = '',
    expformat: Literal['pdf', 'jpg', 'png', 'txt', 'ps', ''] = '',
    verbose: bool = True,
    exprange: Literal['', 'current', 'all'] = '',
    highres: bool = False,
    dpi: int = -1,
    width: int = -1,
    height: int = -1,
    overwrite: bool = False,
    showgui: bool = True,
    clearplots: bool = True,
    callib: str | list[str] | tuple[str, ...] = [],
    headeritems: str = '',
    showatm: bool = False,
    showtsky: bool = False,
    showimage: bool = False,
    colorizeoverlay: bool = False,
) -> Any:
    """Plot selected MeasurementSet or calibration table data.

    CASA's ``plotms``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #1032:
    ``plotindex``, ``extendflag``, ``extcorr``, ``extchannel``,
    ``clearplots``.
    """
    return _call_casa(plotms, locals())

def flagdata(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    mode: Literal['antint', 'clip', 'elevation', 'extend', 'list', 'manual', 'manualflag', 'quack', 'rflag', 'shadow', 'summary', 'tfcrop', 'unflag'] = 'manual',
    autocorr: bool = False,
    inpfile: str | list[str] = '',
    reason: str | list[str] = 'any',
    tbuff: float | list[float] = 0.0,
    spw: str | list[str] = '',
    field: str | list[str] = '',
    antenna: str | list[str] = '',
    uvrange: str | list[str] = '',
    timerange: str | list[str] = '',
    correlation: str = '',
    scan: str | list[str] = '',
    intent: str | list[str] = '',
    array: str | list[str] = '',
    observation: str | int = '',
    feed: str | list[str] = '',
    clipminmax: float | list[float] | tuple[float, ...] = [],
    datacolumn: ParameterData = 'DATA',
    clipoutside: bool | list[bool] = True,
    channelavg: bool | list[bool] = False,
    chanbin: int | list[int] = 1,
    timeavg: bool | list[bool] = False,
    timebin: str = '0s',
    clipzeros: bool = False,
    quackinterval: float | list[float] = 1.0,
    quackmode: str | list[str] = 'beg',
    quackincrement: bool | list[bool] = False,
    tolerance: float = 0.0,
    addantenna: str | dict[str, ParameterData] = '',
    lowerlimit: ParameterData = 0.0,
    upperlimit: ParameterData = 90.0,
    ntime: str | float = 'scan',
    combinescans: bool = False,
    timecutoff: float = 4.0,
    freqcutoff: float = 3.0,
    timefit: str = 'line',
    freqfit: str = 'poly',
    maxnpieces: int = 7,
    flagdimension: str = 'freqtime',
    usewindowstats: str = 'none',
    halfwin: int = 1,
    extendflags: bool = True,
    winsize: ParameterData = 3,
    timedev: ParameterData | Literal['', 'none'] = '',
    freqdev: ParameterData | Literal['', 'none'] = '',
    timedevscale: float = 5.0,
    freqdevscale: float = 5.0,
    spectralmax: float = 1000000.0,
    spectralmin: float = 0.0,
    antint_ref_antenna: str = '',
    minchanfrac: float = 0.6,
    verbose: bool = False,
    extendpols: ParameterData = True,
    growtime: float = 50.0,
    growfreq: float = 50.0,
    growaround: bool = False,
    flagneartime: bool = False,
    flagnearfreq: bool = False,
    minrel: float = 0.0,
    maxrel: float = 1.0,
    minabs: int = 0,
    maxabs: int = -1,
    spwchan: bool = False,
    spwcorr: bool = False,
    basecnt: bool = False,
    fieldcnt: bool = False,
    name: str = 'Summary',
    action: Literal['', 'apply', 'calculate', 'none'] = 'apply',
    display: str = '',
    flagbackup: bool = True,
    savepars: bool = False,
    cmdreason: str = '',
    outfile: str = '',
    overwrite: bool = True,
    writeflags: bool = True,
) -> Any:
    """Run native CASA-style MeasurementSet flagging.

    CASA's ``flagdata``: its parameters, defaults and return value.
    """
    return _call_casa(flagdata, locals())

def flagmanager(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    mode: Literal['delete', 'list', 'rename', 'restore', 'save'] = 'list',
    versionname: str = '',
    oldname: str = '',
    comment: str = '',
    merge: Literal['and', 'or', 'replace'] = 'replace',
) -> Any:
    """Manage MeasurementSet flag-version snapshots.

    CASA's ``flagmanager``: its parameters, defaults and return value.
    """
    return _call_casa(flagmanager, locals())

def impbcor(
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    pbimage: StrPath | NDArray[floating] | list[ParameterData] | tuple[ParameterData, ...] = [],
    outfile: StrPath = '',
    overwrite: bool = False,
    box: str = '',
    region: StrPath | Mapping[str, ParameterData] = '',
    chans: str = '',
    stokes: str = '',
    mask: str = '',
    mode: str = 'divide',
    cutoff: float = -1.0,
    stretch: bool = False,
) -> Any:
    """Apply primary-beam correction to a CASA or FITS image

    CASA's ``impbcor``: its parameters, defaults and return value.
    """
    return _call_casa(impbcor, locals())

def impv(
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    outfile: StrPath = '',
    mode: str = 'coords',
    start: str | list[float] | tuple[float, float] | list[str] | tuple[str, str] = '',
    end: str | list[float] | tuple[float, float] | list[str] | tuple[str, str] = '',
    center: str | list[float] | tuple[float, float] | list[str] | tuple[str, str] = '',
    length: float | str | Mapping[str, ParameterData] = '',
    pa: str | Mapping[str, ParameterData] = '',
    width: int | str | Mapping[str, ParameterData] = 1,
    unit: str = 'arcsec',
    overwrite: bool = False,
    region: StrPath | Mapping[str, ParameterData] = '',
    chans: str = '',
    stokes: str = '',
    mask: str = '',
    stretch: bool = False,
) -> Any:
    """Extract a CASA-style position-velocity image

    CASA's ``impv``: its parameters, defaults and return value.
    """
    return _call_casa(impv, locals())

def imsubimage(
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    outfile: StrPath = '',
    box: str = '',
    region: str = '',
    chans: str = '',
    stokes: str = '',
    mask: str = '',
    dropdeg: bool = False,
    overwrite: bool = False,
    verbose: bool = True,
    stretch: bool = False,
    keepaxes: int | list[int] | tuple[int, ...] = [],
) -> Any:
    """Extract a CASA-style image section

    CASA's ``imsubimage``: its parameters, defaults and return value.
    """
    return _call_casa(imsubimage, locals())

def immath(
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    mode: Literal['evalexpr', 'lpoli', 'pola', 'poli', 'spix', 'tpoli'] = 'evalexpr',
    outfile: StrPath = 'immath_results.im',
    expr: str = 'IM0',
    varnames: str | list[str] | tuple[str, ...] = '',
    sigma: str = '0.0mJy/beam',
    polithresh: str = '',
    mask: str = '',
    region: str = '',
    box: str = '',
    chans: str = '',
    stokes: str = '',
    stretch: bool = False,
    imagemd: str = '',
    prec: str = 'float',
    *,
    overwrite: bool = False,
) -> Any:
    """Evaluate mathematical expressions and spectral or polarization operations on images

    CASA's ``immath``: its parameters, defaults and return value.
    """
    return _call_casa(immath, locals())

def imregrid(
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    template: StrPath | Mapping[str, ParameterData] = 'get',
    output: StrPath = '',
    asvelocity: bool = True,
    axes: int | list[int] | tuple[int, ...] = [-1],
    shape: int | list[int] | tuple[int, ...] = [-1],
    interpolation: Literal['linear', 'nearest', 'cubic'] = 'linear',
    decimate: int = 10,
    replicate: bool = False,
    overwrite: bool = False,
) -> Any:
    """Regrid a CASA image onto a template image

    CASA's ``imregrid``: its parameters, defaults and return value.
    """
    return _call_casa(imregrid, locals())

def feather(
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    highres: StrPath = '',
    lowres: StrPath = '',
    sdfactor: float = 1.0,
    effdishdiam: float = -1.0,
    lowpassfiltersd: bool = False,
    *,
    overwrite: bool = True,
) -> Any:
    """Combine high-resolution and low-resolution images in the Fourier domain

    CASA's ``feather``: its parameters, defaults and return value.
    """
    return _call_casa(feather, locals())

def importfits(
    fitsimage: StrPath = '',
    imagename: StrPath = '',
    whichrep: int = 0,
    whichhdu: int = -1,
    zeroblanks: bool = True,
    overwrite: bool = False,
    defaultaxes: bool = False,
    defaultaxesvalues: ParameterData | list[ParameterData] | tuple[ParameterData, ...] = [],
    beam: str | list[str] | tuple[str, ...] = [],
) -> Any:
    """Import a FITS image into a CASA image

    CASA's ``importfits``: its parameters, defaults and return value.
    """
    return _call_casa(importfits, locals())

def msconvert(
    *,
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    outputvis: StrPath | object = _UNSET,
    outformat: Literal['', 'msv2', 'msv4'] | object = _UNSET,
    datagroups: str | list[str] | tuple[str, ...] | object = _UNSET,
    partitionscheme: Literal['FIELD_ID', 'SCAN_NUMBER'] | list[Literal['FIELD_ID', 'SCAN_NUMBER']] | tuple[Literal['FIELD_ID', 'SCAN_NUMBER'], ...] | object = _UNSET,
    withpointing: bool | object = _UNSET,
    padding: Literal['flagged', 'omitted'] | object = _UNSET,
    chunks: list[int] | tuple[int, ...] | object = _UNSET,
    codec: Literal['blosc', 'none', 'zstd'] | object = _UNSET,
    nthreads: int | object = _UNSET,
    overwrite: bool | object = _UNSET,
) -> TaskCompletion:
    """Convert between MeasurementSet v2 and MSv4, in either direction."""
    return _run("msconvert", _explicit(locals(), ('vis', 'outputvis', 'outformat', 'datagroups', 'partitionscheme', 'withpointing', 'padding', 'chunks', 'codec', 'nthreads', 'overwrite')))

def simanalyze(
    project: str = 'sim',
    image: bool = True,
    imagename: str = 'default',
    skymodel: StrPath = '',
    vis: str = 'default',
    modelimage: str = '',
    imsize: int | list[int] | tuple[int, ...] = [0, 0],
    imdirection: str = '',
    cell: str | list[str] | tuple[str, ...] = '',
    interactive: bool = False,
    niter: int = 0,
    threshold: str = '0.1mJy',
    weighting: Literal['briggs', 'briggsabs', 'briggsbwtaper', 'natural', 'radial', 'superuniform', 'uniform'] = 'natural',
    mask: str | list[str] | tuple[str, ...] = [],
    outertaper: str | list[str] | tuple[str, ...] = [],
    pbcor: bool = True,
    stokes: str = 'I',
    featherimage: str = '',
    analyze: bool = False,
    showuv: bool = True,
    showpsf: bool = True,
    showmodel: bool = True,
    showconvolved: bool = False,
    showclean: bool = True,
    showresidual: bool = False,
    showdifference: bool = True,
    showfidelity: bool = True,
    graphics: Literal['', 'both', 'file', 'none', 'screen'] = 'both',
    verbose: bool = False,
    overwrite: bool = True,
    dryrun: bool = False,
    logfile: str = '',
) -> Any:
    """Image and analyze a simulated observation: CASA's simanalyze

    CASA's ``simanalyze``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #980:
    ``interactive``.
    """
    return _call_casa(simanalyze, locals())

def tsdimaging(
    infiles: StrPath | list[StrPath] | tuple[StrPath, ...] = [],
    outfile: StrPath = '',
    overwrite: bool = False,
    field: str | list[str] | tuple[str, ...] = '',
    spw: str | list[str] | tuple[str, ...] = '',
    antenna: str | list[str] | tuple[str, ...] = '',
    scan: str | list[str] | tuple[str, ...] = '',
    intent: str | list[str] | tuple[str, ...] = 'OBSERVE_TARGET#ON_SOURCE',
    timerange: str | list[str] | tuple[str, ...] = '',
    outframe: str = '',
    mode: str = 'channel',
    nchan: int = -1,
    start: int | str = 0,
    width: int | str = 1,
    veltype: str = 'radio',
    specmode: Literal['cont', 'cube', 'cubedata', 'cubesource', 'mfs', 'mvc'] = 'cube',
    interpolation: Literal['cubic', 'linear', 'nearest', 'none'] = 'linear',
    pointingcolumn: str = 'direction',
    convertfirst: str = 'never',
    projection: str = 'SIN',
    imsize: int | list[int] | tuple[int, ...] = [],
    cell: str | list[str] | tuple[str, ...] = '',
    phasecenter: str = '',
    stokes: str = 'I',
    gridfunction: str = 'BOX',
    convsupport: int = -1,
    truncate: int | float | str = -1,
    gwidth: int | float | str = -1,
    jwidth: int | float | str = -1,
    clipminmax: bool = False,
    minweight: float = 0.1,
    brightnessunit: Literal['', 'K', 'Jy/beam'] = '',
    restfreq: str | list[str] | tuple[str, ...] = '',
) -> Any:
    """Grid total-power spectra by pointing into a CASA image and weight image.

    CASA's ``tsdimaging``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #938:
    ``field``, ``scan``, ``timerange``, ``outframe``, ``mode``, ``veltype``,
    ``specmode``, ``interpolation``, ``pointingcolumn``, ``convertfirst``,
    ``projection``, ``stokes``, ``clipminmax``.
    """
    return _call_casa(tsdimaging, locals())

def concat(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = [],
    concatvis: StrPath = '',
    freqtol: int | float | str | dict[str, object] = '',
    dirtol: int | float | str | dict[str, object] = '',
    respectname: bool = False,
    timesort: bool = False,
    copypointing: bool = True,
    visweightscale: float | list[float] | tuple[float, ...] = [],
    forcesingleephemfield: int | float | str | dict[str, object] = '',
) -> Any:
    """Concatenate simulated interferometric MeasurementSets with per-input weights.

    CASA's ``concat``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #939:
    ``respectname``, ``timesort``, ``forcesingleephemfield``.
    """
    return _call_casa(concat, locals())

def simalma(
    project: str = 'sim',
    dryrun: bool = True,
    skymodel: StrPath = '',
    inbright: str = '',
    indirection: str = '',
    incell: str = '',
    incenter: str = '',
    inwidth: str = '',
    complist: StrPath = '',
    compwidth: str = '"8GHz"',
    setpointings: bool = True,
    ptgfile: StrPath = '$project.ptg.txt',
    integration: str = '10s',
    direction: str | list[str] | tuple[str, ...] = [],
    mapsize: str | list[str] | tuple[str, ...] = ['', ''],
    antennalist: str | list[str] | tuple[str, ...] = ['alma.cycle1.1.cfg', 'aca.cycle1.cfg'],
    correlator: str | list[str] | tuple[str, ...] = ['BLC', 'BLC', 'ACASpec'],
    hourangle: str = 'transit',
    totaltime: str | list[str] | tuple[str, ...] = ['20min', '1h'],
    tpnant: int = 0,
    tptime: str = '0s',
    pwv: float = 0.5,
    image: bool = True,
    imsize: int | list[int] | tuple[int, ...] = [128, 128],
    imdirection: str = '',
    cell: str = '',
    niter: int = 0,
    threshold: str = '0.1mJy',
    graphics: Literal['', 'both', 'file', 'none', 'screen'] = 'both',
    verbose: bool = False,
    overwrite: bool = False,
) -> Any:
    """Simulate ALMA and ACA observations and combine their images

    CASA's ``simalma``: its parameters, defaults and return value.
    """
    return _call_casa(simalma, locals())

def getjyperkalma(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    caltable: StrPath = '',
    infile: str = '',
    endpoint: str = 'asdm',
    timeout: int = 180,
    retry: int = 3,
    retry_wait_time: int = 5,
    spw: str = '',
    pol: str = '',
    backup_hosts: str | list[str] | tuple[str, ...] = [],
    *,
    output: StrPath = '',
    overwrite: bool = False,
    format: Literal['json', 'text'] = 'text',
) -> Any:
    """Generate an ALMA Jy/K gain table from the service or a factor file.

    CASA's ``getjyperkalma``: its parameters, defaults and return value.
    """
    return _call_casa(getjyperkalma, locals())

def getephemtable(
    objectname: str = '',
    asis: bool = False,
    timerange: str = '',
    interval: str = '5m',
    outfile: StrPath = '',
    rawdatafile: StrPath = '',
    overwrite: bool = False,
) -> Any:
    """Retrieve a solar-system ephemeris from JPL Horizons.

    CASA's ``getephemtable``: its parameters, defaults and return value.
    """
    return _call_casa(getephemtable, locals())

def getcalmodvla(
    outfile: StrPath = '',
    overwrite: bool = False,
    source: str = '',
    direction: str = '',
    band: str = '',
    obsdate: int | float | str = 0,
    refdate: int | float | str = 0,
    hosts: str | list[str] | tuple[str, ...] = ['http://obs.vla.nrao.edu/calmodvla'],
) -> Any:
    """Retrieve a calibrator brightness model from the VLA service.

    CASA's ``getcalmodvla``: its parameters, defaults and return value.
    """
    return _call_casa(getcalmodvla, locals())

def getantposalma(
    outfile: StrPath = '',
    overwrite: bool = False,
    asdm: str = '',
    tw: str = '',
    snr: Literal['default'] | int | float = 'default',
    search: str = 'both_latest',
    hosts: str | list[str] | tuple[str, ...] = ['https://asa.alma.cl/uncertainties-service/uncertainties/versions/last/measurements/casa/'],
    firstintegration: bool = True,
    nretry: int = 0,
    rdelay: float = 3.0,
) -> Any:
    """Retrieve antenna positions from the ALMA archive.

    CASA's ``getantposalma``: its parameters, defaults and return value.
    """
    return _call_casa(getantposalma, locals())

def smoothcal(
    vis: StrPath = '',
    tablein: StrPath = '',
    caltable: StrPath = '',
    field: str | list[str] | tuple[str, ...] = [],
    smoothtype: str = 'median',
    smoothtime: float = 60.0,
    ratesmooth: bool = False,
) -> Any:
    """Smooth selected calibration solutions in time.

    CASA's ``smoothcal``: its parameters, defaults and return value.
    """
    return _call_casa(smoothcal, locals())

def rerefant(
    vis: StrPath = '',
    tablein: StrPath = '',
    caltable: StrPath = '',
    refantmode: Literal['flex', 'strict'] = 'flex',
    refant: str = '',
) -> Any:
    """Change the reference antenna in a calibration table.

    CASA's ``rerefant``: its parameters, defaults and return value.
    """
    return _call_casa(rerefant, locals())

def plotweather(
    vis: str = '',
    seasonal_weight: float = 0.5,
    doPlot: bool = True,
    plotName: str = '',
) -> Any:
    """Estimate zenith opacity and plot MeasurementSet weather.

    CASA's ``plotweather``: its parameters, defaults and return value.
    """
    return _call_casa(plotweather, locals())

def initweights(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    wtmode: Literal['delwtsp', 'nyq', 'ones', 'tsys', 'weight', 'tinttsys', 'sigma', 'delsigsp'] = 'nyq',
    tsystable: StrPath = '',
    gainfield: str = '',
    interp: str = '',
    spwmap: int | list[int] | tuple[int, ...] = [],
    dowtsp: bool = False,
) -> Any:
    """Initialize MeasurementSet weights and their channel spectra.

    CASA's ``initweights``: its parameters, defaults and return value.
    """
    return _call_casa(initweights, locals())

def statwt(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    selectdata: bool = True,
    field: str = '',
    spw: str = '',
    intent: str = '',
    array: str = '',
    observation: str = '',
    scan: str = '',
    combine: str = '',
    timebin: str | int = 1,
    slidetimebin: bool = False,
    chanbin: str | int = 'spw',
    minsamp: int = 2,
    statalg: str = 'classic',
    fence: float = -1.0,
    center: str = 'mean',
    lside: bool = True,
    zscore: float = -1.0,
    maxiter: int = -1,
    fitspw: str = '',
    excludechans: bool = False,
    wtrange: float | list[float] | tuple[float, ...] = [],
    flagbackup: bool = True,
    preview: bool = False,
    datacolumn: str = 'corrected',
) -> Any:
    """Estimate MeasurementSet weights from visibility variance.

    CASA's ``statwt``: its parameters, defaults and return value.
    """
    return _call_casa(statwt, locals())

def calstat(
    caltable: StrPath = '',
    axis: str = 'amplitude',
    datacolumn: str = 'gain',
) -> Any:
    """Compute descriptive statistics for a calibration table column.

    CASA's ``calstat``: its parameters, defaults and return value.
    """
    return _call_casa(calstat, locals())

def listcal(
    vis: StrPath = '',
    caltable: StrPath = '',
    field: str = '',
    antenna: str = '',
    spw: str = '',
    listfile: StrPath = '',
    pagerows: int = 50,
) -> Any:
    """List selected antenna calibration solutions and flags.

    CASA's ``listcal``: its parameters, defaults and return value.
    """
    return _call_casa(listcal, locals())

def plotbandpass(
    caltable: str = '',
    antenna: str | int | list[str] | tuple[str, ...] | list[int] | tuple[int, ...] = '',
    field: str | int | list[str] | tuple[str, ...] | list[int] | tuple[int, ...] = '',
    spw: str | int | list[str] | tuple[str, ...] | list[int] | tuple[int, ...] = '',
    yaxis: str = 'amp',
    xaxis: str = 'chan',
    figfile: str = '',
    plotrange: float | list[float] | tuple[float, ...] = [0.0, 0.0, 0.0, 0.0],
    caltable2: str = '',
    overlay: str = '',
    showflagged: bool = False,
    timeranges: str = '',
    buildpdf: bool = False,
    caltable3: str = '',
    markersize: int = 3,
    density: int = 108,
    interactive: bool = True,
    showpoints: str | bool = 'auto',
    showlines: str | bool = 'auto',
    subplot: str | int = '22',
    zoom: str = '',
    poln: list[str] | tuple[str, ...] | str = '',
    showatm: bool = False,
    pwv: float | str = 'auto',
    gs: str = 'gs',
    convert: str = 'convert',
    chanrange: str | list[int] | tuple[int, ...] = '',
    solutionTimeThresholdSeconds: float = 30.0,
    debug: bool = False,
    phase: list[int] | tuple[int, ...] | str = '',
    vis: str = '',
    showtsky: bool = False,
    showfdm: bool = False,
    showatmfield: int | str = '',
    lo1: str | float = '',
    showimage: bool = False,
    showatmpoints: bool = False,
    parentms: str = '',
    pdftk: str = 'pdftk',
    channeldiff: bool | float = False,
    edge: int = 8,
    resample: int = 1,
    platformingThreshold: float = 10.0,
    platformingSigma: float = 10.0,
    basebands: int | str | list[int] | tuple[int, ...] = '',
    showBasebandNumber: bool = False,
    scans: int | str | list[int] | tuple[int, ...] = '',
    figfileSequential: bool = False,
    chanrangeSetXrange: bool = False,
) -> Any:
    """Plot calibration spectra and channel derivatives.

    CASA's ``plotbandpass``: its parameters, defaults and return value.
    """
    return _call_casa(plotbandpass, locals())

def predictcomp(
    objname: str = '',
    standard: Literal['Perley-Taylor 95', 'Butler-JPL-Horizons 2010', 'Perley-Butler 2010', 'Perley-Taylor 99', 'Perley-Butler 2013', 'Perley 90', 'Butler-JPL-Horizons 2012', 'Perley-Butler 2017', 'Baars'] = 'Butler-JPL-Horizons 2010',
    epoch: str = '',
    minfreq: str = '',
    maxfreq: str = '',
    nfreqs: int = 2,
    prefix: str = '',
    antennalist: str = '',
    showplot: bool = False,
    savefig: str = '',
    symb: str = '.',
    include0amp: bool = False,
    include0bl: bool = False,
    blunit: Literal['', 'klambda'] = '',
    showbl0flux: bool = False,
) -> Any:
    """Write a flux calibrator component list and optionally plot its visibility amplitudes.

    CASA's ``predictcomp``: its parameters, defaults and return value.
    """
    return _call_casa(predictcomp, locals())

def wvrgcal(
    vis: StrPath = '',
    caltable: StrPath = '',
    toffset: float = 0.0,
    segsource: bool = True,
    sourceflag: str | list[str] | tuple[str, ...] = [],
    tie: str | list[str] | tuple[str, ...] = [],
    nsol: int = 1,
    disperse: bool = False,
    wvrflag: str | list[str] | tuple[str, ...] = [],
    statfield: str = '',
    statsource: str = '',
    smooth: str = '',
    scale: float = 1.0,
    spw: int | list[int] | tuple[int, ...] = [],
    wvrspw: int | list[int] | tuple[int, ...] = [],
    reversespw: str = '',
    cont: bool = False,
    maxdistm: float = 500.0,
    minnumants: int = 2,
    mingoodfrac: float = 0.8,
    usefieldtab: bool = False,
    refant: str | list[str] | tuple[str, ...] = [],
    offsetstable: StrPath = '',
    rseed: int = 0,
) -> Any:
    """Use ALMA radiometer measurements to calibrate atmospheric phase.

    CASA's ``wvrgcal``: its parameters, defaults and return value.
    """
    return _call_casa(wvrgcal, locals())

def polcal(
    vis: StrPath = '',
    caltable: StrPath = '',
    field: str = '',
    spw: str = '',
    intent: str = '',
    selectdata: bool = True,
    timerange: str = '',
    uvrange: str | int | float | list[str] = '',
    antenna: str = '',
    scan: str = '',
    observation: str | int = '',
    msselect: str = '',
    solint: str | int | float = 'inf',
    combine: str = 'obs,scan',
    preavg: float = 300.0,
    refant: str = '',
    minblperant: int = 4,
    minsnr: float = 3.0,
    poltype: Literal['Dflls', 'D', 'Dlls', 'D+QU', 'D+X', 'PosAng', 'X', 'Dfgen', 'Dfgen+X', 'Df+X', 'Df', 'Dgen+X', 'Dgen+QU', 'Df+QU', 'Dgen', 'Xfparang+QU', 'Xf', 'Dfgen+QU', 'Xj', 'Xparang+QU'] = 'D+QU',
    smodel: float | list[float] | tuple[float, ...] = [],
    append: bool = False,
    docallib: bool = False,
    callib: StrPath = '',
    gaintable: StrPath | list[StrPath] | tuple[StrPath, ...] = [],
    gainfield: str | list[str] | tuple[str, ...] = [],
    interp: str | list[str] | tuple[str, ...] = [],
    spwmap: list[list[int] | tuple[int, ...]] | tuple[list[int] | tuple[int, ...], ...] = [],
) -> Any:
    """Solve polarization leakage and cross-hand calibration.

    CASA's ``polcal``: its parameters, defaults and return value.
    """
    return _call_casa(polcal, locals())

def polfromgain(
    vis: StrPath = '',
    tablein: StrPath = '',
    caltable: StrPath = '',
    paoffset: float = 0.0,
    minpacov: float = 30.0,
) -> Any:
    """Estimate fractional linear polarization from receptor gain amplitude ratios.

    CASA's ``polfromgain``: its parameters, defaults and return value.
    """
    return _call_casa(polfromgain, locals())

def sdcal(
    infile: StrPath = '',
    calmode: Literal['ps', 'otfraster', 'otf', 'tsys', 'apply', 'ps,apply', 'tsys,apply', 'ps,tsys,apply', 'otfraster,apply', 'otfraster,tsys,apply', 'otf,apply', 'otf,tsys,apply'] = 'ps',
    fraction: str | int | float = '10%',
    noff: int = -1,
    width: float = 0.5,
    elongated: bool = False,
    applytable: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    interp: str = '',
    spwmap: list[int] | dict[int | str, list[int]] = {},
    outfile: StrPath = '',
    overwrite: bool = False,
    field: str = '',
    spw: str = '',
    scan: str = '',
    *,
    intent: str = '',
) -> Any:
    """Generate and apply single-dish sky and system-temperature calibration.

    CASA's ``sdcal``: its parameters, defaults and return value.
    """
    return _call_casa(sdcal, locals())

def sdgaincal(
    infile: StrPath = '',
    calmode: Literal['doublecircle'] = 'doublecircle',
    radius: str | int | float = '',
    smooth: bool = True,
    antenna: str = '',
    field: str = '',
    spw: str = '',
    scan: str = '',
    intent: str = '',
    applytable: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    interp: str | list[str] | tuple[str, ...] = '',
    spwmap: list[int] | list[list[int]] | tuple[int, ...] | tuple[tuple[int, ...], ...] | dict[int, list[int]] = [],
    outfile: StrPath = '',
    overwrite: bool = False,
) -> Any:
    """Solve single-dish double-circle gain calibration.

    CASA's ``sdgaincal``: its parameters, defaults and return value.
    """
    return _call_casa(sdgaincal, locals())

def sdsmooth(
    infile: StrPath = '',
    datacolumn: str = 'data',
    antenna: str = '',
    field: str = '',
    spw: str = '',
    timerange: str = '',
    scan: str = '',
    pol: str = '',
    intent: str = '',
    reindex: bool = True,
    kernel: str = 'gaussian',
    kwidth: int = 5,
    outfile: StrPath = '',
    overwrite: bool = False,
) -> Any:
    """Convolve selected single-dish spectra and publish a MeasurementSet.

    CASA's ``sdsmooth``: its parameters, defaults and return value.
    """
    return _call_casa(sdsmooth, locals())

def sdbaseline(
    infile: StrPath = '',
    datacolumn: str = 'data',
    antenna: str = '',
    field: str = '',
    spw: str = '',
    timerange: str = '',
    scan: str = '',
    pol: str = '',
    intent: str = '',
    reindex: bool = True,
    maskmode: str = 'list',
    thresh: float = 5.0,
    avg_limit: int = 4,
    minwidth: int = 4,
    edge: list[int] = [0, 0],
    blmode: str = 'fit',
    dosubtract: bool = True,
    blformat: str | list[str] = 'text',
    bloutput: str | list[str] = '',
    bltable: str = '',
    blfunc: str = 'poly',
    order: int = 5,
    npiece: int = 2,
    applyfft: bool = True,
    fftmethod: str = 'fft',
    fftthresh: float | int | str = 3.0,
    addwn: int | str | list[int] = [0],
    rejwn: int | str | list[int] = [],
    clipthresh: float = 3.0,
    clipniter: int = 0,
    blparam: str = '',
    verbose: bool = False,
    updateweight: bool = False,
    sigmavalue: str = 'stddev',
    showprogress: bool = False,
    minnrow: int = 1000,
    outfile: StrPath = '',
    overwrite: bool = False,
) -> Any:
    """Fit, subtract and apply spectral baselines to selected single-dish observations.

    CASA's ``sdbaseline``: its parameters, defaults and return value.
    """
    return _call_casa(sdbaseline, locals())

def sdfit(
    infile: StrPath = '',
    datacolumn: str = 'data',
    antenna: str = '',
    field: str = '',
    spw: str = '',
    timerange: str = '',
    scan: str = '',
    pol: str = '',
    intent: str = '',
    timebin: str = '',
    timespan: str = '',
    polaverage: Literal['', 'stokes', 'geometric'] = '',
    fitfunc: Literal['gaussian', 'lorentzian'] = 'gaussian',
    fitmode: Literal['list', 'auto'] = 'list',
    nfit: int | list[int] = [0],
    thresh: float = 5.0,
    avg_limit: int = 4,
    minwidth: int = 4,
    edge: list[int] = [0, 0],
    outfile: str = '',
    overwrite: bool = False,
) -> Any:
    """Fit Gaussian or Lorentzian lines to selected single-dish spectra.

    CASA's ``sdfit``: its parameters, defaults and return value.
    """
    return _call_casa(sdfit, locals())

def sdatmcor(
    infile: StrPath = '',
    datacolumn: str = 'data',
    outfile: StrPath = '',
    overwrite: bool = False,
    field: str = '',
    spw: str = '',
    scan: str = '',
    antenna: str = '',
    correlation: str = '',
    timerange: str = '',
    intent: str = '',
    observation: str = '',
    feed: str = '',
    msselect: str = '',
    outputspw: str = '',
    gainfactor: float | dict[str | int, float] | str = 1.0,
    dtem_dh: str | float = '',
    h0: str | float = '',
    atmtype: int = 2,
    atmdetail: bool = False,
    altitude: str | float = '',
    temperature: str | float = '',
    pressure: str | float = '',
    humidity: float = -1.0,
    pwv: str | float = '',
    dp: str | float = '',
    dpm: float = -1.0,
    layerboundaries: str | list[str] | list[float] = '',
    layertemperature: str | list[str] | list[float] = '',
) -> Any:
    """Correct calibrated single-dish spectra for differential atmospheric emission.

    CASA's ``sdatmcor``: its parameters, defaults and return value.
    """
    return _call_casa(sdatmcor, locals())

TASK_SURFACES = ('msexplore', 'calibrate', 'importvla', 'tclean', 'simobserve', 'imhead', 'imstat', 'immoments', 'exportfits', 'mstransform', 'split', 'applycal', 'gaincal', 'accor', 'blcal', 'fringefit', 'pccor', 'appendantab', 'bandpass', 'fluxscale', 'uvsub', 'ft', 'setjy', 'uvmodelfit', 'clearcal', 'delmod', 'gencal', 'plotms', 'flagdata', 'flagmanager', 'impbcor', 'impv', 'imsubimage', 'immath', 'imregrid', 'feather', 'importfits', 'msconvert', 'simanalyze', 'tsdimaging', 'concat', 'simalma', 'getjyperkalma', 'getephemtable', 'getcalmodvla', 'getantposalma', 'smoothcal', 'rerefant', 'plotweather', 'initweights', 'statwt', 'calstat', 'listcal', 'plotbandpass', 'predictcomp', 'wvrgcal', 'polcal', 'polfromgain', 'sdcal', 'sdgaincal', 'sdsmooth', 'sdbaseline', 'sdfit', 'sdatmcor')

__all__ = ['TASK_SURFACES', 'msexplore', 'calibrate', 'importvla', 'tclean', 'simobserve', 'imhead', 'imstat', 'immoments', 'exportfits', 'mstransform', 'split', 'applycal', 'gaincal', 'accor', 'blcal', 'fringefit', 'pccor', 'appendantab', 'bandpass', 'fluxscale', 'uvsub', 'ft', 'setjy', 'uvmodelfit', 'clearcal', 'delmod', 'gencal', 'plotms', 'flagdata', 'flagmanager', 'impbcor', 'impv', 'imsubimage', 'immath', 'imregrid', 'feather', 'importfits', 'msconvert', 'simanalyze', 'tsdimaging', 'concat', 'simalma', 'getjyperkalma', 'getephemtable', 'getcalmodvla', 'getantposalma', 'smoothcal', 'rerefant', 'plotweather', 'initweights', 'statwt', 'calstat', 'listcal', 'plotbandpass', 'predictcomp', 'wvrgcal', 'polcal', 'polfromgain', 'sdcal', 'sdgaincal', 'sdsmooth', 'sdbaseline', 'sdfit', 'sdatmcor']
