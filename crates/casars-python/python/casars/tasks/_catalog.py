"""Generated CASA-named wrappers for every catalog task.

Do not edit by hand; run ``scripts/generate-python-parameter-wrappers.py``.
"""

from __future__ import annotations

from collections.abc import Mapping
from os import PathLike
from typing import Any, Literal, TypeAlias

from ..parameters import ParameterData
from ._runner import TaskCompletion, _call_casa, _run

StrPath: TypeAlias = str | PathLike[str]
_UNSET = object()


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
    frequencytol: str = '150000.0Hz',
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
    field: str = '',
    spw: str = '',
    timerange: str = '',
    uvrange: str = '',
    antenna: str = '',
    scan: str = '',
    observation: str = '',
    intent: str = '',
    datacolumn: str = 'corrected',
    imagename: StrPath = '',
    imsize: int | list[int] | tuple[int, ...] = [100],
    cell: str | list[str] | tuple[str, ...] = [],
    phasecenter: str = '',
    stokes: str = 'I',
    projection: str = 'SIN',
    startmodel: StrPath = '',
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
    hogbom_iteration_mode: str = 'strict',
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
    Accepted only at CASA's default, where CASA reads them, until #944:
    ``restart``.
    Accepted only at CASA's default, where CASA reads them, until #980:
    ``timerange``, ``antenna``, ``scan``, ``observation``, ``projection``,
    ``reffreq``, ``psfphasecenter``, ``vptable``, ``computepastep``,
    ``rotatepastep``, ``restoration``, ``noise``, ``npixels``,
    ``interactive``, ``pbmask``, ``smoothfactor``, ``cutthreshold``,
    ``dogrowprune``, ``minpercentchange``, ``verbose``, ``fastnoise``,
    ``calcres``, ``calcpsf``.
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
    mode: Literal['list', 'put', 'summary'] = 'summary',
    hdkey: str = '',
    hdvalue: str = '',
    verbose: bool = False,
) -> Any:
    """Inspect or update CASA image header metadata.

    CASA's ``imhead``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #977:
    ``verbose``.
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
    center: Literal['mean', 'median', 'zero'] = 'mean',
    lside: bool = True,
    zscore: float = -1.0,
    maxiter: int = -1,
    clmethod: str = 'auto',
    niter: int = 3,
) -> Any:
    """Compute CASA image statistics over optional pixel, region, and channel selections.

    CASA's ``imstat``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #977:
    ``axes``, ``stokes``, ``listit``, ``verbose``, ``mask``, ``stretch``,
    ``logfile``, ``append``, ``algorithm``, ``fence``, ``center``,
    ``lside``, ``zscore``, ``maxiter``, ``clmethod``, ``niter``.
    """
    return _call_casa(imstat, locals())

def immoments(
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    moments: int | list[int] | tuple[int, ...] = [0],
    axis: str = 'spectral',
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
    Accepted only at CASA's default, where CASA reads them, until #977:
    ``axis``, ``region``, ``box``, ``stokes``, ``excludepix``, ``stretch``.
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
    Accepted only at CASA's default, where CASA reads them, until #977:
    ``optical``, ``bitpix``, ``minpix``, ``maxpix``, ``dropstokes``,
    ``stokeslast``, ``history``, ``dropdeg``.
    """
    return _call_casa(exportfits, locals())

def mstransform(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    outputvis: StrPath = '',
    createmms: bool = False,
    separationaxis: str = 'auto',
    numsubms: str = 'auto',
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
    observation: str = '',
    feed: str = '',
    datacolumn: str = 'corrected',
    realmodelcol: bool = False,
    keepflags: bool = True,
    usewtspectrum: bool = False,
    combinespws: bool = False,
    chanaverage: bool = False,
    chanbin: int = 1,
    hanning: bool = False,
    regridms: bool = False,
    mode: str = 'channel',
    nchan: int = -1,
    start: int = 0,
    width: int = 1,
    nspw: int = 1,
    interpolation: str = 'linear',
    phasecenter: str = '',
    restfreq: str = '',
    outframe: str = '',
    veltype: str = 'radio',
    preaverage: bool = False,
    timeaverage: bool = False,
    timebin: str = '0s',
    timespan: str = '',
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
    Accepted only at CASA's default, where CASA reads them, until #978:
    ``createmms``, ``separationaxis``, ``numsubms``, ``tileshape``,
    ``correlation``, ``intent``, ``array``, ``uvrange``, ``observation``,
    ``feed``, ``realmodelcol``, ``usewtspectrum``, ``combinespws``,
    ``hanning``, ``regridms``, ``mode``, ``nchan``, ``start``, ``width``,
    ``nspw``, ``interpolation``, ``phasecenter``, ``restfreq``,
    ``outframe``, ``veltype``, ``preaverage``, ``timeaverage``, ``timebin``,
    ``timespan``, ``maxuvwdistance``, ``docallib``, ``callib``,
    ``douvcontsub``, ``fitspw``, ``fitorder``, ``want_cont``,
    ``denoising_lib``, ``nthreads``, ``niter``, ``disableparallel``,
    ``ddistart``, ``monolithic_processing``, ``reindex``.
    """
    return _call_casa(mstransform, locals())

def split(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    outputvis: StrPath = '',
    keepmms: bool = True,
    field: str = '',
    spw: str = '',
    scan: str = '',
    antenna: str = '',
    correlation: str = '',
    timerange: str = '',
    intent: str = '',
    array: str = '',
    uvrange: str = '',
    observation: str = '',
    feed: str = '',
    datacolumn: str = 'corrected',
    keepflags: bool = True,
    width: int = 1,
    timebin: str = '0s',
    combine: str = '',
    *,
    msselect: str = '',
) -> Any:
    """Create a selected MeasurementSet subset, equivalent to CASA split.

    CASA's ``split``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #978:
    ``keepmms``, ``correlation``, ``intent``, ``array``, ``uvrange``,
    ``observation``, ``feed``, ``timebin``, ``combine``.
    """
    return _call_casa(split, locals())

def applycal(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    field: str = '',
    spw: str = '',
    intent: str = '',
    selectdata: bool = True,
    timerange: str = '',
    uvrange: str = '',
    antenna: str = '',
    scan: str = '',
    observation: str = '',
    msselect: str = '',
    docallib: bool = False,
    callib: StrPath = '',
    gaintable: StrPath | list[StrPath] | tuple[StrPath, ...] = [],
    gainfield: str | list[str] | tuple[str, ...] = [],
    interp: str | list[str] | tuple[str, ...] = [],
    spwmap: list[list[int] | tuple[int, ...]] | tuple[list[int] | tuple[int, ...], ...] = [],
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
    Accepted only at CASA's default, where CASA reads them, until #979:
    ``intent``, ``uvrange``.
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
    uvrange: str = '',
    antenna: str = '',
    scan: str = '',
    observation: str = '',
    msselect: str = '',
    solint: str = 'inf',
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
    Accepted only at CASA's default, where CASA reads them, until #979:
    ``intent``, ``uvrange``, ``preavg``, ``refantmode``, ``normtype``,
    ``solmode``, ``rmsthresh``, ``corrdepflags``, ``append``,
    ``splinetime``, ``npointaver``, ``phasewrap``.
    """
    return _call_casa(gaincal, locals())

def bandpass(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    caltable: StrPath = '',
    field: str = '',
    spw: str = '',
    intent: str = '',
    selectdata: bool = True,
    timerange: str = '',
    uvrange: str = '',
    antenna: str = '',
    scan: str = '',
    observation: str = '',
    msselect: str = '',
    solint: str = 'inf',
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
    Accepted only at CASA's default, where CASA reads them, until #979:
    ``intent``, ``uvrange``, ``solint``, ``corrdepflags``, ``append``,
    ``fillgaps``, ``visnorm``, ``maskcenter``.
    """
    return _call_casa(bandpass, locals())

def fluxscale(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
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
    Accepted only at CASA's default, where CASA reads them, until #979:
    ``listfile``, ``append``, ``antenna``, ``timerange``, ``scan``,
    ``fitorder``, ``display``.
    """
    return _call_casa(fluxscale, locals())

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
    Accepted only at CASA's default, where CASA reads them, until #979:
    ``infile``, ``endpoint``, ``timeout``, ``retry``, ``retry_wait_time``,
    ``ant_pos_time_limit``, ``pol``, ``uniform``.
    """
    return _call_casa(gencal, locals())

def plotms(
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
    """Plot MeasurementSet visibility data with CASA plotms-style selections and axes."""
    return _run("plotms", _explicit(locals(), ('vis', 'format', 'output', 'overwrite', 'selectdata', 'field', 'spw', 'timerange', 'uvrange', 'antenna', 'scan', 'correlation', 'observation', 'array', 'intent', 'feed', 'msselect', 'page_spec', 'preset', 'x_axis', 'y_axis', 'y_axis2', 'datacolumn', 'color_by', 'avgchannel', 'avgtime', 'avgscan', 'avgfield', 'avgbaseline', 'avgantenna', 'avgspw', 'scalar', 'freqframe', 'restfreq', 'veldef', 'iteraxis', 'gridrows', 'gridcols', 'xselfscale', 'yselfscale', 'xsharedaxis', 'ysharedaxis', 'title', 'xlabel', 'ylabel', 'symbol_size', 'showlegend', 'legendposition', 'showmajorgrid', 'showminorgrid', 'headeritems', 'max_points', 'plot_output', 'plot_format', 'flag_action', 'flag_xmin', 'flag_xmax', 'flag_ymin', 'flag_ymax', 'flag_plotindex', 'flag_panel', 'flag_extcorr', 'flag_extchannel', 'flag_selected', 'flag_apply', 'flag_output')))

def flagdata(
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    mode: Literal['antint', 'clip', 'elevation', 'extend', 'list', 'manual', 'manualflag', 'quack', 'rflag', 'shadow', 'summary', 'tfcrop', 'unflag'] = 'manual',
    autocorr: bool = False,
    inpfile: str = '',
    reason: str = 'any',
    tbuff: float = 0.0,
    spw: str = '',
    field: str = '',
    antenna: str = '',
    uvrange: str = '',
    timerange: str = '',
    correlation: str = '',
    scan: str = '',
    intent: str = '',
    array: str = '',
    observation: str = '',
    feed: str = '',
    clipminmax: list[float] | tuple[float, ...] = [],
    datacolumn: str = 'DATA',
    clipoutside: bool = True,
    channelavg: bool = False,
    chanbin: int = 1,
    timeavg: bool = False,
    timebin: str = '0s',
    clipzeros: bool = False,
    quackinterval: float = 1.0,
    quackmode: Literal['beg', 'end', 'endb', 'tail'] = 'beg',
    quackincrement: bool = False,
    tolerance: float = 0.0,
    addantenna: str = '',
    lowerlimit: float = 0.0,
    upperlimit: float = 90.0,
    ntime: str = 'scan',
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
    winsize: int = 3,
    timedev: float | Literal['', 'none'] = '',
    freqdev: float | Literal['', 'none'] = '',
    timedevscale: float = 5.0,
    freqdevscale: float = 5.0,
    spectralmax: float = 1000000.0,
    spectralmin: float = 0.0,
    antint_ref_antenna: str = '',
    minchanfrac: float = 0.6,
    verbose: bool = False,
    extendpols: bool = True,
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
    Accepted only at CASA's default, where CASA reads them, until #978:
    ``autocorr``, ``inpfile``, ``reason``, ``tbuff``, ``uvrange``,
    ``timerange``, ``correlation``, ``intent``, ``array``, ``observation``,
    ``feed``, ``clipminmax``, ``clipoutside``, ``channelavg``, ``chanbin``,
    ``timeavg``, ``timebin``, ``quackincrement``, ``tolerance``,
    ``addantenna``, ``lowerlimit``, ``upperlimit``, ``ntime``,
    ``combinescans``, ``timefit``, ``freqfit``, ``maxnpieces``,
    ``flagdimension``, ``usewindowstats``, ``halfwin``, ``winsize``,
    ``antint_ref_antenna``, ``minchanfrac``, ``verbose``, ``growaround``,
    ``flagneartime``, ``flagnearfreq``, ``minrel``, ``maxrel``, ``minabs``,
    ``maxabs``, ``spwchan``, ``spwcorr``, ``basecnt``, ``fieldcnt``,
    ``name``, ``display``, ``savepars``, ``cmdreason``, ``outfile``,
    ``overwrite``, ``writeflags``.
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
    pbimage: StrPath | list[StrPath] | tuple[StrPath, ...] = [],
    outfile: StrPath = '',
    overwrite: bool = False,
    box: str = '',
    region: str = '',
    chans: str = '',
    stokes: str = '',
    mask: str = '',
    mode: Literal['divide', 'multiply'] = 'divide',
    cutoff: float = -1.0,
    stretch: bool = False,
) -> Any:
    """Apply primary-beam correction to a CASA image

    CASA's ``impbcor``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #977:
    ``box``, ``region``, ``chans``, ``stokes``, ``mask``, ``stretch``.
    """
    return _call_casa(impbcor, locals())

def impv(
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    outfile: StrPath = '',
    mode: Literal['coords', 'length'] = 'coords',
    start: str = '',
    end: str = '',
    center: str | list[str] | tuple[str, ...] = '',
    length: str = '',
    pa: str = '',
    width: int = 1,
    unit: str = 'arcsec',
    overwrite: bool = False,
    region: str = '',
    chans: str = '',
    stokes: str = '',
    mask: str = '',
    stretch: bool = False,
) -> Any:
    """Extract a CASA-style position-velocity image

    CASA's ``impv``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #977:
    ``mode``, ``center``, ``length``, ``pa``, ``unit``, ``region``,
    ``stokes``, ``mask``, ``stretch``.
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
    Accepted only at CASA's default, where CASA reads them, until #977:
    ``region``, ``stokes``, ``mask``, ``dropdeg``, ``verbose``, ``stretch``,
    ``keepaxes``.
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
    """Evaluate tutorial image arithmetic expressions

    CASA's ``immath``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #977:
    ``mode``, ``varnames``, ``sigma``, ``polithresh``, ``mask``, ``region``,
    ``box``, ``chans``, ``stokes``, ``stretch``, ``imagemd``, ``prec``.
    """
    return _call_casa(immath, locals())

def imregrid(
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] = '',
    template: StrPath = 'get',
    output: StrPath = '',
    asvelocity: bool = True,
    axes: int | list[int] | tuple[int, ...] = [-1],
    shape: int | list[int] | tuple[int, ...] = [-1],
    interpolation: Literal['linear', 'nearest'] = 'linear',
    decimate: int = 10,
    replicate: bool = False,
    overwrite: bool = False,
) -> Any:
    """Regrid a CASA image onto a template image

    CASA's ``imregrid``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #977:
    ``asvelocity``, ``axes``, ``shape``, ``decimate``, ``replicate``.
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
    overwrite: bool = False,
) -> Any:
    """Combine high-resolution and low-resolution images in the Fourier domain

    CASA's ``feather``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #977:
    ``effdishdiam``, ``lowpassfiltersd``.
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
    defaultaxesvalues: str | list[str] | tuple[str, ...] = [],
    beam: str | list[str] | tuple[str, ...] = [],
) -> Any:
    """Import a FITS primary image into a CASA image

    CASA's ``importfits``: its parameters, defaults and return value.
    Accepted only at CASA's default, where CASA reads them, until #977:
    ``whichrep``, ``whichhdu``, ``zeroblanks``, ``defaultaxes``,
    ``defaultaxesvalues``, ``beam``.
    """
    return _call_casa(importfits, locals())

TASK_SURFACES = ('msexplore', 'calibrate', 'importvla', 'tclean', 'simobserve', 'imhead', 'imstat', 'immoments', 'exportfits', 'mstransform', 'split', 'applycal', 'gaincal', 'bandpass', 'fluxscale', 'gencal', 'plotms', 'flagdata', 'flagmanager', 'impbcor', 'impv', 'imsubimage', 'immath', 'imregrid', 'feather', 'importfits')

__all__ = ['TASK_SURFACES', 'msexplore', 'calibrate', 'importvla', 'tclean', 'simobserve', 'imhead', 'imstat', 'immoments', 'exportfits', 'mstransform', 'split', 'applycal', 'gaincal', 'bandpass', 'fluxscale', 'gencal', 'plotms', 'flagdata', 'flagmanager', 'impbcor', 'impv', 'imsubimage', 'immath', 'imregrid', 'feather', 'importfits']
