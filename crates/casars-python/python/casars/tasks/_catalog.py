"""Generated CASA-named wrappers for every catalog task.

Do not edit by hand; run ``scripts/generate-python-parameter-wrappers.py``.
"""

from __future__ import annotations

from collections.abc import Mapping
from os import PathLike
from typing import Literal, TypeAlias

from ..parameters import ParameterData
from ._runner import TaskCompletion, _run

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
    *,
    archivefiles: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    bandname: Literal['4', 'C', 'K', 'Ka', 'L', 'P', 'Q', 'S', 'U', 'X', 'none'] | object = _UNSET,
    frequencytol: str | object = _UNSET,
    project: str | object = _UNSET,
    starttime: str | object = _UNSET,
    stoptime: str | object = _UNSET,
    applytsys: bool | object = _UNSET,
    autocorr: bool | object = _UNSET,
    antnamescheme: Literal['new', 'old'] | object = _UNSET,
    keepblanks: bool | object = _UNSET,
    evlabands: bool | object = _UNSET,
) -> TaskCompletion:
    """scan or import old VLA export archives from disk"""
    return _run("importvla", _explicit(locals(), ('archivefiles', 'vis', 'bandname', 'frequencytol', 'project', 'starttime', 'stoptime', 'applytsys', 'autocorr', 'antnamescheme', 'keepblanks', 'evlabands')))

def imager(
    *,
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    imagename: StrPath | object = _UNSET,
    imsize: int | list[int] | tuple[int, ...] | Literal['auto'] | object = _UNSET,
    cell: str | list[str] | tuple[str, ...] | object = _UNSET,
    datacolumn: str | object = _UNSET,
    savemodel: Literal['modelcolumn', 'none'] | object = _UNSET,
    outlierfile: StrPath | object = _UNSET,
    field: str | object = _UNSET,
    phasecenter_field: str | object = _UNSET,
    ddid: str | object = _UNSET,
    phasecenter: str | object = _UNSET,
    spw: str | object = _UNSET,
    channel_start: int | Literal['none'] | object = _UNSET,
    channel_count: int | Literal['none'] | object = _UNSET,
    stokes: str | object = _UNSET,
    specmode: Literal['cube', 'cubedata', 'cubesource', 'mfs'] | object = _UNSET,
    start: str | object = _UNSET,
    width: str | object = _UNSET,
    outframe: Literal['BARY', 'LSRK', 'REST', 'TOPO'] | object = _UNSET,
    veltype: Literal['BETA', 'OPTICAL', 'RADIO'] | object = _UNSET,
    interpolation: Literal['linear', 'nearest'] | object = _UNSET,
    restfreq: str | object = _UNSET,
    restoringbeam: Literal['common', 'none'] | object = _UNSET,
    perchanweightdensity: bool | object = _UNSET,
    dirty_only: bool | object = _UNSET,
    niter: int | object = _UNSET,
    threshold: str | object = _UNSET,
    nmajor: int | object = _UNSET,
    gain: float | object = _UNSET,
    nsigma: float | object = _UNSET,
    psfcutoff: float | object = _UNSET,
    minor_cycle_length: int | object = _UNSET,
    cyclefactor: float | object = _UNSET,
    deconvolver: Literal['clark', 'hogbom', 'mtmfs', 'multiscale'] | object = _UNSET,
    minpsffraction: float | object = _UNSET,
    maxpsffraction: float | object = _UNSET,
    nterms: int | object = _UNSET,
    hogbom_iteration_mode: str | object = _UNSET,
    scales: str | object = _UNSET,
    smallscalebias: float | object = _UNSET,
    usemask: Literal['auto-multithresh', 'user'] | object = _UNSET,
    sidelobethreshold: float | object = _UNSET,
    noisethreshold: float | object = _UNSET,
    lownoisethreshold: float | object = _UNSET,
    negativethreshold: float | object = _UNSET,
    minbeamfrac: float | object = _UNSET,
    growiterations: int | object = _UNSET,
    mask_box: str | object = _UNSET,
    weighting: Literal['briggs', 'briggsbwtaper', 'natural', 'uniform'] | object = _UNSET,
    mask_image: StrPath | object = _UNSET,
    robust: float | object = _UNSET,
    wprojplanes: int | Literal['none'] | object = _UNSET,
    usepointing: bool | object = _UNSET,
    write_pb: bool | object = _UNSET,
    pbcor: bool | object = _UNSET,
    pblimit: float | object = _UNSET,
    gridder: Literal['awproject', 'mosaic', 'standard', 'wproject'] | object = _UNSET,
    facets: int | object = _UNSET,
    parallel: bool | object = _UNSET,
    uvrange: str | object = _UNSET,
    intent: str | object = _UNSET,
    cfcache: StrPath | Literal['none'] | object = _UNSET,
    cf_resident_mb: int | Literal['auto'] | object = _UNSET,
    pointingoffsetsigdev: str | object = _UNSET,
    normtype: Literal['flatnoise', 'flatsky'] | object = _UNSET,
    fitspw: str | object = _UNSET,
    fitorder: int | object = _UNSET,
    save_continuum_residual: bool | object = _UNSET,
    aw_cf_source: Literal['casa-import', 'native-evla'] | object = _UNSET,
    native_cf_cache: StrPath | Literal['none'] | object = _UNSET,
    evla_surface: StrPath | Literal['none'] | object = _UNSET,
    native_cf_policy: Literal['reuse-only', 'generate-missing', 'regenerate'] | object = _UNSET,
    native_cf_working_size: int | Literal['none'] | object = _UNSET,
    native_cf_oversampling: int | Literal['none'] | object = _UNSET,
    native_cf_cache_bytes: int | Literal['none'] | object = _UNSET,
    native_cf_maximum_cells: int | Literal['none'] | object = _UNSET,
    backend: Literal['auto', 'cpu', 'gpu'] | object = _UNSET,
    gridprecision: Literal['auto', 'f32', 'f64'] | object = _UNSET,
    stage_timing: bool | object = _UNSET,
    metal_stage_timing: bool | object = _UNSET,
    science_trace: bool | object = _UNSET,
) -> TaskCompletion:
    """Run CASA-compatible dirty and deconvolved imaging from a MeasurementSet"""
    return _run("imager", _explicit(locals(), ('vis', 'imagename', 'imsize', 'cell', 'datacolumn', 'savemodel', 'outlierfile', 'field', 'phasecenter_field', 'ddid', 'phasecenter', 'spw', 'channel_start', 'channel_count', 'stokes', 'specmode', 'start', 'width', 'outframe', 'veltype', 'interpolation', 'restfreq', 'restoringbeam', 'perchanweightdensity', 'dirty_only', 'niter', 'threshold', 'nmajor', 'gain', 'nsigma', 'psfcutoff', 'minor_cycle_length', 'cyclefactor', 'deconvolver', 'minpsffraction', 'maxpsffraction', 'nterms', 'hogbom_iteration_mode', 'scales', 'smallscalebias', 'usemask', 'sidelobethreshold', 'noisethreshold', 'lownoisethreshold', 'negativethreshold', 'minbeamfrac', 'growiterations', 'mask_box', 'weighting', 'mask_image', 'robust', 'wprojplanes', 'usepointing', 'write_pb', 'pbcor', 'pblimit', 'gridder', 'facets', 'parallel', 'uvrange', 'intent', 'cfcache', 'cf_resident_mb', 'pointingoffsetsigdev', 'normtype', 'fitspw', 'fitorder', 'save_continuum_residual', 'aw_cf_source', 'native_cf_cache', 'evla_surface', 'native_cf_policy', 'native_cf_working_size', 'native_cf_oversampling', 'native_cf_cache_bytes', 'native_cf_maximum_cells', 'backend', 'gridprecision', 'stage_timing', 'metal_stage_timing', 'science_trace')))

def simobserve(
    *,
    project: str | object = _UNSET,
    skymodel: StrPath | object = _UNSET,
    inbright: str | object = _UNSET,
    indirection: str | object = _UNSET,
    incell: str | object = _UNSET,
    incenter: str | object = _UNSET,
    inwidth: str | object = _UNSET,
    complist: StrPath | object = _UNSET,
    compwidth: str | object = _UNSET,
    comp_nchan: int | object = _UNSET,
    setpointings: bool | object = _UNSET,
    ptgfile: StrPath | object = _UNSET,
    integration: str | object = _UNSET,
    direction: str | list[str] | tuple[str, ...] | object = _UNSET,
    mapsize: str | list[str] | tuple[str, ...] | object = _UNSET,
    maptype: Literal['ALMA', 'ALMA-OT', 'ALMA2012', 'alma', 'hex', 'hexagonal', 'square'] | object = _UNSET,
    pointingspacing: str | object = _UNSET,
    caldirection: str | object = _UNSET,
    calflux: str | object = _UNSET,
    obsmode: Literal['', 'int', 'sd'] | object = _UNSET,
    refdate: str | object = _UNSET,
    hourangle: str | object = _UNSET,
    totaltime: str | object = _UNSET,
    antennalist: str | object = _UNSET,
    sdantlist: str | object = _UNSET,
    sdant: int | object = _UNSET,
    outframe: str | object = _UNSET,
    thermalnoise: Literal['', 'tsys-atm', 'tsys-manual'] | object = _UNSET,
    user_pwv: float | object = _UNSET,
    t_ground: float | object = _UNSET,
    t_sky: float | object = _UNSET,
    tau0: float | object = _UNSET,
    seed: int | object = _UNSET,
    leakage: float | object = _UNSET,
    correlator: Literal['', 'ACA', 'ACASpec', 'BLC', 'BLC_4x4bit'] | object = _UNSET,
    graphics: Literal['', 'both', 'file', 'none', 'screen'] | object = _UNSET,
    verbose: bool | object = _UNSET,
    overwrite: bool | object = _UNSET,
) -> TaskCompletion:
    """Simulate an observation of a sky model: CASA's simobserve"""
    return _run("simobserve", _explicit(locals(), ('project', 'skymodel', 'inbright', 'indirection', 'incell', 'incenter', 'inwidth', 'complist', 'compwidth', 'comp_nchan', 'setpointings', 'ptgfile', 'integration', 'direction', 'mapsize', 'maptype', 'pointingspacing', 'caldirection', 'calflux', 'obsmode', 'refdate', 'hourangle', 'totaltime', 'antennalist', 'sdantlist', 'sdant', 'outframe', 'thermalnoise', 'user_pwv', 't_ground', 't_sky', 'tau0', 'seed', 'leakage', 'correlator', 'graphics', 'verbose', 'overwrite')))

def imhead(
    *,
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    mode: Literal['list', 'put', 'summary'] | object = _UNSET,
    hdkey: str | object = _UNSET,
    hdvalue: str | object = _UNSET,
) -> TaskCompletion:
    """Inspect or update CASA image header metadata."""
    return _run("imhead", _explicit(locals(), ('imagename', 'mode', 'hdkey', 'hdvalue')))

def imstat(
    *,
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    box: str | object = _UNSET,
    region: str | object = _UNSET,
    chans: str | object = _UNSET,
) -> TaskCompletion:
    """Compute CASA image statistics over optional pixel, region, and channel selections."""
    return _run("imstat", _explicit(locals(), ('imagename', 'box', 'region', 'chans')))

def immoments(
    *,
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    outfile: StrPath | object = _UNSET,
    moments: Literal['-1', '0', '1', '2', '3'] | object = _UNSET,
    chans: str | object = _UNSET,
    includepix: str | object = _UNSET,
    mask: str | object = _UNSET,
    overwrite: bool | object = _UNSET,
) -> TaskCompletion:
    """Create CASA-style image moment maps"""
    return _run("immoments", _explicit(locals(), ('imagename', 'outfile', 'moments', 'chans', 'includepix', 'mask', 'overwrite')))

def exportfits(
    *,
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    fitsimage: StrPath | object = _UNSET,
    velocity: bool | object = _UNSET,
    overwrite: bool | object = _UNSET,
) -> TaskCompletion:
    """Export CASA images to FITS"""
    return _run("exportfits", _explicit(locals(), ('imagename', 'fitsimage', 'velocity', 'overwrite')))

def mstransform(
    *,
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    outputvis: StrPath | object = _UNSET,
    spw: str | object = _UNSET,
    width: int | object = _UNSET,
    field: str | object = _UNSET,
    scan: str | object = _UNSET,
    antenna: str | object = _UNSET,
    timerange: str | object = _UNSET,
    msselect: str | object = _UNSET,
    datacolumn: str | object = _UNSET,
    keepflags: bool | object = _UNSET,
) -> TaskCompletion:
    """Materialize a selected MeasurementSet into a new output MS."""
    return _run("mstransform", _explicit(locals(), ('vis', 'outputvis', 'spw', 'width', 'field', 'scan', 'antenna', 'timerange', 'msselect', 'datacolumn', 'keepflags')))

def split(
    *,
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    outputvis: StrPath | object = _UNSET,
    spw: str | object = _UNSET,
    width: int | object = _UNSET,
    field: str | object = _UNSET,
    scan: str | object = _UNSET,
    antenna: str | object = _UNSET,
    timerange: str | object = _UNSET,
    msselect: str | object = _UNSET,
    datacolumn: str | object = _UNSET,
    keepflags: bool | object = _UNSET,
) -> TaskCompletion:
    """Create a selected MeasurementSet subset, equivalent to CASA split."""
    return _run("split", _explicit(locals(), ('vis', 'outputvis', 'spw', 'width', 'field', 'scan', 'antenna', 'timerange', 'msselect', 'datacolumn', 'keepflags')))

def applycal(
    *,
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    gaintables: StrPath | list[StrPath] | tuple[StrPath, ...] | Literal['none'] | object = _UNSET,
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
    selectdata: bool | object = _UNSET,
    field: str | object = _UNSET,
    spw: str | object = _UNSET,
    antenna: str | object = _UNSET,
    scan: str | object = _UNSET,
    observation: str | object = _UNSET,
    array: str | object = _UNSET,
    timerange: str | object = _UNSET,
    msselect: str | object = _UNSET,
    flagbackup: bool | object = _UNSET,
) -> TaskCompletion:
    """Apply one or more calibration tables to a MeasurementSet."""
    return _run("applycal", _explicit(locals(), ('vis', 'gaintables', 'callib', 'gainfield', 'interp', 'spwmap', 'apply_mode', 'calwt', 'output', 'overwrite', 'parang', 'format', 'selectdata', 'field', 'spw', 'antenna', 'scan', 'observation', 'array', 'timerange', 'msselect', 'flagbackup')))

def gaincal(
    *,
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    gaintables: StrPath | list[StrPath] | tuple[StrPath, ...] | Literal['none'] | object = _UNSET,
    callib: StrPath | object = _UNSET,
    gainfield: str | object = _UNSET,
    interp: str | object = _UNSET,
    spwmap: str | object = _UNSET,
    output: StrPath | object = _UNSET,
    overwrite: bool | object = _UNSET,
    format: Literal['json', 'text'] | object = _UNSET,
    out_table: StrPath | object = _UNSET,
    refant: str | object = _UNSET,
    gain_type: Literal['g', 't'] | object = _UNSET,
    solve_mode: Literal['ap', 'p'] | object = _UNSET,
    solint: str | object = _UNSET,
    gain_combine: str | object = _UNSET,
    gain_model_source: Literal['model-column', 'point'] | object = _UNSET,
    smodel: str | object = _UNSET,
    min_snr: float | object = _UNSET,
    solnorm: bool | object = _UNSET,
    selectdata: bool | object = _UNSET,
    field: str | object = _UNSET,
    spw: str | object = _UNSET,
    antenna: str | object = _UNSET,
    scan: str | object = _UNSET,
    observation: str | object = _UNSET,
    array: str | object = _UNSET,
    timerange: str | object = _UNSET,
    msselect: str | object = _UNSET,
    minblperant: int | object = _UNSET,
) -> TaskCompletion:
    """Solve antenna gain calibration for a MeasurementSet."""
    return _run("gaincal", _explicit(locals(), ('vis', 'gaintables', 'callib', 'gainfield', 'interp', 'spwmap', 'output', 'overwrite', 'format', 'out_table', 'refant', 'gain_type', 'solve_mode', 'solint', 'gain_combine', 'gain_model_source', 'smodel', 'min_snr', 'solnorm', 'selectdata', 'field', 'spw', 'antenna', 'scan', 'observation', 'array', 'timerange', 'msselect', 'minblperant')))

def bandpass(
    *,
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    gaintables: StrPath | list[StrPath] | tuple[StrPath, ...] | Literal['none'] | object = _UNSET,
    callib: StrPath | object = _UNSET,
    gainfield: str | object = _UNSET,
    interp: str | object = _UNSET,
    spwmap: str | object = _UNSET,
    output: StrPath | object = _UNSET,
    overwrite: bool | object = _UNSET,
    format: Literal['json', 'text'] | object = _UNSET,
    out_table: StrPath | object = _UNSET,
    refant: str | object = _UNSET,
    smodel: str | object = _UNSET,
    min_snr: float | object = _UNSET,
    bandpass_combine: Literal['field', 'none', 'scan', 'scan,field'] | object = _UNSET,
    bandtype: Literal['b', 'bpoly'] | object = _UNSET,
    solnorm: bool | object = _UNSET,
    selectdata: bool | object = _UNSET,
    field: str | object = _UNSET,
    spw: str | object = _UNSET,
    antenna: str | object = _UNSET,
    scan: str | object = _UNSET,
    observation: str | object = _UNSET,
    array: str | object = _UNSET,
    timerange: str | object = _UNSET,
    msselect: str | object = _UNSET,
    degamp: int | object = _UNSET,
    degphase: int | object = _UNSET,
) -> TaskCompletion:
    """Solve bandpass calibration for a MeasurementSet."""
    return _run("bandpass", _explicit(locals(), ('vis', 'gaintables', 'callib', 'gainfield', 'interp', 'spwmap', 'output', 'overwrite', 'format', 'out_table', 'refant', 'smodel', 'min_snr', 'bandpass_combine', 'bandtype', 'solnorm', 'selectdata', 'field', 'spw', 'antenna', 'scan', 'observation', 'array', 'timerange', 'msselect', 'degamp', 'degphase')))

def fluxscale(
    *,
    output: StrPath | object = _UNSET,
    overwrite: bool | object = _UNSET,
    format: Literal['json', 'text'] | object = _UNSET,
    out_table: StrPath | object = _UNSET,
    fluxscale_input: StrPath | object = _UNSET,
    reference_fields: str | object = _UNSET,
    transfer_fields: str | object = _UNSET,
    refspwmap: str | object = _UNSET,
    gainthreshold: float | Literal['none'] | object = _UNSET,
    incremental: bool | object = _UNSET,
) -> TaskCompletion:
    """Scale gain solutions using reference calibrator fields."""
    return _run("fluxscale", _explicit(locals(), ('output', 'overwrite', 'format', 'out_table', 'fluxscale_input', 'reference_fields', 'transfer_fields', 'refspwmap', 'gainthreshold', 'incremental')))

def gencal(
    *,
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    output: StrPath | object = _UNSET,
    overwrite: bool | object = _UNSET,
    format: Literal['json', 'text'] | object = _UNSET,
    out_table: StrPath | object = _UNSET,
    caltype: Literal['antpos', 'gceff', 'opac'] | Literal['none'] | object = _UNSET,
    parameter: str | object = _UNSET,
    gaincurve_table: StrPath | object = _UNSET,
    spw: str | object = _UNSET,
    antenna: str | object = _UNSET,
) -> TaskCompletion:
    """Generate a calibration table such as antpos, gceff, or opac."""
    return _run("gencal", _explicit(locals(), ('vis', 'output', 'overwrite', 'format', 'out_table', 'caltype', 'parameter', 'gaincurve_table', 'spw', 'antenna')))

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
    *,
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    mode: Literal['clip', 'extend', 'manual', 'quack', 'rflag', 'summary', 'tfcrop'] | object = _UNSET,
    spw: str | object = _UNSET,
    field: str | object = _UNSET,
    scan: str | object = _UNSET,
    antenna: str | object = _UNSET,
    datacolumn: str | object = _UNSET,
    action: Literal['flag', 'unflag'] | object = _UNSET,
    flagbackup: bool | object = _UNSET,
    clipzeros: bool | object = _UNSET,
    quackinterval: float | object = _UNSET,
    quackmode: Literal['beg', 'end'] | object = _UNSET,
    timecutoff: float | object = _UNSET,
    freqcutoff: float | object = _UNSET,
    timedev: float | Literal['none'] | object = _UNSET,
    freqdev: float | Literal['none'] | object = _UNSET,
    extendflags: bool | object = _UNSET,
    extendpols: bool | object = _UNSET,
) -> TaskCompletion:
    """Run native CASA-style MeasurementSet flagging."""
    return _run("flagdata", _explicit(locals(), ('vis', 'mode', 'spw', 'field', 'scan', 'antenna', 'datacolumn', 'action', 'flagbackup', 'clipzeros', 'quackinterval', 'quackmode', 'timecutoff', 'freqcutoff', 'timedev', 'freqdev', 'extendflags', 'extendpols')))

def flagmanager(
    *,
    vis: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    mode: Literal['delete', 'list', 'rename', 'restore', 'save'] | object = _UNSET,
    versionname: str | object = _UNSET,
    oldname: str | object = _UNSET,
    comment: str | object = _UNSET,
    merge: Literal['and', 'or', 'replace'] | object = _UNSET,
) -> TaskCompletion:
    """Manage MeasurementSet flag-version snapshots."""
    return _run("flagmanager", _explicit(locals(), ('vis', 'mode', 'versionname', 'oldname', 'comment', 'merge')))

def impbcor(
    *,
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    pbimage: StrPath | object = _UNSET,
    outfile: StrPath | object = _UNSET,
    cutoff: float | object = _UNSET,
    box: str | object = _UNSET,
    region: str | object = _UNSET,
    chans: str | object = _UNSET,
    stokes: str | object = _UNSET,
    mask: str | object = _UNSET,
    mode: Literal['divide', 'multiply'] | object = _UNSET,
    overwrite: bool | object = _UNSET,
    stretch: str | object = _UNSET,
) -> TaskCompletion:
    """Apply primary-beam correction to a CASA image"""
    return _run("impbcor", _explicit(locals(), ('imagename', 'pbimage', 'outfile', 'cutoff', 'box', 'region', 'chans', 'stokes', 'mask', 'mode', 'overwrite', 'stretch')))

def impv(
    *,
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    outfile: StrPath | object = _UNSET,
    start: str | object = _UNSET,
    end: str | object = _UNSET,
    width: int | object = _UNSET,
    chans: str | object = _UNSET,
    overwrite: bool | object = _UNSET,
) -> TaskCompletion:
    """Extract a CASA-style position-velocity image"""
    return _run("impv", _explicit(locals(), ('imagename', 'outfile', 'start', 'end', 'width', 'chans', 'overwrite')))

def imsubimage(
    *,
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    outfile: StrPath | object = _UNSET,
    box: str | object = _UNSET,
    chans: str | object = _UNSET,
    overwrite: bool | object = _UNSET,
) -> TaskCompletion:
    """Extract a CASA-style image section"""
    return _run("imsubimage", _explicit(locals(), ('imagename', 'outfile', 'box', 'chans', 'overwrite')))

def immath(
    *,
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    expr: str | object = _UNSET,
    outfile: StrPath | object = _UNSET,
    overwrite: bool | object = _UNSET,
) -> TaskCompletion:
    """Evaluate tutorial image arithmetic expressions"""
    return _run("immath", _explicit(locals(), ('imagename', 'expr', 'outfile', 'overwrite')))

def imregrid(
    *,
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    template: StrPath | object = _UNSET,
    output: StrPath | object = _UNSET,
    interpolation: Literal['linear', 'nearest'] | object = _UNSET,
    overwrite: bool | object = _UNSET,
) -> TaskCompletion:
    """Regrid a CASA image onto a template image"""
    return _run("imregrid", _explicit(locals(), ('imagename', 'template', 'output', 'interpolation', 'overwrite')))

def feather(
    *,
    imagename: StrPath | list[StrPath] | tuple[StrPath, ...] | object = _UNSET,
    highres: StrPath | object = _UNSET,
    lowres: StrPath | object = _UNSET,
    sdfactor: float | object = _UNSET,
    overwrite: bool | object = _UNSET,
) -> TaskCompletion:
    """Combine high-resolution and low-resolution images in the Fourier domain"""
    return _run("feather", _explicit(locals(), ('imagename', 'highres', 'lowres', 'sdfactor', 'overwrite')))

def importfits(
    *,
    fitsimage: StrPath | object = _UNSET,
    imagename: StrPath | object = _UNSET,
    overwrite: bool | object = _UNSET,
) -> TaskCompletion:
    """Import a FITS primary image into a CASA image"""
    return _run("importfits", _explicit(locals(), ('fitsimage', 'imagename', 'overwrite')))

TASK_SURFACES = ('msexplore', 'calibrate', 'importvla', 'imager', 'simobserve', 'imhead', 'imstat', 'immoments', 'exportfits', 'mstransform', 'split', 'applycal', 'gaincal', 'bandpass', 'fluxscale', 'gencal', 'plotms', 'flagdata', 'flagmanager', 'impbcor', 'impv', 'imsubimage', 'immath', 'imregrid', 'feather', 'importfits')

__all__ = ['TASK_SURFACES', 'msexplore', 'calibrate', 'importvla', 'imager', 'simobserve', 'imhead', 'imstat', 'immoments', 'exportfits', 'mstransform', 'split', 'applycal', 'gaincal', 'bandpass', 'fluxscale', 'gencal', 'plotms', 'flagdata', 'flagmanager', 'impbcor', 'impv', 'imsubimage', 'immath', 'imregrid', 'feather', 'importfits']
