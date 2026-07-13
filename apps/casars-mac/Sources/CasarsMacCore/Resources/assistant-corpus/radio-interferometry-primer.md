# CASA-RS Radio Interferometry Primer

This redistribution-cleared CASA-RS primer supplies baseline terminology for
the scientific assistant. It is a concise orientation, not a substitute for a
project's cited papers or observatory documentation.

## Visibilities and baselines

A radio interferometer correlates voltages from pairs of antennas. Each pair
samples a spatial frequency whose coordinates are conventionally written
`u`, `v`, and `w` in wavelengths. Earth rotation changes the projected
baseline, filling tracks through the Fourier plane. A MeasurementSet stores
visibility samples together with time, antenna, spectral-window,
polarization, flag, weight, and coordinate metadata.

## Calibration

Calibration estimates instrumental and propagation effects from sources with
known or constrained behavior. A nearby complex-gain calibrator commonly
tracks time-dependent antenna amplitudes and phases. Bandpass calibration
models frequency-dependent response, while flux-density calibration places
amplitudes on a physical scale. Calibration quality should be assessed from
the calibrator data and transferred solutions, not inferred only from a final
image.

## Flagging and weights

Flags exclude samples judged unusable without deleting the underlying rows.
Weights describe the relative statistical contribution of unflagged samples.
Flagging, averaging, and calibration can all change the effective sampling and
noise properties, so their parameters and resulting data products belong in
the processing record.

## Imaging

Gridding maps irregular visibility samples onto a regular Fourier grid. A
weighting scheme trades sensitivity against resolution and point-spread-
function behavior. The inverse Fourier transform produces a dirty image and a
dirty beam. Deconvolution constructs a sky model whose convolution with the
dirty beam explains the data to a chosen stopping criterion. Restoration
usually combines a clean-beam-convolved model with the residual image.

Wide fields, non-coplanar baselines, mosaics, and direction-dependent effects
require additional projection or correction machinery. The correct choice is
dataset- and science-dependent and should be tied to cited task semantics and
the actual implementation in the installed CASA-RS release.

## Spectral-line work

Spectral-line analysis depends on frequency and velocity conventions,
reference frames, channel selection, continuum subtraction, and spectral
resolution. Regridding or averaging can correlate channels and alter the
effective resolution. Moment maps and position-velocity products should retain
the selections, masks, units, and coordinate conventions that generated them.

## Scientific provenance

Reproducible interpretation requires the task parameters, input identities,
software and provider versions, generated products, diagnostic outcomes, and
scientific notes. CASA-RS notebooks record these elements while keeping the
editable narrative in ordinary Markdown.
