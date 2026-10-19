# A taxonomy of interferometric imaging modes for casa-rs performance work

Truth class: non-normative research
Date: 2026-10-16
Work issue: #449 (the 10x programme); the slower modes it identifies are
tracked in their own issues.

Written 2026-10-16 from the Radio Astronomy Oracle corpus, the casa-rs
imaging foundation (plan of 2026-10-07 as landed by IF-11), the imager
parameter catalog, and CASA's `tclean` definition. No code was changed and
no runs were made for it. Speedups quoted in sections 4 and 5 are the
measurements on record on that date (the #648 evidence table and the run
receipts); later measurements are recorded on #449, not here.

Purpose: replace labels like "standard MFS Clark" with a small set of
independent axes, so that (a) measured speedups against CASA sit on a grid,
(b) each mode's cost drivers are explicit, and (c) a short benchmark set can be
chosen for the 10x programme whose target is VLASS-style AW-projection MT-MFS
at 12,150 pixels square.

Citation conventions. Corpus items are cited as `File.pdf, slide N` or
`File.pdf, p. N` (PDF page, as the corpus registry numbers them). casa-rs is
cited by file and section or line (`plan §5.3` is
`docs/imaging-architecture/imaging-foundation-plan-20261007.md`;
`ARCHITECTURE.md L567` is a line in that file; `catalog:parameter.x` is an id
in `crates/casa-provider-contracts/resources/parameter-catalog.json`). CASA is
cited as `tclean.xml L123` (`casatasks/xml/tclean.xml`) or a C++ file and
line. Where a number is my own derivation from a cited relation, it says so.

## 0. Vocabulary

- **Sample**: one selected row x channel x correlation before flagging
  (`N_s`). casa-rs grids one `Placement` per sample after flagging and
  routing (plan §5.3).
- **Pass** (major cycle): one traversal of the samples that grids `V` or
  `V − A·m` into every image domain, followed by the FFT and normalisation
  (ARCHITECTURE.md L567-586; Synthesis-Imaging-II-1998.pdf, p. 179).
- **Minor cycle**: image-plane component search and PSF subtraction between
  passes (Marvil_Imaging2026.pdf, slide 30).
- **Kernel support** `N_sup`: the width in uv cells of the convolution
  function one sample touches; the cost per sample is `N_sup²`
  (Jagannathan-Widefield.pdf, slide 33, 35).
- **Convolution function (CF) cell**: one stored kernel for one (w-plane,
  frequency, parallactic angle, antenna pair, Mueller row) key (plan §5.3
  `CfKey`, §5.6).
- **Plane**: one output spectral channel's grid; **term**: one Taylor
  coefficient grid (plan §5.1).
- **Image domain**: a main image, an outlier field or a facet, each with its
  own grid (ARCHITECTURE.md L573-575, L646-655).
- **PB**: primary beam; **flat noise** divides the image by one PB, **flat
  sky** by PB² (Jagannathan-Widefield.pdf, slide 29; tclean.xml L1275).
- `N_pix` image pixels per plane, `N_pl` planes, `N_t` Taylor terms, `N_maj`
  passes after the initial one, `N_iter` minor-cycle components.

## 1. Axes

Nine axes describe a run. Sizes (samples, pixels, planes, terms, iterations)
are not axes; they are the numbers each axis value multiplies (section 2).

### 1.1 Spectral treatment (S)

What one grid plane means along frequency.

| Value | Physics | tclean | casa-rs | Cost terms it changes | Sources |
|---|---|---|---|---|---|
| `mfs` | All channels on one grid; flat-spectrum sky assumed. Residual spectral structure leaves artefacts whose level scales with dynamic range | `specmode='mfs'`, `deconvolver` ≠ `mtmfs` or `nterms=1` | `specmode=mfs`, `nterms=1`; `Basis::Constant` | Baseline: 1 plane, 1 term | URV_Wideband_Imaging_SIW2026.pdf, slide 10, 13; Marvil_Wideband2024.pdf, slide 12 |
| `mtN` | MT-MFS: each component has a Taylor polynomial in `(ν−ν0)/ν0`; the pass grids each sample into `N_t` term grids weighted by `s^t` and forms `2N_t−1` PSFs; the minor cycle solves an `N_t×N_t` system per component; alpha and curvature products follow | `specmode='mfs'`, `deconvolver='mtmfs'`, `nterms=N`, `reffreq` | `deconvolver=mtmfs`, `nterms`; `Basis::Taylor`; Stokes I and one image domain only | Gridding x`N_t`, PSF grids `2N_t−1` on the first pass, FFTs x terms, minor cycle x`N_t²`, products (alpha, error) | Marvil_Wideband2024.pdf, slide 20-21; URV_Wideband_Imaging_SIW2026.pdf, slide 17-18; plan §5.1 table, §5.3 kernel contract; `availability.rs` L23-27 |
| `cubeN` | One plane per output channel; each plane is an independent narrow-band image with its own PSF and beam; samples route to planes by `chanMap` with nearest or linear interpolation | `specmode='cube'`/`'cubedata'`, `nchan`, `start`, `width`, `interpolation` | `specmode=cube|cubedata`, `interpolation`; `Basis::ChannelLocal`; `Partition::Planes`; waves when planes do not fit | FFTs x`N_pl`, grid memory x`N_pl`, minor cycle x`N_pl` (independent planes), per-plane beams; density grid x`N_pl` with `perchanweightdensity` | Synthesis-Imaging-II-1998.pdf, p. 243; URV_Wideband_Imaging_SIW2026.pdf, slide 7-8; plan §5.4; ARCHITECTURE.md L588-603 |
| `cubeCS` | Cube after a visibility-domain polynomial fit to line-free channels, subtracted from every channel (UVLIN). Fast; valid only near the phase centre; most channels then clean quickly | `uvcontsub` task; in casa-rs the imager's `fitspw`, `fitorder` | `fitspw`, `fitorder`, writes `CORRECTED_DATA` (plan §5.3 "visibility-domain continuum fit") | Adds one per-row, per-correlation polynomial fit over channels and a column write | Pihlstrom-SpectralLines2024.pdf, slide 44, 46; Svoboda_Spectral_Lines.pdf, slide 36, 38; Synthesis-Imaging-II-1998.pdf, p. 269 |
| `cubeE` | Ephemeris (moving) source: phase centre follows the body; background sources smear | `specmode='cubesource'`, `phasecenter=` ephemeris or `TRACKFIELD` | `specmode=cubesource` | Per-row phase-centre shift per time; same per-sample order as a fixed shift | tclean.xml L304; Synthesis-Imaging-II-1998.pdf, p. 657; catalog:parameter.specmode |
| `mvc` | MT-MFS via cube: coarse cube major cycles (5-10 intervals), per-channel PB correction, then Taylor collapse; the recommended path for wideband mosaics | `specmode='mvc'` | Not in the catalog | Cube-like passes, MT-MFS minor cycle | Marvil_Wideband2024.pdf, slide 22-23; URV_Wideband_Imaging_SIW2026.pdf, slide 23, 25; tclean.xml L456; ARCHITECTURE.md L657-658 |

`mtN` ties this axis to the deconvolver (section 1.11).

### 1.2 Visibility-to-grid operator (K)

Which convolution function places a sample. The per-sample cost is
`N_sup²` taps (Jagannathan-Widefield.pdf, slide 33), and "gridding
(degridding) runtime will rise in proportion to the support size" (tclean.xml
L744-745).

| Value | Physics | tclean | casa-rs | `N_sup` and CF count | Sources |
|---|---|---|---|---|---|
| `S` standard | Prolate spheroidal anti-aliasing kernel; real, separable; 2-D FFT of a flat sky | `gridder='standard'` | `Spheroidal`: half-support 3 (7 taps), oversampling 100; one kernel | 7x7 = 49 taps; 1 CF | tclean.xml L747; plan §5.1 table, §5.3 "seven-tap separable path"; Synthesis-Imaging-II-1998.pdf, p. 167 (`O(Mm²)`, spheroidal optimal) |
| `Wn` W-projection | Fresnel kernel `FT[e^{2πi w(n−1)}]` folded into the gridding kernel, one per w-plane; one grid, one FFT; support grows with `|w|` and with the field of view | `gridder='wproject'`, `wprojplanes=n` (`-1` automatic) | `WPlanes`; `wprojplanes` | Support "5x5 to few 100 x few 100"; CASA's auto count is `≈ 1.05·w_max · sin(cell·N/2)` planes | Jagannathan-Widefield.pdf, slide 16-17; tclean.xml L757, L984; `WPConvFunc.cc` L174-176; Interferometry and Synthesis in Radio Astronomy.pdf, p. 623-624 |
| `Fn` facets | `n×n` tangent planes, each gridded about its own centre with the standard kernel so `w` can be ignored inside a facet; one PSF from a reference facet; `N_poly ≈ 2Bλ/(f D²)` facets for full-beam imaging | `gridder='widefield'`/`facets=n` | `facets=n`; standard kernel and constant basis only; each facet is a pass domain | 49 taps x `n²` domains; `n²` FFTs | Perley-Geometry-SIW2026.pdf, slide 13; Synthesis-Imaging-II-1998.pdf, p. 419-420; tclean.xml L925; ARCHITECTURE.md L646-655; plan §5.7 |
| `M` mosaic | Kernel = `FT[PB]` per antenna-class pair and frequency, re-phased per pointing by `e^{2πi(uΔl+vΔm)}`; a weight image from `FT[PB²]`; azimuthally symmetric beams, no squint or rotation | `gridder='mosaic'`, `mosweight` | `MosaicPb`: per (frequency, antenna-class pair), Lanczos oversampling 10, cached per pointing pair | Support "between 7 and 50"; CFs = pointings x frequency buckets x antenna-class pairs | tclean.xml L791 and gridder text; Jagannathan-Widefield.pdf, slide 45; plan §5.1 table, §5.3 L470-474 |
| `AWn[pcb]` AW-projection | Kernel = `FT[A_i ⊗ A_j*]` from ray-traced apertures (feed legs, squint), times the W kernel per plane; `p` = pointing-table offsets as phase gradients, `c` = conjugate-frequency beams `√(2ν0²−ν²)`, `b` = wideband (frequency-scaled) A-terms. Image divided by the PB afterwards | `gridder='awproject'`, `wprojplanes`, `usepointing`, `pointingoffsetsigdev`, `conjbeams`, `wbawp`, `aterm`, `psterm`, `cfcache`, `computepastep`, `rotatepastep` | `AwCatalog` (CASA import or native EVLA), `cf_resident_mb`, `usepointing`, `pointingoffsetsigdev`, `aw_cf_source`; CPU only; cubes refused (#719) | Support "7 to 50", "significantly larger" with W; CFs = w-planes x frequency cells x PA cells x antenna-type pairs x Mueller rows | tclean.xml L814-817, L1064, L1073, L1099, L1121; Jagannathan-Widefield.pdf, slide 23, 26, 29; plan §5.1 table, §5.6; ARCHITECTURE.md L644-645, L691-692 |

Not in casa-rs and not in CASA's `tclean`: w-stacking and w-snapshots
(Jagannathan-Widefield.pdf, slide 16, 20-21). They are listed for
completeness; w-stacking's memory is one image per w-layer (slide 21).

When each correction is needed: the w-term matters for baselines beyond
`B_thresh = D²/(πλ)` (about 950 m for the VLA at L band; nearly every MeerKAT
baseline) (Jagannathan-Widefield.pdf, slide 13; Perley-Geometry-SIW2026.pdf,
slide 12 gives the equivalent Clark condition `λB/D² < 1`). The A-term matters
when sources sit where the beam varies with frequency (beam scales as λ,
spectral index about −1.4 at the half-power point), with time (parallactic
angle rotates the feed-leg shadows) or polarisation (squint)
(Jagannathan-Widefield.pdf, slide 23-26; URV_Wideband_Imaging_SIW2026.pdf,
slide 22; Synthesis-Imaging-II-1998.pdf, p. 132).

### 1.3 Sky geometry (G)

How many image domains and pointings a run holds.

| Value | Physics | tclean | casa-rs | Cost | Sources |
|---|---|---|---|---|---|
| `1f` | One pointing, one image | `field=` | selection | Baseline | — |
| `Kf` joint mosaic | `K` pointings gridded onto one uv grid with a phase ramp per pointing; coupled through overlapping PBs; one PSF across the mosaic; noise `σ²/Z(l,m)` with `Z = Σ PB²` | `field=` many, `gridder='mosaic'` or `'awproject'`, `mosweight` | Many fields with `M` or `AW`; per-row pointing offset in `Placement.gradient`/phase | Samples grow with `K`; CFs re-phased per pointing (`M`) or per pointing group (`AW` with `p`); density grid per field when `mosweight` | Jagannathan-Widefield.pdf, slide 38, 41, 45, 53; Plunket-Mosaicking2024.pdf, slide 29, 33-34; tclean.xml L1023 |
| OTF raster | On-the-fly: continuous scanning, many phase centres, high data rate; VLASS | `usepointing=True` with `awproject` | `usepointing`, `pointingoffsetsigdev` | As `Kf` plus per-row pointing phase gradients and antenna grouping | Plunket-Mosaicking2024.pdf, slide 27; Jagannathan-Widefield.pdf, slide 43; tclean.xml L1121 |
| `+o` outliers | Extra small image domains at bright sources outside the main field; all domains share each pass (Cotton–Schwab); their models are summed before the residual is formed | `outlierfile` | `outlierfile`; MFS single-term only; residual at native channels | Each sample placed on every domain: gridding x domains; FFTs x domains | Synthesis-Imaging-II-1998.pdf, p. 179; Jagannathan-Widefield.pdf, slide 18; tclean.xml L1625; ARCHITECTURE.md L573-582; catalog:parameter.outlierfile |
| `shift` | Phase-centre shift: a per-row phasor | `phasecenter` | `phasecenter`; `Placement.phase` | One complex multiply per sample | plan §5.3 `SampleBlock` |
| Linear mosaic | Deconvolve pointings independently, divide by PB, co-add. Shallower; not a joint operator | separate `tclean` runs plus `linearmosaic` | not an imager mode | `K` independent runs | Plunket-Mosaicking2024.pdf, slide 29, 32; Jagannathan-Widefield.pdf, slide 44 |

### 1.4 Polarisation products (P)

| Value | Physics | tclean | casa-rs | Cost | Sources |
|---|---|---|---|---|---|
| `I` | Stokes I from the parallel hands (`RR+LL` or `XX+YY`) | `stokes='I'` | `polarization=I` | 2 correlations per sample feed 1 grid polarisation | Plant-Correlators.pptx, slide 64 |
| `IQUV` / `IV` / `QU` | All four correlations needed; Q, U, V may be negative so positivity cannot constrain CLEAN; Stokes conversion is CASA's `½·Sᴴ` | `stokes='IQUV'` etc. | 4 grid polarisations; Taylor basis is Stokes I only; Metal auto is Stokes I only | Grids x4, FFTs x4, minor cycle x4 (Clark picks `max(I²+Q²+U²+V²)`), samples x2 (4 correlations) | Synthesis-Imaging-II-1998.pdf, p. 148; Marvil_Imaging2026.pdf, slide 36; tclean.xml L324, L1316-1319; plan §5.3 adjoint law; `availability.rs` L23-24, L143-146 |
| `RR`, `LL`, `XX`, `YY`, `RRLL` | Raw correlation planes | `stokes='RR'` ... | `polarization=RR|LL|XX|YY` | As the count of planes | tclean.xml L324 |
| Full-Mueller | A-projection with the 4x4 Mueller beam corrects direction-dependent leakage and squint; otherwise an unpolarised sky produces apparent polarisation off axis | `awproject` with `stokes='IQUV'` | `MuellerRouting` exists; polarised Taylor basis refused | CF Mueller planes x (routed pairs); not I-only | Jagannathan-Widefield.pdf, slide 23, 28-29, 31; Perley-Polarization-SIW2026.pdf, slide 46; plan §5.3 |

### 1.5 Weighting (W)

The PSF is the inverse FFT of the gridded weights; weighting trades
sensitivity for sidelobes and resolution (Marvil_Imaging2026.pdf, slide 22-23;
Synthesis-Imaging-II-1998.pdf, p. 158).

| Value | Physics | tclean | casa-rs | Cost | Sources |
|---|---|---|---|---|---|
| `nat` | Data weights as they are; best point-source sensitivity, broadest beam | `weighting='natural'` | `WeightingGeneration::Natural` | No density pass | Marvil_Imaging2026.pdf, slide 22 |
| `uni` | Weight ÷ local sample density per uv cell; sharper beam, more noise | `weighting='uniform'` | `Density { robust: None }` | One extra traversal reading flags and weights only, plus a density grid of `N_pix` cells | Synthesis-Imaging-II-1998.pdf, p. 158; ARCHITECTURE.md L569-571, L605-608 |
| `brR` | Briggs robust `R` in [−2, 2]: `−2` uniform, `+2` natural; the "knee" compromise | `weighting='briggs'`, `robust=R` | `robust` | As `uni` | Synthesis-Imaging-II-1998.pdf, p. 158; ALMA-Manual-Imaging-Tutorial_SISS-2026.pdf, slide 34; tclean.xml `robust` |
| `brbwR` | Briggs with a bandwidth taper for cubes (CASA `briggsbwtaper`) | `weighting='briggsbwtaper'` | `briggsbwtaper` | As `uni` | Plunket-Mosaicking2024.pdf, slide 34; plan §5.3 `bandwidth` |
| `+pcwd` | Per-channel weight density for cubes (CASA default for `specmode='cube'`) | `perchanweightdensity` | `perchanweightdensity`; CASA's rotated/unrotated lookup bug recorded | Density grid x`N_pl` | tclean.xml `perchanweightdensity`; catalog:parameter.perchanweightdensity; plan §5.3 L640-643 |
| `+mosw` | Density per field for mosaics | `mosweight=True` (default) | not a catalog entry | Density grid x fields | tclean.xml L1023; Plunket-Mosaicking2024.pdf, slide 34 |
| `+tap` | Gaussian uv taper; equivalent to smoothing the PSF | `uvtaper` | `Taper` | Per-sample multiply | Synthesis-Imaging-II-1998.pdf, p. 208-209; Marvil_Imaging2026.pdf, slide 22 |

### 1.6 Deconvolution algorithm and cycle control (D)

| Value | Physics | tclean | casa-rs | Cost per component / per cycle | Sources |
|---|---|---|---|---|---|
| `dirty` | No minor cycle: PSF, dirty image, weight image only | `niter=0` | `dirty_only` or `niter=0` | One pass | tclean.xml `niter` |
| `hog` Högbom | Find the peak, subtract a shifted full-size PSF, repeat; one major cycle in the original algorithm; CASA runs it inside Cotton–Schwab passes | `deconvolver='hogbom'` | `Hogbom` with CASA's inclusive loop | Per component: `O(N_pix)` (PSF of image size) | Marvil_Imaging2026.pdf, slide 28, 30; tclean.xml L1309-1312 |
| `clk` Clark | Minor cycle on a list of pixels above the highest exterior sidelobe using a PSF patch; a Clark major cycle refreshes the residual by FFT | `deconvolver='clark'` | `Clark`: active list, exact refresh by FFT or direct sparse sums, whichever is cheaper | Per component `O(patch²)`; per Clark cycle one FFT pair of `N_pix` or a sparse sum | Synthesis-Imaging-II-1998.pdf, p. 179; Interferometry and Synthesis in Radio Astronomy.pdf, p. 595; tclean.xml L1316-1319; plan §5.5 |
| `ms[s…]` multiscale | Components are deltas and paraboloids/Gaussians of the listed scales (pixels); residual smoothed per scale; peak over scales with `smallscalebias` | `deconvolver='multiscale'`, `scales`, `smallscalebias` | `Multiscale` with a `ScaleBank` | Per cycle `N_scales` smoothings of `N_pix`; per component patch x scales | Svoboda_Spectral_Lines.pdf, slide 41; Marvil_Imaging2026.pdf, slide 30; tclean.xml L1335; plan §5.5 |
| `mt-ms[s…]` | MT-MFS multiscale: `N_t` residual terms, `2N_t−1` PSF terms, per-scale `N_t×N_t` Hessian solve per component | `deconvolver='mtmfs'`, `scales`, `nterms` | `Taylor` solver | As `ms` x `N_t²` | Marvil_Wideband2024.pdf, slide 20-21; tclean.xml L1343-1352; plan §5.5 |
| `asp`, `mem`, `clarkstokes` | Adaptive-scale pixels; maximum entropy; Clark on Stokes planes | `deconvolver='asp'|'mem'|'clarkstokes'` | not in the catalog (`asp` reserved by `Candidate`) | — | Svoboda_Spectral_Lines.pdf, slide 42; tclean.xml L1309; plan §5.5 |
| Cycle control | `cyclethreshold = max(peak · clamp(sidelobe · cyclefactor, minpsffraction, maxpsffraction), threshold, nsigma·rms)`; `cycleniter` per plane; `nmajor`; a cycle ends when its threshold or iteration cap is reached. Divergence is answered by more frequent major cycles | `niter`, `threshold`, `nsigma`, `cyclefactor`, `cycleniter`, `minpsffraction`, `maxpsffraction`, `nmajor`, `gain` | same names; `Controller` (SIIterBot) and per-plane `PlaneControl` | Sets `N_maj` and `N_iter`, hence every repeated term | tclean.xml L1949-1952, L1969, `cycleniter`; Butterfield-ErrorRecognition-SIW2026.pptx, slide 37; plan §5.5 L833-840 |

Cotton–Schwab (subtract components from the ungridded data every major
cycle) is what every CASA and casa-rs clean does; it is "often faster than
the Clark CLEAN, the major exception occurring for data sets with a large
number of visibility samples, where gridding over and over again becomes
prohibitively expensive" (Synthesis-Imaging-II-1998.pdf, p. 179-180). That
sentence is the whole reason `N_maj` is a cost axis.

### 1.7 Normalisation and primary-beam handling (N)

| Value | Physics | tclean | casa-rs | Cost | Sources |
|---|---|---|---|---|---|
| `fn` flat noise | Residual ÷ `pbmax·√weight` where above `pblimit·pbmax²`; the minor cycle sees sky x PB; noise flat | `normtype='flatnoise'` (default) | `normtype=flatnoise`; model x PB before degridding, ÷ PB after the solve | Per-pixel divides per pass; one weight-image grid (`FT[PB²]`) in the first pass | tclean.xml L1275; plan §5.7 table; Jagannathan-Widefield.pdf, slide 29 |
| `fs` flat sky | Residual ÷ weight; noise rises at the PB edge; Taylor family uses the flat-noise cutoff (recorded CASA quirk) | `normtype='flatsky'` | `normtype=flatsky` | As `fn` | tclean.xml L1275; plan §5.7 L883-890 |
| `pbsq` | No normalisation; minor cycle sees sky x PB² | `normtype='pbsquare'` | not in the catalog | — | tclean.xml L1275 |
| `pblX` | PB cutoff `X` for divides and the T/F mask; negative leaves uncorrected products unmasked | `pblimit` | `pblimit` | Per pixel | tclean.xml `pblimit`; catalog:parameter.pblimit |
| `pbc` | `.image.pbcor = .image ÷ .pb` above `pblimit` | `pbcor=True` | `pbcor`, `write_pb` | One extra product of `N_pix × N_pl` | tclean.xml L1575-1576; plan §5.7 |
| Wideband PB | PB spectral index (about −1.4 at the half-power point) must be undone before a wideband sky model: `conjbeams` during gridding, `mvc`, or `widebandpbcor` afterwards | `conjbeams`, `specmode='mvc'`, `widebandpbcor` | `conjbeams` through the AW catalog; `widebandpbcor` task | CF frequency cells x2 (conjugate map) | URV_Wideband_Imaging_SIW2026.pdf, slide 22-25; Synthesis-Imaging-II-1998.pdf, p. 455; plan §5.6 |

Standard and W-projection runs have no PB in the operator; they normalise by
`sumwt` only (plan §5.7 row "dirty/residual (standard)"). So this axis is
live only for `M` and `AW`, and for `pbc`/`write_pb` on the others.

### 1.8 Masking (M)

| Value | Physics | tclean | casa-rs | Cost | Sources |
|---|---|---|---|---|---|
| `m0` | Whole image is the support | `usemask='user'`, `mask=''` | `mask=''` | — | — |
| `mu` | User box, region, or mask image; constrains where components are picked | `usemask='user'`, `mask=` | `usemask=user`, `mask`, `mask_box`, `mask_image` | Peak search over the mask only | Marvil_Imaging2026.pdf, slide 30; tclean.xml `usemask` |
| `ma` | Auto-multithresh: mask grown from sidelobe and S/N thresholds, pruned by `minbeamfrac`, dilated to `lownoisethreshold`; recomputed every major cycle; needs a robust noise estimate (`fastnoise`) | `usemask='auto-multithresh'`, `noisethreshold`, `sidelobethreshold`, `lownoisethreshold`, `negativethreshold`, `minbeamfrac`, `fastnoise` | same names | Per major cycle: smoothing, thresholding, pruning and dilation over `N_pix × N_pl`, plus image statistics | Svoboda_Spectral_Lines.pdf, slide 44-46, 49; ALMA-Manual-Imaging-Tutorial_SISS-2026.pdf, slide 38; tclean.xml `fastnoise` |
| `mpb` | PB mask at a gain level | `usemask='pb'`, `pbmask` | not in the catalog | — | tclean.xml `usemask` |

### 1.9 Visibility side effects (X)

| Value | Physics | tclean | casa-rs | Cost | Sources |
|---|---|---|---|---|---|
| `-` | MS opened read-only | `savemodel='none'` | default | — | tclean.xml `savemodel` |
| `wM` | Final pass predicts the model at native channels and writes `MODEL_DATA` (for self-calibration) | `savemodel='modelcolumn'` | `savemodel=modelcolumn`; every plane must be resident in the final pass; excludes Metal `auto` | One extra prediction pass plus a column write of `N_s × 8` bytes | tclean.xml `savemodel`; ARCHITECTURE.md L570-571, L601-603, L663-666; MarvilSelfcal2026.pdf, slide 7 |
| `wC` | Continuum-subtracted `CORRECTED_DATA` written by the fit of 1.1 | `uvcontsub` | `fitspw`/`fitorder` write-back | Column write of `N_s × 8` bytes plus the fit | plan §5.3 L651-652; Svoboda_Spectral_Lines.pdf, slide 36 |
| virtual model | Store the model and gridder state in the MS, degrid on demand | `savemodel='virtual'` | not in the catalog | — | tclean.xml `savemodel` |

### 1.10 Size parameters

These are not modes but they decide which cost term wins (section 2.3).

- `N_s`: rows x channels x correlations selected. VLA: "351 baselines, 1000+
  channels every 2-5 seconds = 200M samples/hour" (Marvil_Imaging2026.pdf,
  slide 5). Channel averaging is bounded by bandwidth smearing,
  `δν < ν0·D/b_max` (1 MHz for VLA A, 30 MHz for D)
  (URV_Wideband_Imaging_SIW2026.pdf, slide 41), so `N_s` is set by the
  science, not by the imager.
- `N_pix`: image pixels per plane. Cell oversamples the synthesised beam by
  4-5 (Marvil_Imaging2026.pdf, slide 14) or 5-7
  (Butterfield-ErrorRecognition-SIW2026.pptx, slide 41); the field is the
  full primary beam, or the mosaic. The uv cell is `1/(N·Δl)`
  (Marvil_Imaging2026.pdf, slide 15), so for a fixed cell a larger image means
  finer uv cells and wider kernels in cells (section 2.1).
- `N_pl`, `N_t`, `N_dom` (image domains), `N_pol`.
- `N_maj` and `N_iter`: outputs of the cycle controller, inputs to cost.

### 1.11 Where the axes are not orthogonal

| Coupling | What it does | Source |
|---|---|---|
| `mtN` ties S to D | `deconvolver='mtmfs'` is both the spectral model and the minor-cycle algorithm; `nterms` sets the number of grids on both sides | tclean.xml L1343; plan §5.1 |
| `AW`/`M` tie K to N and G | Only PB-aware kernels produce a weight image, so `flatnoise`/`flatsky`/`pblimit` are live only for them; a joint mosaic needs one of them; the weight image is itself one more grid in the first pass | plan §5.1 table, §5.7; Plunket-Mosaicking2024.pdf, slide 34 |
| `AW` + `mt2` + `conjbeams` | The conjugate beam removes the PB's spectral index so the Taylor terms model the sky only; without it `alpha` carries the PB's −1.4 | URV_Wideband_Imaging_SIW2026.pdf, slide 22-23; tclean.xml L1073 |
| `cube` + `uni`/`br` + `+pcwd` | Per-channel density changes the weighting from one density grid to `N_pl` | tclean.xml `perchanweightdensity` |
| `Kf` + `br` + `+mosw` | Per-field density grids | tclean.xml L1023 |
| `F` restricted | Facets: standard kernel and constant basis only in casa-rs; CASA refuses AW and mosaic facets | ARCHITECTURE.md L655-657; `availability.rs` L35-40 |
| `AW` + `cube` | Refused in casa-rs until per-plane weights have a CASA comparison | ARCHITECTURE.md L644-645 |
| Backend | Metal grids the standard kernel set only, Stokes I, one domain, no visibility write; `gridprecision` f64 on CPU by default | ARCHITECTURE.md L660-692 |
| D and S | Planes are independent in cubes (one thread each); Taylor terms are solved jointly; a Clark minor cycle on one plane is spread across the team instead | plan §5.5 L826-829 |
| `wM` and residency | Writing visibilities forces every plane resident in the final pass | ARCHITECTURE.md L601-603 |

## 2. Cost model

### 2.1 Cost terms

Symbols: `N_s` samples after flagging, `N_sup` kernel width in cells,
`N_mu` Mueller planes routed per sample (1 for Stokes I), `N_t` terms,
`N_pix`, `N_pl`, `N_dom`, `N_pol`, `N_maj` residual passes, `N_iter`
components, `N_sc` scales, `N_cf` distinct CF cells.

| Term | Scaling | Notes and sources |
|---|---|---|
| **G** gridding and degridding | `N_s · N_sup² · N_mu · N_dom · (N_t grid + N_t degrid) · (1 + N_maj)`; the first pass also grids `2N_t−1` PSF terms and, for `M`/`AW`, one weight grid | Gridding cost `∝ N_vis × N_sup²`: prolate spheroidal 3x3 to 7x7, A-term about 9x9, W-term up to 200x200 (4000x the spheroidal); "approximately 80% of total imaging cost" (Jagannathan-Widefield.pdf, slide 35-36; Jagannathan-Widefield2024.pdf, slide 26); `O(Mm²)` (Synthesis-Imaging-II-1998.pdf, p. 167); casa-rs kernel contract (plan §5.3) |
| **W-kernel support** | `N_sup(W) ≈ w · θ_FoV²` cells (my derivation: the Fresnel phase `≈ π w θ²` (Jagannathan-Widefield.pdf, slide 13) has local uv frequency `w·θ`, so the kernel spans `w·θ_FoV` wavelengths, and a uv cell is `1/θ_FoV` (Marvil_Imaging2026.pdf, slide 15)) | For a fixed cell size, going from 4,096 to 12,150 pixels multiplies `θ_FoV` by 2.97 and `N_sup(W)` by 8.8, so `N_sup²` by about 78 for samples whose kernel is W-dominated. "Kernel support scales with `w_max`" (Jagannathan-Widefield.pdf, slide 16) |
| **A-kernel support** | `N_sup(A) ≈ (2D/λ) · θ_FoV` cells (aperture autocorrelation diameter over the uv cell) | Grows linearly with image side at fixed cell: x2.97 from 4,096 to 12,150 pixels, `N_sup²` x8.8. "Support size is set by how far out in the primary beam you need to go" (Jagannathan-Widefield.pdf, slide 35) |
| **C** CF generation and storage | Generation `∝ N_cf · N_sup² · oversampling²` (FFT of the aperture or PB screen per cell); storage `N_cf · N_sup² · oversampling² · 8` bytes; load cost per cell miss | CASA fills the cache at the start of a run and warns it "can be time consuming" (tclean.xml L1099); `N_cf` = w-planes x frequency cells x PA cells (`computepastep`, `rotatepastep`) x antenna-type pairs x Mueller rows (plan §5.6; tclean.xml L1127-1128); casa-rs keeps an in-memory LRU bounded by `cf_resident_mb` and walks one DATA_DESCRIPTION at a time so one CF group is resident (plan §5.6; catalog:parameter.cf_resident_mb). Mosaic: `MosaicPb` builds per (frequency, antenna-class pair) at Lanczos oversampling 10 and re-phases per pointing pair (plan §5.3 L472-474) |
| **F** FFTs | `(1 + N_maj) · N_dom · N_pol · N_pl · N_t · 2 · c · N_pix log₂ N_pix`, plus `2N_t−1` PSF FFTs once; "a few times `N² log₂ N`" per transform | Synthesis-Imaging-II-1998.pdf, p. 153; Marvil_Imaging2026.pdf, slide 13; casa-rs: per-plane FFTs run single-threaded on each worker, FFTW only (ARCHITECTURE.md L593, L641) |
| **D** minor cycles | Högbom `N_iter · N_pix`; Clark `N_iter · patch² + N_cyc · F(N_pix)`; multiscale adds `N_sc` smoothings of `N_pix` per cycle and `N_sc` candidates per step; MT-MFS multiplies by `N_t²` and keeps `2N_t−1` PSF terms; cubes multiply by `N_pl` (independent planes) | tclean.xml L1309-1352; Synthesis-Imaging-II-1998.pdf, p. 179; plan §5.5. Peak search over the mask is `O(N_pix)` per step unless incremental, which at `1.5e8` pixels and `2e4` components is `3e12` compares, the same order as gridding (my arithmetic; see 2.4) |
| **P** normalisation, masks, products | Per pass: divides over `N_pix · N_pl · N_pol`; weight/PB images; `ma` costs smoothing, thresholding, dilation per major cycle; restoration one convolution per plane; writers `N_pix · N_pl · 4` bytes per product (19 products for the VLASS recipe) | plan §5.7; Svoboda_Spectral_Lines.pdf, slide 44-49; `vlass-imaging-recovery-contract.md` ("exact 19-product inventory") |
| **I** I/O and memory | MS read: `N_s · (8 data + 4 weight + 1 flag)` bytes per traversal, `(1 + N_maj + density pass)` traversals; column writes `N_s · 8`; grid memory `N_pix · N_pl · N_pol · N_t · (8 or 16) · N_workers_regions` bytes; cube state paged when it does not fit; CASA at 12,150² peaked at 12.8-13.8 GB RSS and swapped hundreds of GB cumulatively | ARCHITECTURE.md L567-603 (two-slot stream, waves, residency); plan §5.4; `vlass-imaging-recovery-contract.md` L263-269, L318-323 |

Per-sample fixed costs that no axis removes: flag test, phase shift, Stokes
routing, weight lookup, placement rounding (plan §5.3 `Placement`,
`SampleBlock`). They set the floor for `S` kernels and are what the Wave3 row
measures.

### 2.2 What each axis value does to the terms

| Axis value | G | C | F | D | P | I |
|---|---|---|---|---|---|---|
| `mfs` → `mtN` | x`N_t` (grid and degrid), first pass `2N_t−1` PSFs | CF unchanged; conjugate frequency map doubles frequency cells with `c` | x`N_t` (+PSF terms once) | x`N_t²` per component; `N_t` residual, `2N_t−1` PSF planes resident | alpha, error, tt products | grids x`N_t` |
| `mfs` → `cubeN` | same per sample; samples route to planes | `M`/`AW` kernels per frequency already | x`N_pl` | x`N_pl` independent | per-plane beams; x`N_pl` products | grids x`N_pl`; waves and paging when over budget |
| `cube` → `cubeCS` | — | — | — | fewer components (most channels empty) | — | fit per row; `CORRECTED_DATA` write |
| `S` → `Wn` | x`(N_sup(W)/7)²`, complex taps | `n` kernels of growing support | — | — | — | CF memory |
| `S` → `Fn` | x`n²` domains at 49 taps | — | x`n²` smaller FFTs | one joint image | stitch by `sumwt` | — |
| `S` → `M` | x`(N_sup/7)²`, complex; plus weight grid once | per (frequency, antenna-class pair) x pointings | +1 weight FFT | — | PB normalisation, `.pb`, `.weight` | CF cache |
| `S` → `AWn[pcb]` | x`(N_sup/7)² · N_mu`; weight grid once | `N_cf` = planes x frequency cells x PA cells x pairs x Mueller; generation or import; LRU misses | +1 weight FFT | — | PB normalisation | CF cache bytes; CPU only |
| `1f` → `Kf` | `N_s` grows with pointings | re-phased kernels per pointing (`M`) / pointing groups (`AW` `p`) | one grid | one image | — | — |
| `+o` | x`N_dom` | — | x`N_dom` | per domain | per domain | — |
| `I` → `IQUV` | x2 samples (4 correlations), x4 grid pols | Mueller planes | x4 | x4 (joint Clark peak) | x4 | x4 |
| `nat` → `uni`/`br` | — | — | — | — | — | +1 traversal (flags and weights only), density grid `N_pix` (x`N_pl` with `+pcwd`, x fields with `+mosw`) |
| `dirty` → `hog`/`clk`/`ms` | repeated `N_maj` times | — | repeated | as 2.1 | restoration | repeated MS reads |
| `fn` ↔ `fs` | — | — | — | — | same order | — |
| `m0` → `ma` | — | — | — | smaller search | per-cycle image ops | — |
| `-` → `wM`/`wC` | +1 prediction pass (degrid only) | — | — | — | — | column write; all planes resident |

### 2.3 Cost regimes

Six regimes cover the combinations that matter. Each is recognised from
numbers a run summary already has: samples, pixels, planes, terms, kernel
width, passes, iterations.

| Regime | Dominant term | Recognise it when | Levers |
|---|---|---|---|
| **R1 sample-dominated, narrow kernel** | G with `N_sup = 7` | `N_s · 49 · (1+N_maj)` ≫ `50 · N_pix log N_pix · N_pl · (1+N_maj)`, i.e. samples per output pixel per pass ≳ 20 and `K = S` | Per-sample fixed costs, memory bandwidth of grid updates, worker partition (planes vs regions), Metal |
| **R2 kernel/CF-dominated** | G with `N_sup ≥ 15`, plus C | `K ∈ {W, M, AW}` and a large field: `N_sup²` of 200-40,000; or `N_cf` large relative to samples per cell (cache misses) | Kernel storage layout, tap loop, CF residency, dropping Mueller planes not routed, pass order by CF group |
| **R3 pixel-dominated** | F and P | samples per pixel per pass ≲ 10 and `N_pix ≥ 1.7e7`; cost roughly constant when `N_s` changes (the pilot rows: x4 samples gave 44.8 → 46.5 s) | FFT plan, normalisation passes, product writers, f32 vs f64 grids |
| **R4 deconvolution-dominated** | D | `N_iter ≥ 1e4`, or `N_iter · N_pix · N_sc` comparable to G; many Clark cycles each refreshing by FFT; deep thresholds | Active lists, incremental peak search, patch size, cycle control, threads per plane |
| **R5 memory/I/O-bound** | I | `N_pix · N_pl · N_t · N_pol · bytes` above about a quarter of RAM (waves, paging, swap); or `wM`/`wC`; or `N_s · 13 B · traversals` beyond disk bandwidth x wall | Residency plan, grid precision, column write path, read-ahead |
| **R6 fixed-cost/startup-dominated** | process start, table open, CF generation, plan time | wall under about 30 s; ratios vary run to run by 2x | Not usable for speedup claims; diagnostic only |

Worked recognition on the current large rows (numbers from the brief;
arithmetic mine):

| Row | `N_s` | `N_pix · N_pl` | samples/pixel/pass | passes | G tap-updates (`N_s·N_sup²·passes`) | Regime |
|---|---|---|---|---|---|---|
| Cube 512 x 1024², Clark natural | 4.19e9 | 5.37e8 | 7.8 | 18 | 3.7e12 (real, separable) | R1 with R5 pressure (4.3 GB of f32 grids per set; `Partition::Planes`) |
| Wave3 MFS 4096², Clark uniform | 4.31e9 | 1.68e7 | 257 | 25 (incl. density) | 5.1e12 | R1, pure: one plane, all workers on one grid (`Partition::Regions`) |
| Pilot 4 SPW MFS 4096², Clark uniform | 6.47e7 | 1.68e7 | 3.9 | ~10 | 3.2e10 | R3/R4: pixels and 10,000 components, not samples |
| Pilot 16 SPW, 5 mJy | 2.59e8 | 1.68e7 | 15 | fewer | 1.3e11 | R3/R4 (casa-rs 46.5 s vs 44.8 s with 4x samples) |
| VLASS 63 fields 16 SPW AW MT-MFS 4096² | 1.68e8 | 1.68e7 | 10 | ~13 (the 4-SPW rows ran 11-12 majors; this row's count is not in the brief) | 1.68e8 · N_sup² · 2 terms · 13 ≈ 4.4e9 · N_sup² → 1e12–4e12 for `N_sup` 15–30 | R2 (AW kernels, 32 planes, 16 SPW x 64 channel frequency cells), CPU, 1 worker |

### 2.4 Scaling the regimes to the 12,150-pixel VLASS target

Same data (167.7 M samples), same cell (0.6"), 2.97x the image side:

- **F, P, I**: `N_pix` x8.8, `N_pix log N_pix` x9.9. One complex f64 grid
  plane is 2.36 GB; an MT-MFS first pass holds 2 data + 3 PSF + 1 weight
  grids, so 14 GB in f64 or 7 GB in f32 for one worker, before images,
  FFT scratch and CF cells. On a 32 GB laptop this is R5 territory;
  `gridprecision` and the number of region workers decide whether the first
  pass fits (ARCHITECTURE.md L667-670; plan §5.4). CASA's own 12,150² runs
  peaked at 12.8-13.8 GB RSS with 361-411 GB of swap-in
  (`vlass-imaging-recovery-contract.md` L263-269, L318-323).
- **G**: the A-term support grows x2.97 (so `N_sup²` x8.8); W-dominated
  kernels grow x8.8 in width (`N_sup²` x78). The per-sample kernel cost
  therefore rises between 9x and 78x depending on how many samples are
  W-dominated at 12,150². CASA's own ratio between its 4096² and 12,150²
  63-field clean references is 82,352 / 9,074 = 9.1, which matches the
  pixel-count and A-term scalings and not the W-term extreme; whether that is
  because few samples are W-dominated or because CASA is pixel-bound there is
  open (section 6). Reading the frozen CF cache headers (support per cell,
  which `AwCatalog::open_casa` reads at open; plan §5.6) would settle the
  kernel widths without a run.
- **D**: `niter` 20,000 (casa-b) on 1.48e8 pixels with 3 scales and 2
  terms: a non-incremental peak search is `2e4 · 1.5e8 · 3 ≈ 9e12`
  compares, more than the gridding. CASA's casa-b selected only 444
  components across eight minor cycles before its n-sigma stop
  (`vlass-imaging-recovery-contract.md` L231-233), so in CASA's run D was
  small; a casa-rs run that cleans deeper would move into R4.
- **C**: `N_cf` is unchanged (same planes, frequency cells, PA cells) but
  each cell is `N_sup²·oversampling²` larger: 9-78x the bytes, so the
  resident set under `cf_resident_mb` shrinks and misses rise.

So the 12,150² rows are R2 with R5 pressure and a possible R4 tail; the
4096² rows are R2 alone. A benchmark set for the 10x goal must therefore
measure the kernel width growth (R2), the first-pass memory plan (R5) and the
minor cycle at `1.5e8` pixels (R4) separately.

## 3. Mode signature

A signature is nine slash-separated fields in axis order, then a size tag:

```
S / K / G / P / W / D / N / M / X  @ side²·cell · samples · planes
```

| Field | Tokens |
|---|---|
| S | `mfs`, `mtN` (Taylor terms), `cubeN` (planes), `cubeCS` (continuum-subtracted), `cubeE` (ephemeris), `mvc` |
| K | `S`, `Wn` (w-planes; `Wa` automatic), `Fn` (n x n facets), `M`, `AWn` + flags `p` (pointing table), `c` (conjugate beams), `b` (wideband A) |
| G | `1f`, `Kf` (K joint pointings), `+o` (outliers), `+sh` (phase-centre shift) |
| P | `I`, `IQUV`, `IV`, `QU`, `RR`, `RRLL`, `XX`, ... |
| W | `nat`, `uni`, `brR` (robust R), `brbwR`, flags `+pcwd`, `+mosw`, `+tap` |
| D | `dirty`, `hog`, `clk`, `ms[s1,s2,…]`, `mt-ms[s…]`; then `nN` (niter), `tX` (threshold), `σN` (nsigma), `MN` (major cycles actually run) |
| N | `fn`, `fs`, `pbsq`; `pblX`; `pbc` |
| M | `m0`, `mu`, `ma`, `mpb` |
| X | `-`, `wM`, `wC` |
| size | `@4096²·0.6"·168M·1pl`; `·Wk` workers and `·cpu|gpu` when timing |

Defaults may be dropped: `1f`, `I`, `fn`, `m0`, `-`. Pixel count and
samples stay, because they decide the regime.

Examples:

- `mt2/AW32pcb/63f/br1/mt-ms[0,5,12] n2000/mu @4096²·0.6"·168M·cpu W1`
  — the 1.90x VLASS row.
- `cube512/S/nat/clk t0.5mJy M17 @1024²·4.19G·512pl·gpu W4` — the 5.22x cube.
- `mfs/S/uni/clk n10000 t5mJy M23 @4096²·0.8"·4.31G·gpu W4` — Wave3.
- `cube256CS/S/nat/dirty @512²·1.2M·256pl/wC` — the ALMA continuum-subtracted
  row (the `wC` is implied by `cubeCS` and may be dropped).
- `cube18/W8/2f/nat/dirty @512²·6.1M·18pl` — the small W-projection cube
  with two fields sharing a phase centre.
- `mfs/M/7f/br0.5/dirty/fn pbl0.2 @512²·8.4M` — ALMA mosaic dirty.

## 4. Placement of the current benchmark rows

Speedup = CASA wall / casa-rs wall. "Equal work" notes where the two ran
different numbers of major cycles; TESTING.md L509-512 asks for `--nmajor`
caps in that case.

### 4.1 Large rows

| # | Signature | `N_s` | `N_pix`·planes | Regime | Speedup | Notes |
|---|---|---|---|---|---|---|
| L1 | `cube512/S/nat/clk t0.5mJy M17 @1024²·4.19G·512pl·gpu W4` | 4.19e9 | 1.05e6 x 512 | R1 (+R5) | 5.22x (4,144 vs 21,623 s) | Per-plane partition, no merge; 4.3 GB f32 grids per set |
| L2 | `mfs/S/uni/clk n10000 t0.5mJy @4096²·0.05"·64.7M·cpu W1` | 6.47e7 | 1.68e7 | R3/R4 | 5.49x (44.8 vs 246 s) | 3.9 samples per pixel; density pass |
| L3 | `mfs/S/uni/clk n10000 t5mJy @4096²·0.05"·259M·cpu W1` | 2.59e8 | 1.68e7 | R3/R4 | 2.88x (46.5 vs 134 s) | casa-rs +4%, CASA −46% for 4x samples and a 10x shallower threshold: casa-rs is pixel-bound here, CASA was cycle-bound |
| L4 | `mfs/S/uni/clk n10000 t5mJy M23 @4096²·0.8"·4.31G·gpu W4` | 4.31e9 | 1.68e7 | R1 | 1.71x (4,654 vs 7,945 s) | Not equal work: 23 vs 15 majors; dividing by majors + 1 gives casa-rs 194 s and CASA 497 s per pass (about 2.6x, my arithmetic). The weakest large standard-kernel row |
| L5 | `mt2/AW32pcb/63f/br1/mt-ms[0,5,12] n2000/mu @4096²·0.6"·168M·cpu W1` | 1.68e8 | 1.68e7 | R2 | 1.90x (4,765 vs 9,074 s) | AW on CPU only; one worker |

### 4.2 Small rows (512², 6.3 M samples unless stated; R6 for all)

| Signature | Speedup | Reading |
|---|---|---|
| `mfs/S/nat/dirty` | 1.9x | fixed-cost floor |
| `mfs/S/br0.5/hog n25/mu` | 2.1x | |
| `mfs/S/br0.5/clk n25/mu` | 2.3x | |
| `mfs/S/br0.5/ms n25/mu` | 2.1x | |
| `mfs/S/br0.5/hog n25/mu(box)` | 2.1x | |
| `mfs/S/br0.5/hog n25/ma` | 1.7x | automask per cycle costs more in casa-rs at this size |
| `mfs/S/br0.5/hog n25/mu/wM ·W4` | 0.67x | R5: column write; slower than CASA |
| `cube24/S/nat/dirty ·24pl` | 2.3x | |
| `cube24/S/nat/hog n2 ·24pl` | 2.1x | |
| `mfs/Wa/nat/dirty` | 1.1x | CF generation dominates at this size |
| `mfs/Wa/nat/hog/mu/wM` | 0.63x | R5 + CF |
| `mfs/F2/nat/dirty` and `.../hog` | 1.7x / 2.1x | |
| `cube18/W8/2f/nat/dirty @512²·6.1M·18pl` | 1.7x | two fields sharing a phase centre |
| `cube256CS/S/nat/dirty @512²·1.2M·256pl` (ALMA) | 3.8x | fit + `CORRECTED_DATA` |
| `mfs/M/7f/br/dirty @512²·8.4M` (ALMA) | 0.51x | R2/R6: mosaic CF generation per (frequency, antenna pair, pointing) dominates; casa-rs slower |
| `cube16/M/7f/br/dirty @512²·16pl` (ALMA) | 0.76x | same |
| `mt2/AW32pcb/9f/br1/dirty @512²·6.0M` | 45x | casa-rs cache warm, CASA cache state unrecorded: not a comparison |
| `mt2/AW32pcb/9f/br1/mt-ms n30/mu @512²·6.0M` | 19x | same caveat; the drop from 45x says the minor cycle share is large in casa-rs |

### 4.3 Frozen CASA references without a current casa-rs time

| ID | Signature | CASA wall | casa-rs |
|---|---|---|---|
| casa-c | `mt2/AW32pcb/63f/br1/dirty @4096²·0.6"·41.9M` | 541 s | 225 s on an old build (2.4x; contract L339-351); not rerun on the IF build |
| casa-d | `mt2/AW32pcb/63f/br1/mt-ms[0,5,12] n2000 M11/mu @4096²·41.9M` | 3,470 s | 257 s on a retired Metal replay build (13.5x; contract L624-628); not rerun |
| casa-g | as L5 | 9,074 s | 4,765 s (L5) |
| casa-a | `mt2/AW32pcb/1f/br1/mt-ms[0,5,12] n2000 M9/mu @12150²·0.6"·2.7M` | 3,798 s | none |
| casa-b | `mt2/AW32pcb/63f/br1/mt-ms[0,5,12] n20000 M?/mu @12150²·168M` | 82,352 s | none |
| landmark | `mt2/AW32pcb/1f/br1/mt-ms[0,5,12] n2000/mu @4096²·0.67M (4 SPW)` | 3,631.8 s (contract L550-552) | 28.65 s on the retired build |

The contract's four acceptance rows are single-field and 63-field, dirty and
clean, at 12,150²; only the two clean ones have frozen CASA times.

### 4.4 Coverage matrix

"Large" = a measured speedup on a row in regimes R1-R5; "small" = 512² only
(R6); "none" = no timed comparison.

| Axis | Value | Coverage |
|---|---|---|
| S | `mfs` | large (L2-L4) |
| S | `mt2` | large (L5) |
| S | `mtN>2` | none |
| S | `cubeN` | large (L1, natural, Clark, standard) |
| S | `cubeCS` | small (ALMA 256 ch) |
| S | `cubeE` | none timed (T2 row `moving-source-cube-alma` exists for correctness) |
| K | `S` | large |
| K | `Wn` | small only (dirty, clean, cube); casa-rs ≤ 1.1x |
| K | `Fn` | small only |
| K | `M` | small only; casa-rs slower (0.51x, 0.76x) |
| K | `AW32pcb` | large (L5; CPU, 1 worker); dirty and single-field only on old builds |
| G | `1f` | large |
| G | `63f` OTF | large (L5) |
| G | `+o` | none timed (T2 `multidomain-outlier-vla` correctness only) |
| P | `I` | large |
| P | `IQUV` | none timed (T2 `full-stokes-alma` correctness only) |
| W | `nat` | large (L1) |
| W | `uni` | large (L2-L4) |
| W | `br` | large (L5), small (0.5) |
| W | `+pcwd`, `brbw`, `+tap`, `+mosw` | none |
| D | `dirty` | small; large only through casa-c on an old build |
| D | `hog` | small |
| D | `clk` | large (L1-L4) |
| D | `ms` | small |
| D | `mt-ms` | large (L5) |
| N | `fn` | large (L5) |
| N | `fs`, `pbc` | none |
| M | `m0` | large (L1-L4) |
| M | `mu` | large (L5) |
| M | `ma` | small (1.7x) |
| X | `-` | large |
| X | `wM` | small; casa-rs slower (0.63-0.67x) |
| X | `wC` | small (3.8x) |
| Backend | Metal W4 | L1, L4 (standard only) |
| Backend | CPU W1 | L2, L3, L5 |
| Backend | CPU W>1 | none on a large row |

By regime: R1 has L1 and L4 (both Metal); R2 has L5 only (CPU, 1 worker,
4096²); R3/R4 have L2-L3; R5 has nothing but the small `wM` rows and the
cube's paging inside L1; R6 is the whole small set. Nothing measures the
12,150² geometry, the W-kernel growth, or an AW first pass on the current
build.

## 5. A minimal representative benchmark set

Constraints: each row runs in minutes on a 32 GB Apple laptop; prefer the
datasets and frozen CASA references that exist; every row names its backend,
precision, workers and cache state (TESTING.md L493-507). AW rows are CPU
(ARCHITECTURE.md L691-692). "Needs CASA" marks rows whose reference must be
frozen once; CASA's own time for each is estimated from the frozen neighbours.

| # | Signature | Data and selection | Why it is in the set | Regime | What it predicts for 12,150² | CASA reference |
|---|---|---|---|---|---|---|
| B1 | `mt2/AW32pcb/1f/br1/mt-ms[0,5,12] n2000/mu @4096²·0.6"·0.67M·cpu W1` | VLASS fragment, field 1525, SPWs 2,7,12,17 | Few samples, so G is small: isolates CF generation and loading (C), the MT-MFS first pass (F) and the minor cycle (D) at 4096²; the contract's preserved landmark | R2 (C) + R4 | The sample-independent part of the single-field 12,150² rows; with B3, the C share | frozen: 3,631.8 s (contract L550-552) |
| B2 | `mt2/AW32pcb/63f/br1/dirty @4096²·0.6"·41.9M·cpu W1` | VLASS fragment, 63 fields, SPWs 2,7,12,17 | The AW first pass alone: 2 data + 3 PSF + weight grids, every CF group touched once, pointing groups; 42 M samples | R2 | The first-pass term of casa-b and the dirty acceptance rows; its CF-miss count scales with cell bytes (x9-78) | frozen casa-c: 541 s |
| B3 | `mt2/AW32pcb/63f/br1/mt-ms[0,5,12] n2000 M11/mu @4096²·41.9M·cpu W1` | same | The central VLASS proxy: 11-12 residual passes; per-pass cost = G(AW) + F + P; divided by B2 gives the residual-pass cost | R2 | Per-pass AW gridding at 4096² kernel widths; multiply by the measured support growth to predict casa-b's passes | frozen casa-d: 3,470 s. Expect casa-rs about 1,000-1,500 s on W1 (a quarter of L5's samples): the longest row here; cap `--nmajor 3` for iteration, full row for the record |
| B4 | `mt2/AW32pcb/1f/br1/dirty @12150²·0.6"·2.7M·cpu W1` | VLASS fragment, field 1525, SPWs 2-17 | The only full-geometry row that fits in minutes: measures the 12,150² kernel widths (read from the CF headers and from stage timing), the 6-grid first-pass memory plan and the 1.5e8-pixel FFTs with few samples | R2 + R5 | Directly: the first-pass term of the two single-field acceptance rows and the memory plan for the 63-field ones | **none**; a CASA dirty run is bounded above by casa-a's 3,798 s, so freezing one is an hour. Gap |
| B5 | `mt2/AW32pcb/1f/br1/mt-ms[0,5,12] n2000 M9/mu @12150²·2.7M·cpu W1` | same | The one full-geometry clean row with a frozen CASA time that a laptop can run; measures the 12,150² minor cycle (D at 1.5e8 pixels, 3 scales, 2 terms) and nine residual passes | R2 + R4 + R5 | It is an acceptance row | frozen casa-a: 3,798 s. casa-rs time unknown on the IF build; it may exceed "minutes" — run it last |
| B6 | `mt2/W32/1f/br1/dirty @4096²·0.6"·2.7M·cpu W1` | VLASS field 1525, SPWs 2-17, `gridder=wproject`, `wprojplanes=32` | W-projection on the same data, image and planes as B1/B4 but without the A-term or the CF cache: the difference to the AW dirty row at the same size is the A-term and cache cost; rerun at 12,150² gives the W-kernel growth alone | R2 (W only) | Separates `N_sup(W) ∝ θ²` from `N_sup(A) ∝ θ` in the 12,150² prediction | **none**; CASA minutes. Gap |
| B7 | `mfs/S/uni/clk n10000 t0.5mJy @4096²·0.05"·64.7M·cpu W1` | EVLA pilot, 4 of 32 SPWs | Existing 5.49x row; the standard-kernel control for F, P and the Clark minor cycle at 4096²; no CF cost | R3/R4 | The pixel-bound floor: scale by `N_pix log N_pix` (x9.9) for the FFT and normalisation share of every 12,150² pass | frozen: 246 s |
| B8 | `mfs/S/uni/clk n10000 t5mJy @4096²·0.8"·0.54G·{cpu W1, gpu W4}` | Wave3 MS, 64 of 512 channels | Sample-dominated standard gridding in minutes (an eighth of L4): per-sample throughput and the W1-vs-W4 serial check the owner asks for; L4 itself is 78 minutes | R1 | The per-sample fixed cost that AW kernels multiply; a ceiling on what narrowing kernels could buy | **none** (L4's reference is the full MS). CASA about 15-20 min. Gap |
| B9 | `cube128/S/nat/clk t0.5mJy @1024²·1.05G·128pl·gpu W4` | the 512-channel VLA C-array MS, 128 channels | Cube residency and per-plane partition at a quarter of L1 (L1 is 69 minutes); the only R5 row that is not a column write | R1 + R5 | Not VLASS; keeps the other large workload class honest while AW work proceeds | **none** (L1's reference is 512 channels). CASA about 1.5 h. Gap |
| B10 | `mfs/M/7f/br0.5/hog n500/fn pbl0.2 @1024²·8.4M·cpu W1` | ALMA 7-field mosaic MS (the 0.51x row's data) | Mosaic PB kernels share the `FT[PB]`, weight-image and flat-noise machinery with AW; the 512² row is slower than CASA and the cause (CF generation vs fixed cost) is unknown; 1024² moves it out of R6 | R2 (M) | Whether the PB-kernel generation cost seen at 512² is fixed or per sample; a smaller analogue of the AW C term | frozen 2026-10-18: CASA 6.7.6, 59.2 s (#732; workload `b10-mosaic-mfs-alma-1024-clean`) |
| B11 | `cube16/M/7f/nat/hog n500 pbl0.2 lin @1024²·8.4M·cpu W1` | same MS, the 16 channels B10 images, as a cube | B10's data and size as a cube: mosaic workers run plane owners here and region owners in B10, so the two rows scale by different paths; the products step (16 planes restored and PB-corrected) is a visible share. Natural weighting, because a Briggs mosaic cube with a per-channel density does not yet match CASA (#789) | R2 (M) + R4 | With B10, the mosaic bar of plan #785 Phase 2 (#787: 5× CASA on the best plan) | frozen 2026-10-19: CASA 6.7.6, 27.8 s (#787; workload `b11-mosaic-cube-alma-1024-clean`) |

Optional eleventh row, outside the VLASS path but a known regression:
`mfs/S/uni/clk n10000/wM @4096²·64.7M` (B7 plus `savemodel=modelcolumn`),
to find whether the 0.63-0.67x small-row result is the column write or the
extra prediction pass (R5). CASA: B7 plus the write.

Reading the set against the target:

- B2 → B3 gives first-pass versus residual-pass AW cost at 4096²; B4 gives
  the first pass at 12,150² with few samples; B6 at both sizes gives the W
  share. Together they predict casa-b's per-pass cost as
  `pass(4096²) × support growth`, where the growth is measured, not assumed.
- B1 and B5 bracket the minor cycle at the two sizes with the same `niter`.
- B7 and B8 give the standard-kernel floors (pixel-bound and sample-bound)
  that any AW improvement cannot pass.
- B9 and B10 keep the cube and mosaic regimes measured without long runs.

Gaps: no frozen CASA time for the single-field or 63-field dirty 12,150²
rows (two of the four acceptance rows), for B6, B8, B9 or B10; the 63-field
12,150² rows cannot be made minutes-long on a laptop by any selection that
keeps the geometry (casa-b took CASA 22.9 h); no dataset in casatestdata gives
a W-projection or AW row above R6 (the `refim_*` MSs are 1-183 MB), so the
VLASS fragment is the only wide-field source at scale; `vla_wideband_2ptg_w_squint.ms`
and `refim_mawproject.ms` are 1 KB stubs on the NAS copy and need the real
files before they can seed anything.

## 6. Open questions

1. **Kernel widths at 12,150².** The sources give ranges (7-50 cells for A
   and mosaic kernels, "significantly larger" with W, up to 200x200;
   tclean.xml L791-817; Jagannathan-Widefield.pdf, slide 36) and one scaling
   statement ("support scales with `w_max`", slide 16). The `θ²` growth of
   the W kernel is my derivation from the slide-13 phase; it predicts a 9x to
   78x kernel-cost increase from 4096² to 12,150², while CASA's own clean
   references scale by 9.1x. Either most VLASS samples are not W-dominated at
   12,150², or CASA is pixel-bound there. The frozen CF cache headers answer
   the first half without a run.
2. **CASA's single-field time is nearly size-independent**: 3,631.8 s at
   4096² with 4 SPWs against 3,798 s at 12,150² with 16 SPWs (4x samples,
   8.8x pixels). Something fixed dominates CASA's single-field AW runs (CF
   cache fill, restoration of 19 products, or its minor cycle); the recovery
   contract does not record the cache state of the 4096² reference. CASA's
   stage timers for both runs would show it. Until then a single-field
   speedup says little about the 63-field one, which the contract already
   forbids inferring (`vlass-imaging-recovery-contract.md` L519-522).
3. **Where the MT-MFS minor cycle sits at 1.5e8 pixels.** The corpus gives
   the algorithm (Marvil_Wideband2024.pdf, slide 20-21) but no cost model;
   casa-rs's `Taylor` solver (plan §5.5) does not state whether its peak
   search is incremental. The small AW rows (45x dirty, 19x clean) suggest
   casa-rs's minor cycle share is larger than CASA's. B1 versus B5 measures
   it.
4. **Mosaic and W-projection slower than CASA at 512²** (0.51x, 0.76x, 1.1x).
   Whether `MosaicPb` kernel generation (Lanczos oversampling 10 per
   frequency, antenna-class pair and pointing) or `WPlanes` generation is a
   fixed cost or grows with samples is unmeasured above R6. B10 and B6 answer
   it.
5. **`MODEL_DATA` writes at 0.63-0.67x.** Column write or prediction pass:
   the optional row separates them.
6. **Equal-work comparisons.** L4 ran 23 major cycles against CASA's 15, and
   L1 17 against an unrecorded CASA count; the speedups mix cycle control with
   throughput. TESTING.md L509-512 asks for `--nmajor` caps; the per-pass
   figures in 4.1 are my division, not a measurement.
7. **Standard kernel width.** The corpus says 3x3 (Jagannathan-Widefield.pdf,
   slide 34-35), the book says `m = 6` cells (Synthesis-Imaging-II-1998.pdf,
   p. 167), CASA says 7x7 (tclean.xml L747), casa-rs half-support 3 (plan
   §5.1). These are the same kernel described by half-width, width and tap
   count; cost models should use 49 taps.
8. **Which recipe is "the" wideband mosaic.** CASA's own guidance is
   `specmode='mvc'` for wideband mosaics (URV_Wideband_Imaging_SIW2026.pdf,
   slide 23, 25); the VLASS recipe under test is `mfs` + AW + `conjbeams`,
   and `mvc` is not in the casa-rs catalog (ARCHITECTURE.md L657-658). The
   10x target is defined on the VLASS recipe; a future `mvc` would be a
   different signature (`mvc/AW…`) and a different regime (cube passes).
9. **AW on more than one CPU worker.** L5 ran one worker; no large AW row
   records W4, so the `Partition::Regions` scaling of wide kernels (halo
   size grows with `N_sup`) is unknown. B3 at W1 and W4 would show it.
10. **Cube weighting.** `perchanweightdensity` and `briggsbwtaper` are
    recommended for cubes (Plunket-Mosaicking2024.pdf, slide 34) and multiply
    the density grid by `N_pl`, but no cube row uses them; L1 is natural.
11. **The sources are silent on CF-cache cost models.** CASA says only that
    filling the cache "can be time consuming" (tclean.xml L1099). The count
    `N_cf` and the per-cell bytes above are from casa-rs's index rules (plan
    §5.6) and my arithmetic; no corpus document quantifies A-projection
    kernel generation.

## Appendix: source ids

| Citation file | Corpus id |
|---|---|
| Jagannathan-Widefield.pdf (2026) | jagannathan-widefield-3b281d7d14f6 |
| Jagannathan-Widefield2024.pdf | jagannathan-widefield2024-8d1bffb6beec |
| URV_Wideband_Imaging_SIW2026.pdf | urv-wideband-imaging-siw2026-646f8466fba1 |
| Marvil_Wideband2024.pdf | marvil-wideband2024-6142f23151c0 |
| Marvil_Imaging2026.pdf | marvil-imaging2026-57343c49c331 |
| Plunket-Mosaicking2024.pdf | plunket-mosaicking2024-35d7af5d3a90 |
| Svoboda_Spectral_Lines.pdf (2026) | svoboda-spectral-lines-52967ab30fd7 |
| Pihlstrom-SpectralLines2024.pdf | pihlstrom-spectrallines2024-36c108be0ab7 |
| Perley-Geometry-SIW2026.pdf | perley-geometry-siw2026-63fb00084c9e |
| Perley-Polarization-SIW2026.pdf | perley-polarization-siw2026-5b17d117b65b |
| Butterfield-ErrorRecognition-SIW2026.pptx | butterfield-errorrecognition-siw2026-e10a7be06702 |
| ALMA-Manual-Imaging-Tutorial_SISS-2026.pdf | alma-manual-imaging-tutorial-siss-2026-981b33dcf769 |
| Plant-Correlators.pptx | plant-correlators-dbd92a0312c8 |
| MarvilSelfcal2026.pdf | marvilselfcal2026-f6c9c563d8fd |
| Synthesis-Imaging-II-1998.pdf | synthesis-imaging-ii-1998-f739aa16ac0a |
| Interferometry and Synthesis in Radio Astronomy.pdf | interferometry-and-synthesis-in-radio-astronomy-855e570763d3 |

casa-rs files: `docs/imaging-architecture/imaging-foundation-plan-20261007.md`
(§5.1-5.8), `ARCHITECTURE.md` (L524-694), `TESTING.md` (L451-512),
`crates/casa-imaging-application/src/availability.rs`,
`crates/casa-provider-contracts/resources/parameter-catalog.json`,
`docs/tutorial-parity/vlass-imaging-recovery-contract.md`,
`resources/imaging-architecture/representative-science-matrix.json`. CASA:
`casatasks/xml/tclean.xml`,
`casatools/src/code/synthesis/TransformMachines2/WPConvFunc.cc`.
