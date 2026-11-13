# MEM reference plane

`mem_casa_676.f32le` stores four 32 by 32 arrays, in x-major order,
encoded as little-endian IEEE 754 binary32: dirty PSF, dirty residual,
full-mask one-iteration model, and boxed-mask two-iteration model.

They were generated with CASA 6.7.6.14 `tclean` and the CASA test dataset
`refim_eptwochan.ms`, with `imsize=32`, `cell='8arcsec'`,
`deconvolver='mem'`, `parallel=False`, `savemodel='none'`, and otherwise
CASA defaults. The box uses `mask='box[[10pix,10pix],[21pix,21pix]]'`.
The dirty products come from `niter=0`; the models come from separate
`niter=1` and `niter=2` runs. Their logged cycle thresholds are respectively
0.2000812143087387 and 0.1576884388923645 Jy. The model metadata `ALPHA`
values are 0.2783987820148468 and 0.3704831302165985.

This fixture covers CASA's image-plane MEM step, including its masked-out
model floor and its virtual residual dispatch. See issue #1060.

## Sparse AW restoring-beam fixture

`sparse_aw_psf.f32le` contains the 51 × 51 fit window of the first
plane of a CASA 6.7.6.14 AW PSF, with little-endian Float samples and
the y index varying fastest. It comes from `refim_point_linRL.ms`
with linear sampling, `start=0, width=1, nchan=3`, LSRK output, and a
1024-square image with 10 arcsec pixels. The window spans zero-based
pixels 487 through 537 along each spatial axis. The CASA fit selected
405 samples after cropping. The recorded beam was 1619.6436768 by
64.0271606 arcsec at 69.2624969 degrees for the original PSF and the
one-Float-step perturbations of pixel (513, 513). [Reproduction and
measurements in #1140](https://github.com/bglenden/casa-rs/issues/1140#issuecomment-6802113024).
