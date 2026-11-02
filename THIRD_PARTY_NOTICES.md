# Third-party notices

## FFTW

The imaging FFT implementation uses FFTW 3, Copyright (c) 2003, 2007–11
Matteo Frigo, Copyright (c) 2003, 2007–11 Massachusetts Institute of
Technology. FFTW is available under the GNU General Public License v2.0 or
later. The FFTW source and license are available at
<https://www.fftw.org/> and <https://www.fftw.org/fftw3_doc/License-and-Copyright.html>.

Distributions that link casa-rs imaging code with FFTW, including the native
applications and Python extensions, are combined GPLv3-or-later works. The
GPLv3 license text is included in [COPYING](COPYING). Independently authored
casa-rs source retains its LGPLv3-or-later grant; see [LICENSE](LICENSE).

Release bundles must include this notice and the applicable license texts.

Official release binaries and wheels use unmodified FFTW 3.3.11. Its complete
source archive `fftw-3.3.11.tar.gz` and `build-fftw.sh` build recipe accompany
the binary assets on the same GitHub release. The identical upstream source
is also available at <https://www.fftw.org/fftw-3.3.11.tar.gz> (SHA-256
`5630c24cdeb33b131612f7eb4b1a9934234754f9f388ff8617458d0be6f239a1`).
The casa-rs source and release workflows are available from the corresponding
release tag at <https://github.com/bglenden/casa-rs>.

Local builds may select a different FFTW installation via `PKG_CONFIG_PATH`.
Redistributors of those builds must provide the source and build information
corresponding to that actual dependency, including any distributor patches;
the official-release archive does not cover a different installation.

## ATM

`crates/casa-atmosphere` is a Rust port of ATM, the atmospheric transmission
model distributed with CASA (`casatools/src/code/atmosphere/ATM`, ATM 1.0.0),
Copyright (c) Instituto de Estructura de la Materia (IEM-CSIC), J. R. Pardo
et al., in the framework of the ALMA collaboration. ATM is available under
the GNU Lesser General Public License v2.1 or later; the port, including its
line catalogues and reference atmospheres (generated from the ATM sources by
`tools/atmosphere/extract_atm_tables.py`), is distributed under that
license's "or later" option as LGPLv3-or-later, with ATM's copyright notice
kept in each ported file. The ATM sources are part of CASA, available from
<https://github.com/casangi/casa6> and <https://casa.nrao.edu>.

## CFITSIO

`crates/casa-images` reads and writes FITS through the `fitsio` crate with
its `fitsio-src` feature, which compiles the CFITSIO 3.49 sources bundled
in `fitsio-sys` (with that crate's three build patches) into casa-rs
binaries and Python extensions. CFITSIO is from NASA's HEASARC
(<https://heasarc.gsfc.nasa.gov/fitsio/>) and is distributed under this
notice:

> Copyright (Unpublished--all rights reserved under the copyright laws of
> the United States), U.S. Government as represented by the Administrator
> of the National Aeronautics and Space Administration. No copyright is
> claimed in the United States under Title 17, U.S. Code.
>
> Permission to freely use, copy, modify, and distribute this software
> and its documentation without fee is hereby granted, provided that this
> copyright notice and disclaimer of warranty appears in all copies.
>
> DISCLAIMER:
>
> THE SOFTWARE IS PROVIDED 'AS IS' WITHOUT ANY WARRANTY OF ANY KIND,
> EITHER EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED TO,
> ANY WARRANTY THAT THE SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY
> IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
> PURPOSE, AND FREEDOM FROM INFRINGEMENT, AND ANY WARRANTY THAT THE
> DOCUMENTATION WILL CONFORM TO THE SOFTWARE, OR ANY WARRANTY THAT THE
> SOFTWARE WILL BE ERROR FREE. IN NO EVENT SHALL NASA BE LIABLE FOR ANY
> DAMAGES, INCLUDING, BUT NOT LIMITED TO, DIRECT, INDIRECT, SPECIAL OR
> CONSEQUENTIAL DAMAGES, ARISING OUT OF, RESULTING FROM, OR IN ANY WAY
> CONNECTED WITH THIS SOFTWARE, WHETHER OR NOT BASED UPON WARRANTY,
> CONTRACT, TORT, OR OTHERWISE, WHETHER OR NOT INJURY WAS SUSTAINED BY
> PERSONS OR PROPERTY OR OTHERWISE, AND WHETHER OR NOT LOSS WAS SUSTAINED
> FROM, OR AROSE OUT OF THE RESULTS OF, OR USE OF, THE SOFTWARE OR
> SERVICES PROVIDED HEREUNDER.
