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
