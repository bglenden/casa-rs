# ATM oracle fixtures

`profiles.json`, `refractivity.json`, `refractivity.bin`,
`layer_refractivity.json`, `absorption.json` and `sky.json` are written by the
C++ oracle (`tools/atmosphere/atm_oracle.cc` and `atm_oracle_sky.cc`)
compiled from CASA's own ATM sources with casa-rs's corrections
(`tools/atmosphere/atm-corrections.patch`, `docs/CASA (C++) bugs.md`)
applied:

```bash
tools/atmosphere/build-atm-oracle.sh ~/SoftwareProjects/casa/casatools/src/code/atmosphere/ATM
```

- **Source:** CASA at `61020062cee290f5466cffed5ec5032e0c7a3434`, ATM 1.0.0
  (`casatools/src/code/atmosphere/ATM`), with the patch.
- **Compiler:** Apple clang 21.0.0 (clang-2100.3.34.2), `-O2 -ffp-contract=off`,
  on Apple silicon. Complex division goes through the system compiler-rt
  `__divdc3`, which the flags do not rebuild (see `oracle_refractivity.rs`).

An independent rebuild from that source regenerated the four files then
committed byte for byte (#894 review). Built without the patch
(`--as-shipped --out <directory>`), the oracle still writes the files as
they were before the corrections, byte for byte.
