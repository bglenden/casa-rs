# ATM oracle fixtures

`profiles.json`, `refractivity.json`, `refractivity.bin` and
`layer_refractivity.json` are written by the C++ oracle
(`tools/atmosphere/atm_oracle.cc`) compiled from CASA's own ATM sources:

```bash
tools/atmosphere/build-atm-oracle.sh ~/SoftwareProjects/casa/casatools/src/code/atmosphere/ATM
```

- **Source:** CASA at `61020062cee290f5466cffed5ec5032e0c7a3434`, ATM 1.0.0
  (`casatools/src/code/atmosphere/ATM`).
- **Compiler:** Apple clang 21.0.0 (clang-2100.3.34.2), `-O2 -ffp-contract=off`,
  on Apple silicon. Complex division goes through the system compiler-rt
  `__divdc3`, which the flags do not rebuild (see `oracle_refractivity.rs`).

An independent rebuild from that source regenerated all four files byte for
byte (#894 review).
