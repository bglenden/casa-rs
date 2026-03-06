# Question: ~16 mas deviation between casacore IAU 2000A and SOFA/ERFA direction conversions

## Summary

We're comparing casacore's direction conversion (`MDirection` J2000 → APP) against the IAU SOFA reference implementation (via ERFA). For the IAU 1976/1980 model, the two agree to ~1.5 mas — consistent with casacore's Sun deflection and Stumpff velocity series being different from SOFA's `ab()` and `epv00`. But for **IAU 2000A mode, the deviation is ~16 mas**, which appears to be a genuine difference in the precession-nutation computation.

We'd like to understand whether this IAU 2000A deviation is expected, or if it indicates a bug in either implementation.

## Measured deviations

Test: J2000 direction (lon=1.0 rad, lat=0.5 rad) → APP, observer at the VLA.

### IAU 1976/1980 (casacore default)

| Epoch | Angular separation (mas) |
|-------|-------------------------|
| J2000.0 (MJD 51544.5) | 1.4 |
| 2020-01-01 (MJD 58849.0) | 1.5 |

### IAU 2000A

| Epoch | Angular separation (mas) |
|-------|-------------------------|
| J2000.0 (MJD 51544.5) | 16.2 |
| 2020-01-01 (MJD 58849.0) | 16.1 |
| J2000.0, near ecliptic pole | 21.9 |

The IAU 2000A deviation is ~10× larger than IAU 1976/1980 and direction-dependent, pointing to a difference in the precession-nutation model rather than aberration/velocity.

## SOFA computation method

For the SOFA/ERFA side, the APP direction is computed as:
1. Bias-precession-nutation matrix: `eraPnm00a` (IAU 2000A) or `eraPnm80` (IAU 1976)
2. Apply BPN to J2000 direction cosines
3. Earth velocity: `eraEpv00` (VSOP87), converted to fraction of c
4. Rotate velocity by same BPN matrix
5. Apply aberration: `eraAb(p_true, v_true, sun_dist, bm1)`

For casacore, we use:
```cpp
// IAU 2000A mode enabled via AipsrcValue:
AipsrcValue<Bool>::set(
    AipsrcValue<Bool>::registerRC("measures.iau2000.b_use", False), True);
AipsrcValue<Bool>::set(
    AipsrcValue<Bool>::registerRC("measures.iau2000.b_use2000a", False), True);
MDirection::Convert toApp(dir_j2000, MDirection::Ref(MDirection::APP, frame));
MDirection app = toApp();
```

## Known algorithmic differences

| Component | casacore | SOFA/ERFA |
|-----------|----------|-----------|
| Precession (IAU 2000) | Euler angles (ζA, zA, θA) with frame bias in constant terms | Lieske 1977 (ψA, ωA, χA) + `pr00` corrections + `bi00` bias |
| Nutation (IAU 2000A) | Internal series | `eraNut00a` (Mathews et al. 2002) |
| Earth velocity | Stumpff polynomial series | VSOP87 (`eraEpv00`) |
| Sun deflection | Full (`applySolarPos`) | ~0.4 µas in `eraAb()` |

The Earth velocity difference contributes only ~0.1 mas. Sun deflection contributes ~1-2 mas but affects both IAU models equally. The extra ~14 mas for IAU 2000A must come from the precession-nutation differences.

## Test program

A self-contained C++ test program is attached below. It links against both casacore and ERFA and prints the deviations directly.

Build:
```bash
g++ -std=c++17 -O2 -o casacore_vs_sofa_deviation casacore_vs_sofa_deviation.cpp \
    $(pkg-config --cflags --libs casacore) $(pkg-config --cflags --libs erfa) -lm
```

Example output:
```
--- IAU 1976/1980 (casacore default) ---
  casacore APP: lon=1.000018931173227 lat=0.499988058986953 rad
  SOFA/ERFA APP: lon=1.000018923468733 lat=0.499988058512320 rad
  Angular separation: 1.398 mas

--- IAU 2000A ---
  casacore APP: lon=1.000018878924623 lat=0.499988068006849 rad
  SOFA/ERFA APP: lon=1.000018914721858 lat=0.499987996189479 rad
  Angular separation: 16.169 mas
```

## Questions

1. Is the ~16 mas IAU 2000A deviation expected from the different precession parameterizations, or does it indicate a discrepancy in casacore's IAU 2000 implementation?
2. Has casacore's IAU 2000A mode been validated against SOFA/ERFA at this level? (We noticed SOFA is an optional build dependency "only for testing".)
3. If this is a known limitation, is there a preferred "ground truth" — should we treat SOFA or casacore as more authoritative for IAU 2000A?

## Context

We're developing a Rust implementation of casacore's data formats and measures system, using SOFA (via the [`sofars`](https://crates.io/crates/sofars) Rust crate) as the computational backend. Understanding this deviation helps us set interop test tolerances and decide whether to transliterate casacore's algorithms directly.
