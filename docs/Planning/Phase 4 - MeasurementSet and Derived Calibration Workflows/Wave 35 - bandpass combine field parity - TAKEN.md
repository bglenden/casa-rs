# Wave 35 - bandpass combine field parity

## Summary

Close the remaining first-wave `bandpass combine=*` semantics by extending the
`B Jones` solver to support `combine='field'` and `combine='scan,field'`,
validate the grouping/output behavior on synthetic multi-field fixtures, and
check downstream CASA parity on `ngc5921.ms`.

## Scope

- Broaden the `BandpassSolveRequest` combine representation beyond the old
  scan-only boolean.
- Keep the app layer thin by exposing the same semantics through the existing
  `casa-calibration` CLI surface.
- Add synthetic multi-field regression coverage.
- Add slow CASA downstream parity coverage for both combined-field variants.

## Notes

- The real `ngc5921.ms` `combine='field'` workload does not pool multiple
  fields inside the same scan bucket, so the accepted parity contract there
  uses a looser solver-specific downstream tolerance than the tighter baseline
  `B` / `scan` cases.
