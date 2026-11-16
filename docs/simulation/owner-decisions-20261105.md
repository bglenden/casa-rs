# Simulation equivalence: owner decisions of 2026-11-05

The owner approved the simulation equivalence proposals presented on
2026-11-05 for [#1053](https://github.com/bglenden/casa-rs/issues/1053).
The approvals apply to the specific behavior and evidence recorded in the
compatibility reference and the linked work below; they do not imply that
unimplemented methods or unrelated deviations are complete.

## Audit proposals

The owner approved these existing behaviors, numbered as presented:

1. Start the measures tool with an empty frame and require the missing frame
   elements for conversions.
2. Keep `sm.predict` from rewriting its input model image.
3. Raise for a model the simulator cannot read.
4. Evaluate the component spectrum and primary beam at each channel's own
   frequency instead of reproducing the CASA spectral-frame shift.
5. Write simulated gain and leakage tables at `close`, with the resulting
   table timing and subtable differences.
6. Keep the supplied component flux precision.
7. Withdraw the proposed difference in component shape record units and
   match CASA's in-memory units. This is unfinished: [#1187](https://github.com/bglenden/casa-rs/issues/1187)
   tracks `torecord` and [#1185](https://github.com/bglenden/casa-rs/issues/1185)
   tracks `getshape`.
8. Convert a component spectrum through a real image measures frame when
   available; otherwise retain the input frequencies rather than convert
   through CASA's placeholder frame.
9. Make `simobserve` project paths work using the directory's final name.
10. Preserve the exact single direction supplied to `simobserve`.

## Later proposals

- Raise `ValueError` for scalar non-finite or cyclic component selections,
  leaving the selection unchanged, rather than aborting the process
  ([#1176](https://github.com/bglenden/casa-rs/issues/1176)).
- Preserve big-endian integer arrays' values in quantity records rather than
  reproduce CASA's byte-order corruption
  ([#1183](https://github.com/bglenden/casa-rs/issues/1183)).
- For `componentlist.isphysical()` on an empty list, use a catchable error
  rather than exiting the process; implementation and populated-list
  comparison remain open ([#1158](https://github.com/bglenden/casa-rs/issues/1158)).
- Follow casacore's observatory table interpretation even for the problematic
  rows; retain the EVLA fallback
  ([#1111](https://github.com/bglenden/casa-rs/pull/1111)).
- Retain whitespace trimming in the Rust `MDirection::from_source_name` and
  casacore's HI override ([#1114](https://github.com/bglenden/casa-rs/pull/1114)).
- Log a verbose `simutil.msg` once without terminal color codes, name its
  logger `casars.<origin>`, and leave no image open after `is4d`
  ([#1116](https://github.com/bglenden/casa-rs/pull/1116)).

The comparison inputs, CASA and casa-rs behavior, rationale and impact are
in the generated [CASA compatibility reference](../reference/casa-compatibility.md)
and [CASA bug records](../CASA%20(C%2B%2B)%20bugs.md).
