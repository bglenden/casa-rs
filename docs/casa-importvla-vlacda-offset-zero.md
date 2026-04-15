# `importvla` crash on old VLA export data when a CDA disappears between logical records

## Title

`importvla` crashes on NRAO Archive export file `AG189_1_46325.23029_46325.80807.exp` with `Failed Assertion: offset != 0`

## Issue body

`importvla` aborts on the NRAO Archive export file:

`AG189_1_46325.23029_46325.80807.exp`

The archive revision is `11`.

The failure is:

```text
RuntimeError: (.../VLABaselineRecord.cc : 57) Failed Assertion: offset != 0
```

The immediate assertion is in `nrao/VLA/VLABaselineRecord.cc`:

```cpp
void VLABaselineRecord::attach(ByteSource& record, uInt offset) {
  ...
  DebugAssert(offset != 0, AipsError);
  itsOffset = offset;
}
```

The probable root cause is in `nrao/VLA/VLACDA.cc`:

- `VLALogicalRecord::read()` reattaches all 4 CDAs on every logical record.
- `VLACDA` caches `itsACorr` / `itsXCorr` baseline-record objects.
- If a CDA is valid in one logical record, those cached baseline objects can be created.
- If that same CDA is absent in the next logical record, `CDAOffset(i) == 0`, but
  `VLACDA::attach()` still tries to reattach the cached baseline objects.
- That eventually calls `VLABaselineRecord::attach(..., 0)`, which triggers the
  assertion.

So this looks like a stale-cache bug when a previously valid CDA becomes absent in a
later logical record.

A minimal source-level reproducer is:

```cpp
VLACDA cda;
cda.attach(src, 16, 8, 2, 1);
(void)cda.autoCorr(0);   // create cached baseline object
cda.attach(src, 0, 8, 2, 1);   // reattach same VLACDA to absent CDA
```

With assertions enabled, this reaches the same `offset != 0` failure in
`VLABaselineRecord::attach()`.

A fix in `VLACDA::attach()` appears to be to clear cached baseline objects and return
early when the new CDA is invalid, before reattaching cached entries. For example:

```cpp
itsNant = newNant;

if (itsOffset == 0 || itsBaselineSize == 0 || itsNant == 0) {
  deleteACorr(0);
  deleteXCorr(0);
  itsNchan = newChan;
  return;
}

const uInt xCorrOffset = itsOffset + itsBaselineSize*itsNant;
```

This seems consistent with the existing `VLACDA::isValid()` semantics, where a CDA is
invalid when `itsOffset == 0`.

With that guard in place, `importvla` no longer aborts on this file and proceeds past
the previous crash point.
