# Proposal: preserve prior numbered image histories

Status: accepted, owner2026-11-01: “Preserve prior histories”. Work issue: #980 (restart #944).

For fresh `restart=False` numbering, the owner agreed to leave prior output files
completely unchanged, including their history tables. New products still
need their own correct history; that completeness remains unfinished and
is not waived here.

Pinned CASA6.7.6.14 /6.7.6-14 appends the later task call to histories of
all old images matching the originally requested prefix, even when creating
`image_2`, `image_3`, etc. Our paired runs measured changes only to existing
`logtable/table.dat` and `logtable/table.f0`; scientific data stayed unchanged.
CASA `cleanhelper.write_tclean_history` searches `imagename + '*.*'`, filters
image-like names and logtable directories, then calls `write_task_history`
on every match. The retained upstream source and raw receipts accompany the
[activation receipt](../../resources/casa-compat/evidence/tclean-numbering-activation-20261101.json).

casa-rs currently preserves every old file byte for byte, excluding locks;
all20 native activation rows pass that check. Reproducing CASA's prefix-wide
history updates would write into older independent numbered results. Preserving
them makes a fresh output operation leave prior results untouched. User impact:
an old image's history will not contain the later calls that created other
numbered images. Its scientific content and original history remain intact.

This is an agreed precise metadata deviation for prior outputs during fresh numbering,
not a general history exclusion or a scientific-equivalence claim. It does not
change the default `restart=True` behavior or accept missing new-image history.
The absolute-directory CASA bug has a separate already-agreed decision.
