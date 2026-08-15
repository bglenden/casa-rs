# VLASS Imaging Recovery Salvage Audit

Truth class: bounded recovery decision

Decision date: 2026-08-02

Verification: `python -m unittest tools.perf.imager.test_vlass_recovery_contract`
plus read-only SHA-256 revalidation of the cited external receipts

This audit closes the eight-engineer-hour salvage phase required by the
approved VLASS merge-recovery contract. It selects one primary source seed and
one conditional reserve from the preserved archive. It does not authorize a
new architecture, count reduced evidence as final acceptance, or claim a
matched clean speedup before the new CASA clean references complete.

The machine-readable decision is
`tools/perf/imager/vlass_recovery_salvage_catalog.json`.

## Primary selection

The primary seed is the already-promoted `c23831b08` reduced-clean
checkpoint:

- shared multi-SPW, POINTING, AWProject, MT-MFS, product, planner, telemetry,
  and UI/task substrate;
- compact exact source-order AW replay, the promoted Metal replay paths, and
  f64 FFTW;
- mask- and scale-supported sparse MT-MFS minor-cycle state; and
- the four-SPW hybrid-clean and full-16-SPW v59 configurations that had already
  passed the frozen scientific floor.

The exact durable source checkpoint and scientific-promotion commit are both
`c23831b081555423e15c76e6f71215251ee68fd9`. The shared-substrate extraction
boundary remains `95cdb0664f15c32657100057e62531806a801dda`.

The first audit draft incorrectly selected the older
`9a14c6b5748e1a56367ca9adf8ad9e1667cd1626` seed and recorded a malformed
`c23831b08` expansion. A graph and source audit showed that `9a14c6b57`
predates the promoted hybrid-clean and full-16 correctness work. Its neutral
Mac-mini run was stopped after 8 minutes 26 seconds while still near major
cycle 40; the preserved partial log has SHA-256
`d3be7d42353b84338106ee3ff76048e832f0e27a1091d226ff640a53c3206df1`.
The first PR3 trim from that obsolete seed also failed the four-SPW science
comparison by 28 to 45 percent and omitted `.mask`; its run-log and comparison
hashes are
`6a88cba1ab3522d5683664f2269e51303f1396b96ab87832925ade5f65004a0d`
and
`55afa68eecc4e33395908ebb02086131571424ab56e98977a33c9c873243a0b4`.
Neither result is a candidate.

The corrected checkpoint was rebuilt from a clean detached worktree on the
Mac mini. The exact binary SHA-256 is
`4eaa35ae7cbbfcb1f8286dede7d48d0336ac1183c14113292f1f89942c9b6398`;
the four-SPW runner SHA-256 is
`1a9ba23a044908dd29120fe1137b86d09e5e25491b6f31be938a968bfadfe448`.
The exact 4096-square four-SPW and full-16-SPW gates were then rerun once for
this checkpoint before any full-geometry casa-rs row.

The read-only evidence revalidated for selection is:

| Evidence | Result | SHA-256 |
| --- | --- | --- |
| 4096-square four-SPW clean scientific floor | Passed; 29.43 s historical wall | `dcf405a389acadc6852a74d1086e645bd16adc402817bbaa000a99c65b96dc9f` |
| 4096-square full-16-SPW clean scientific floor | Passed; 101.646 s, 641 components, five major cycles | `f06859c9215a26b15dd32731345b9fdb1aaf1ab0fc267938638dd016b99518a1` |
| Full-16 v59 comparison | Passed the active scientific floor; sub-ppm products with two cutoff-boundary alpha pixels | `fb4a83e321028f50ab5234e07f2dcd30b0bcfc608a6d2bbcba06b4fff2565993` |
| Full-16 v59 run log | 19 products, no warning or divergence; 12,123,078,656-byte sampled peak RSS | `1133ccc666de490b74a905e1dd9d99ab99c1e6a8056cb7f2e892464e08d2c893` |
| 12150-square single-field dirty receipt | All 18 dirty products passed; 104.542929 s versus matched CASA 1276.157 s, 12.207x | `0faf0102ce09802c234de1880f1bcc4f5da9d8bb34e3678dd5ff09e9adec6e34` |

The dirty result is supporting operator evidence only. It is not a clean
speedup and cannot substitute for CASA A or CASA B.

The corrected checkpoint's new Mac-mini receipts are:

| Evidence | Result | SHA-256 |
| --- | --- | --- |
| Four-SPW run log | 46.14 s; 171 major cycles; 2,000 iterations; 19 products; zero warnings | `32b54ea0fa067202b502099e88fa8bd2d3b419ba46cd19e46db05b7700e21d46` |
| Four-SPW full comparison | Exact inventory and metadata; expected alpha cutoff topology only | `f96a1906f584c21d01afc1a43f4fbf9a814f5dfdad095a5c4e863edeed70f9a3` |
| Four-SPW scientific floor | Promoted; every scientific gate passed | `4fa269de10f0bd29e9dd021e19ae39777fd61231328ab3eacc01ca2352c0aaa6` |
| Full-16 run log | 76.23 s; 641 components; six reported cycles including final refresh; 19 products; zero warnings | `e059da388f70a9d1b735d602fee6e1df66b956f5493359f87ce018eadd5c9b95` |
| Full-16 full comparison | Exact inventory and metadata; expected alpha cutoff topology only | `eadfb13466c64d346bf6bec6c30725028f655713568ee0afa952a1fcc31a347a` |
| Full-16 scientific floor | Promoted; every scientific gate passed | `a98548ba0f9d96d5fe72304e69e08b8af5c77362aa687cc135115cdde243b057` |

The four-SPW and full-16 launcher receipts measured peak RSS of
8,271,118,336 and 6,573,064,192 bytes, respectively, with zero swaps. These
are development-host measurements, not final 32 GiB laptop acceptance or
matched full-geometry clean speedups.

## Conditional reserve

The only reserve is the already-measured compensation-lifetime transform from
source checkpoint `aea444b5e40fde5486e2ea421e5f8e2cf32d6174`. It collapses
and releases the f32 low compensation plane before f64 readback and FFT.

On the 4096-square full-16-SPW row it released 1,073,741,824 bytes at the
initial-grid peak and 268,435,456 bytes at each residual refresh, completed in
71.386 seconds, retained 641 updates, and passed the scientific floor. The
revalidated hashes are:

| Artifact | SHA-256 |
| --- | --- |
| Run log | `395be24287cc11884cb5c1423b2d4070e2573c79ba9457446c8ea4ed5ed78343` |
| Product comparison | `e41c117fb2ed177547cda7db84cbda722a14c5fc32b69473e896310ee202a05a` |
| Trajectory comparison | `ad360a24256329dcf7f924f6bbfda6f33af83c661445d54a4fb5c703cd506d61` |
| Scientific-floor receipt | `97df719a331d88f890914448cd08c41d7828b62ac5ea247b858b25196b4fc9ad` |

This reserve activates only if the primary freeze cannot admit or bound the
12150-square clean row on the 32 GiB laptop. Extraction is limited to the
collapse operation, lifetime accounting, telemetry, and focused tests. The
surrounding solver and direct-CF experiments are excluded. Making this
precision/lifetime policy a production default still requires Brian’s
approval.

## Retired alternatives

The architecture tournament is closed. In particular:

- the exact Metal direct-CF f32 residual path completed a 12150-square clean
  row in 819.120 seconds but failed image/model/residual science decisively
  and incurred destructive swap; its workload, comparison, and telemetry
  hashes are
  `3ae18ec7a6dc5521e04294d8a674963417cb2339107349b93d88e0ee7b8a7c04`,
  `973a78ac840275c09bc47ecc9d387d2fd60ed8d4c76669a623e70840939f99cf`,
  and
  `6eef9f204a283fc801e841d08b87ff21b8971c35ad0f386741de014d554ef279`;
- mmap and partial replay atlases thrashed or completed slowly with failed
  products;
- analytic ordered response, flexible GCR, post-weight spectral quadrature,
  ButterflyPACK, two-anchor multisecant, and initial/final-only scheduling
  failed their bounded discriminators; and
- the sum-first spatial polynomial remains research-only because no complete
  executable all-scale operator exists.

These remain accessible at archive commit `4c3cf8cc9`; they are not imported
into the recovery train.

## Extraction route

PR2 reconstructs the production shared substrate through `95cdb0664`, without
historical experiments or the removed optimization ledger. PR3 applies only
the production deltas needed for the promoted `c23831b08` checkpoint, removes
diagnostic-only controls, exposes the selected planner/runtime policy through
canonical task and UI parameters, and creates the new immutable candidate
freeze. The later architecture tournament remains excluded.

The reduced ladder then runs once for that freeze. A passing primary proceeds
to matched full-geometry A and B rows using the new frozen CASA references.
The reserve is used only after a declared primary memory/admission failure.
No second primary, new solver, or architecture tournament is authorized.
